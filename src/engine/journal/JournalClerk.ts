import { EntryIndex, Journal, JournalEntry } from './index';
import { concatMap, endWith, filter, finalize, map, share, takeUntil } from 'rxjs/operators';
import { EMPTY, from, merge, NEVER, Observable, of, Subject } from 'rxjs';
import { idling } from '../local';
import { CausalOperator, CausalTimeRange } from '../ops';
import { Triple } from '../quads';
import { TreeClock } from '../clocks';
import { Logger } from 'loglevel';
import { inflate } from '../util';

export interface ClerkAction {
  action: string;
  [key: string]: any;
}

function clerkAction<T extends object>(action: string, body: T): T & ClerkAction {
  return { action, ...body, toString: () => `Clerk ${action}: ${JSON.stringify(body)}` };
}

const CHECKPOINT = null;
export type CheckPoint = typeof CHECKPOINT;

/**
 * The journal clerk processes journal entries in the background according to a provided strategy.
 *
 * TODO: Keep track of progress in the store
 * TODO: Resilience to bursts of entries (by falling back to journal store as the queue)
 * TODO: Truncation strategies
 */
export class JournalClerk {
  readonly activity: Observable<ClerkAction>;
  private fusion: Fusion | undefined;
  private closing = new Subject;

  constructor(
    private readonly journal: Journal,
    { checkpoints, schedule, log }: {
      /** A checkpoint commits any active fusion */
      checkpoints?: Observable<CheckPoint>,
      /** The schedule delays activity to some suitable time */
      schedule?: Observable<unknown>,
      /** For logging activity */
      log?: Logger
    } = {}) {
    this.activity = merge(journal.entries, checkpoints ?? NEVER).pipe(
      // Redundant if the journal is closed first.
      takeUntil(this.closing),
      // Ensure a final commit
      endWith(CHECKPOINT),
      // For every entry, ask the schedule for a slot to process
      concatMap(entry => {
        if (entry == CHECKPOINT)
          return this.checkpoint(); // Process a checkpoint asap
        else
          return inflate(schedule ?? idling, () => this.process(entry));
      }),
      // Don't duplicate processing per subscriber
      share());
    // Kick things off by subscribing, even if no logger is present
    this.activity.subscribe(
      action => log?.debug(`${action}`),
      err => log?.error(err),
      () => this.closing.complete());
  }

  async close() {
    this.closing.next(true);
    await this.closing.toPromise();
  }

  private checkpoint(): Observable<ClerkAction> {
    return this.fusion == null ? EMPTY : from(this.fusion.commit()).pipe(
      finalize(() => delete this.fusion),
      filter<FusionAction>(action => action != null),
      map(action => action.clerkAction));
  }

  private process(entry: JournalEntry): Observable<ClerkAction> {
    return merge(
      this.disposeOperationIfIsolated(entry),
      this.applyFusion(entry));
  }

  private disposeOperationIfIsolated(entry: JournalEntry): Observable<ClerkAction> {
    // If the previous operation for the entry's time is not causally contiguous, AND does not
    // have a corresponding journal entry, it is safe to garbage collect.
    const [tick, tid] = entry.prev;
    if (tick > 0 && tick < entry.operation.tick - 1)
      return inflate(this.journal.disposeOperationIfUnreferenced(tid),
        done => done ? of(clerkAction('garbage collected', { tid })) : EMPTY);
    return EMPTY;
  }

  private applyFusion(entry: JournalEntry): Observable<ClerkAction> {
    if (this.fusion == null) {
      this.fusion = new Fusion(this.journal, entry);
      return EMPTY;
    } else {
      return inflate(this.fusion.next(entry), action => {
        if (action?.entry === entry) {
          // We appended the entry to the existing fusion.
          return of(action.clerkAction);
        } else {
          // We're done with the fusion. Start a new one.
          this.fusion = new Fusion(this.journal, entry);
          return action == null ? EMPTY : of(action.clerkAction);
        }
      });
    }
  }
}

interface FusionAction {
  action: 'appended' | 'committed';
  entry: JournalEntry;
  clerkAction: ClerkAction;
}

class Fusion {
  private last: JournalEntry;
  private entries: EntryIndex[] = [];
  private operator: CausalOperator<Triple, TreeClock>;

  constructor(
    private readonly journal: Journal,
    private readonly first: JournalEntry) {
    this.operator = first.operation.asMeldOperation().fusion();
    this.trackEntry(first);
  }

  private action = (action: FusionAction['action'], entry: JournalEntry) => ({
    action, entry, clerkAction: clerkAction(action, {
      time: entry.operation.time, footprint: this.operator.footprint
    })
  });

  /**
   * @param entry the next entry to fuse, or start a new fusion with
   * @returns an action with `entry` if it was added to the fusion or a fused entry if the fusion
   *   was committed; or `null` if the fusion was discarded
   */
  async next(entry: JournalEntry): Promise<FusionAction | null> {
    if (CausalTimeRange.contiguous(this.last.operation, entry.operation)) {
      this.append(entry);
      return this.action('appended', entry);
    } else {
      return this.commit();
    }
  }

  /**
   * @returns a fused entry if the fusion was committed; or `null` if the fusion was discarded
   */
  async commit(): Promise<FusionAction | null> {
    // Only do anything if the fusion is significant
    if (this.entries.length > 1) {
      const operation = this.journal.toMeldOperation(this.operator.commit());
      const fusedEntry = JournalEntry.fromOperation(
        this.journal, this.last.key, this.first.prev, operation);
      await this.journal.spliceEntries(this.entries, fusedEntry);
      return this.action('committed', fusedEntry);
    }
    return null;
  }

  private append(entry: JournalEntry) {
    this.operator.next(entry.operation.asMeldOperation());
    this.trackEntry(entry);
  }

  private trackEntry(entry: JournalEntry) {
    this.entries.push({ key: entry.key, tid: entry.operation.tid });
    this.last = entry;
  }
}

