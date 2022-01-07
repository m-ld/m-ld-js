import { EntryIndex, Journal, JournalEntry } from './index';
import { concatMap, debounceTime, endWith, map, share, take, takeUntil, tap } from 'rxjs/operators';
import { EMPTY, merge, Observable, of, race, Subject, timer } from 'rxjs';
import { idling } from '../local';
import { CausalOperator, CausalTimeRange } from '../ops';
import { Triple } from '../quads';
import { TreeClock } from '../clocks';
import { completed, getIdLogger, inflate } from '../util';
import { array } from '../../util';
import { TickTid } from './JournalOperation';
import { MeldConfig } from '../../config';

export type JournalClerkConfig = Pick<MeldConfig, '@id' | 'logLevel' | 'journal'>;

export class ClerkAction {
  constructor(
    readonly action: string,
    readonly body: { [key: string]: any }) {
  }

  toString = () => `${this.action} ${JSON.stringify(this.body)}`;
}

export enum CheckPoint {
  /** General administration checkpoint */
  ADMIN,
  /** Required journal breakpoint – fusions must not cross */
  SAVEPOINT
  // TODO: TRUNCATE, entries before will be dropped
}

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
  private readonly closing = new Subject;
  /** @see JournalConfig.maxEntryFootprint */
  readonly maxEntryFootprint: number;

  constructor(
    readonly journal: Journal,
    config: JournalClerkConfig,
    { checkpoints, schedule }: {
      /** @see CheckPoint */
      checkpoints?: Observable<CheckPoint>,
      /**
       * The schedule delays activity to some suitable time. It should emit one value in response
       * to a subscription.
       */
      schedule?: Observable<unknown>
    } = {}) {
    this.maxEntryFootprint = config.journal?.maxEntryFootprint ?? 10000;
    // Default checkpoints are general admin debounced after last entry
    const adminDebounce = config.journal?.adminDebounce ?? 1000;
    checkpoints ??= journal.tail.pipe(
      map(() => CheckPoint.ADMIN),
      debounceTime(adminDebounce));
    // Try to schedule everything (except save points) in idle time.
    const awaitSchedule = schedule?.pipe(take(1)) ??
      // Chrome sometimes never calls the idle callback, blocking activity, so time out
      race(idling(), timer(adminDebounce).pipe(tap(() => log.warn('Idle request timed out'))));
    this.activity = merge(journal.tail, checkpoints).pipe(
      // Redundant if the journal is closed first.
      takeUntil(this.closing),
      // Ensure administration has been completed
      endWith(CheckPoint.SAVEPOINT),
      // For every entry, ask the schedule for a slot to process
      concatMap(entry => inflate(
        // Process save points immediately
        entry === CheckPoint.SAVEPOINT ? of(0) : awaitSchedule,
        () => this.process(entry))),
      // Don't duplicate processing per subscriber
      share());
    // Kick things off by subscribing the logger
    const log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.activity.subscribe({
      next: action => log.debug(`${action}`),
      error: err => log.error(err),
      complete: () => this.closing.complete()
    });
  }

  async close() {
    this.closing.next(true);
    await completed(this.closing);
  }

  private process(entry: JournalEntry | CheckPoint): Observable<ClerkAction> {
    if (typeof entry === 'number')
      return this.checkpoint(entry);
    else {
      return this.processEntry(entry);
    }
  }

  private processEntry(entry: JournalEntry) {
    return merge(
      this.disposePrevOperationIfIsolated(entry),
      this.applyFusion(entry));
  }

  private checkpoint(entry: CheckPoint.ADMIN | CheckPoint.SAVEPOINT) {
    return this.fusion == null ? EMPTY : inflate(this.fusion.commit(), action => {
      if (entry === CheckPoint.SAVEPOINT || !this.fusion!.appendable)
        delete this.fusion;
      return array(action);
    });
  }

  private disposePrevOperationIfIsolated(entry: JournalEntry): Observable<ClerkAction> {
    // If the previous operation for the entry's time is not causally contiguous, AND does not
    // have a corresponding journal entry, it is safe to garbage collect.
    const [tick, tid] = entry.prev;
    if (tick > 0 && tick < entry.operation.tick - 1)
      return inflate(this.journal.disposeOperationIfUnreferenced(tid),
        done => done ? of(new ClerkAction('garbage collected', { tid })) : EMPTY);
    return EMPTY;
  }

  private applyFusion(entry: JournalEntry): Observable<FusionAction> {
    if (this.fusion == null) {
      this.fusion = new Fusion(this, entry);
      return EMPTY;
    } else {
      return inflate(this.fusion.next(entry), action => {
        // If no append happened, we must start a new fusion.
        if (action?.action !== 'appended')
          this.fusion = new Fusion(this, entry);
        return array(action);
      });
    }
  }
}

class FusionAction extends ClerkAction {
  constructor(action: 'appended' | 'committed', time: TreeClock, footprint: number) {
    super(action, { time, footprint });
  }
}

class Fusion {
  private readonly prev: TickTid;
  private readonly removals: EntryIndex[] = [];
  private readonly operator: CausalOperator<Triple, TreeClock>;
  private last: JournalEntry;

  constructor(
    private readonly clerk: JournalClerk,
    first: JournalEntry) {
    this.prev = first.prev;
    this.operator = first.operation.asMeldOperation().fusion();
    this.reset(first);
  }

  /**
   * @param entry the next entry to fuse, or start a new fusion with
   * @returns an action with `entry` if it was added to the fusion or a fused entry if the fusion
   *   was committed; or undefined if the fusion was discarded
   */
  async next(entry: JournalEntry): Promise<FusionAction | undefined> {
    if (this.appendable && CausalTimeRange.contiguous(this.last.operation, entry.operation))
      return this.append(entry);
    else
      return this.commit();
  }

  /**
   * Commits the current fusion to the journal. After calling this method, more entries can still
   * be appended, as fusions to the committed fusion.
   * @returns a commit action if the fusion was committed; or undefined if the fusion was discarded
   */
  async commit(): Promise<FusionAction | undefined> {
    // Only do anything if the fusion is significant
    if (this.removals.length > 1) {
      // CAUTION: constructing an operation can be expensive
      const operation = this.clerk.journal.toMeldOperation(this.operator.commit());
      const fusedEntry = JournalEntry.fromOperation(
        this.clerk.journal, this.last.key, this.prev, operation);
      await this.clerk.journal.spliceEntries(this.removals, fusedEntry);
      // Start again with the fused entry
      this.reset(fusedEntry);
      return this.action('committed', fusedEntry);
    }
  }

  get appendable() {
    return this.operator.footprint <= this.clerk.maxEntryFootprint;
  }

  private append(entry: JournalEntry) {
    this.operator.next(entry.operation.asMeldOperation());
    this.trackEntry(entry);
    return this.action('appended', entry);
  }

  private action(action: 'appended' | 'committed', entry: JournalEntry) {
    return new FusionAction(action, entry.operation.time, this.operator.footprint);
  }

  private trackEntry(entry: JournalEntry) {
    this.removals.push(entry.index);
    this.last = entry;
  }

  private reset(first: JournalEntry) {
    this.removals.length = 0;
    this.trackEntry(first);
  }
}

