import { Meld, Snapshot, DeltaMessage, Revup } from '.';
import { LiveValue } from "./LiveValue";
import { TreeClock } from './clocks';
import { Observable, Subject as Source, BehaviorSubject, asapScheduler, of } from 'rxjs';
import { observeOn, tap, distinctUntilChanged, first, skip, catchError } from 'rxjs/operators';
import { LogLevelDesc, Logger } from 'loglevel';
import { getIdLogger, check, HotSwitch, delayUntil, PauseableSource } from './util';
import { MeldError } from './MeldError';

export abstract class AbstractMeld implements Meld {
  protected static checkLive =
    check((m: AbstractMeld) => m.live.value === true, () => new MeldError('Meld is offline'));
  protected static checkNotClosed =
    check((m: AbstractMeld) => !m.closed, () => new MeldError('Clone has closed'));

  readonly updates: Observable<DeltaMessage>;
  readonly live: LiveValue<boolean | null>;

  private readonly updateSource = new PauseableSource<DeltaMessage>();
  private readonly liveSource: BehaviorSubject<boolean | null> = new BehaviorSubject(null);
  
  private closed = false;
  protected readonly log: Logger;

  constructor(readonly id: string, logLevel: LogLevelDesc = 'info') {
    this.log = getIdLogger(this.constructor, id, logLevel);

    // Update notifications are delayed to ensure internal processing has priority
    this.updates = this.updateSource.pipe(observeOn(asapScheduler),
      tap(msg => this.log.debug('has update', msg)));

    // Live notifications are distinct, only transitions are notified; an error
    // indicates a return to undecided liveness followed by completion.
    this.live = Object.defineProperties(
      this.liveSource.pipe(catchError(() => of(null)), distinctUntilChanged()),
      { value: { get: () => this.liveSource.value } });

    // Log liveness
    this.live.pipe(skip(1)).subscribe(
      live => this.log.debug('is', live == null ? 'gone' : live ? 'live' : 'dead'));
  }

  protected nextUpdate = (update: DeltaMessage) => this.updateSource.next(update);
  protected pauseUpdates = (until: PromiseLike<unknown>) => this.updateSource.pause(until);

  protected warnError = (err: any) => this.log.warn(err);

  protected setLive(live: boolean | null) {
    return this.liveSource.next(live);
  };

  abstract newClock(): Promise<TreeClock>;
  abstract snapshot(): Promise<Snapshot>;
  abstract revupFrom(time: TreeClock): Promise<Revup | undefined>;

  close(err?: any) {
    this.closed = true;
    if (err) {
      this.updateSource.error(err);
      this.liveSource.error(err);
    } else {
      this.updateSource.complete();
      this.liveSource.complete();
    }
  }
}

export function comesAlive(meld: Pick<Meld, 'live'>, expected: boolean | null | 'notNull' = true): Promise<boolean | null> {
  const filter: (live: boolean | null) => boolean =
    expected === 'notNull' ? (live => live != null) : (live => live === expected);
  return meld.live.pipe(first(filter)).toPromise();
};