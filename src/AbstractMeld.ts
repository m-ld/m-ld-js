import { Meld, Snapshot, DeltaMessage } from './m-ld';
import { TreeClock } from './clocks';
import { Observable, Subject as Source, BehaviorSubject, asapScheduler } from 'rxjs';
import { observeOn, tap, distinctUntilChanged, first, skip, map } from 'rxjs/operators';
import { LogLevelDesc, Logger } from 'loglevel';
import { getIdLogger, check } from './util';
import { MeldError } from './m-ld/MeldError';

export abstract class AbstractMeld implements Meld {
  protected static checkLive =
    check((m: AbstractMeld) => m.isLive() === true, () => new MeldError('Meld is offline'));
  protected static checkNotClosed =
    check((m: AbstractMeld) => !m.closed, () => new MeldError('Clone has closed'));

  readonly updates: Observable<DeltaMessage>;
  readonly live: Observable<boolean | null>;

  private readonly updateSource: Source<DeltaMessage> = new Source;
  private readonly liveSource: BehaviorSubject<boolean | null> = new BehaviorSubject(null);
  private closed = false;

  protected readonly log: Logger;

  constructor(readonly id: string, logLevel: LogLevelDesc = 'info') {
    this.log = getIdLogger(this.constructor, id, logLevel);

    // Update notifications are delayed to ensure internal processing has priority
    this.updates = this.updateSource.pipe(observeOn(asapScheduler),
      tap(msg => this.log.debug('has update', msg)));

    // Live notifications are distinct, so only transitions are notified
    this.live = this.liveSource.pipe(distinctUntilChanged());
    this.live.pipe(skip(1)).subscribe(live =>
      this.log.debug('is', live ? 'live' : 'dead'));
  }

  protected nextUpdate = (update: DeltaMessage) => this.updateSource.next(update);
  protected isLive = (): boolean | null => this.liveSource.value;
  protected warnError = (err: any) => this.log.warn(err);

  protected setLive(live: boolean | null) {
    return this.liveSource.next(live);
  };

  abstract newClock(): Promise<TreeClock>;
  abstract snapshot(): Promise<Snapshot>;
  abstract revupFrom(time: TreeClock): Promise<Observable<DeltaMessage> | undefined>;

  close(err?: any) {
    if (err) {
      this.updateSource.error(err);
      this.liveSource.error(err);
    } else {
      this.updateSource.complete();
      this.liveSource.complete();
    }
    this.closed = true;
  }
}

export function isLive(meld: Pick<Meld, 'live'>): Promise<boolean | null> {
  return meld.live.pipe(first()).toPromise();
};

export function comesAlive(meld: Pick<Meld, 'live'>, expected: boolean | null = true): Promise<boolean | null> {
  return meld.live.pipe(first(live => live === expected)).toPromise();
};

/**
 * For the purpose of this method, 'online' means the Meld's liveness is
 * determinate. Intuitively, it just means we're connected to something.
 * @see MeldStore.status
 */
export function isOnline(meld: Pick<Meld, 'live'>): Promise<boolean> {
  return meld.live.pipe(map(live => live != null), first()).toPromise();
};

/**
 * @see isOnline
 */
export function comesOnline(meld: Pick<Meld, 'live'>, expected: boolean = true): Promise<boolean> {
  return meld.live.pipe(map(live => live != null), first(online => online === expected)).toPromise();
};