import { Meld, Snapshot, DeltaMessage, ValueSource } from './m-ld';
import { TreeClock } from './clocks';
import { Observable, Subject as Source, BehaviorSubject, asapScheduler } from 'rxjs';
import { observeOn, tap, distinctUntilChanged, first, skip } from 'rxjs/operators';
import { LogLevelDesc, Logger } from 'loglevel';
import { getIdLogger, check } from './util';
import { MeldError } from './m-ld/MeldError';

export abstract class AbstractMeld implements Meld {
  protected static checkLive =
    check((m: AbstractMeld) => m.live.value === true, () => new MeldError('Meld is offline'));
  protected static checkNotClosed =
    check((m: AbstractMeld) => !m.closed, () => new MeldError('Clone has closed'));

  readonly updates: Observable<DeltaMessage>;

  private readonly updateSource: Source<DeltaMessage> = new Source;
  private readonly liveSource: BehaviorSubject<boolean | null> = new BehaviorSubject(null);
  private closed = false;

  protected readonly log: Logger;

  constructor(readonly id: string, logLevel: LogLevelDesc = 'info') {
    this.log = getIdLogger(this.constructor, id, logLevel);

    // Update notifications are delayed to ensure internal processing has priority
    this.updates = this.updateSource.pipe(observeOn(asapScheduler),
      tap(msg => this.log.debug('has update', msg)));

    // Log liveness
    this.live.pipe(skip(1)).subscribe(live =>
      this.log.debug('is', live ? 'live' : 'dead'));
  }

  get live(): ValueSource<boolean | null> {
    // Live notifications are distinct, only transitions are notified
    const source = this.liveSource.pipe(distinctUntilChanged());
    return Object.defineProperty(source, 'value', { get: () => this.liveSource.value });
  }

  protected nextUpdate = (update: DeltaMessage) => this.updateSource.next(update);
  protected warnError = (err: any) => this.log.warn(err);

  protected setLive(live: boolean | null) {
    return this.liveSource.next(live);
  };

  abstract newClock(): Promise<TreeClock>;
  abstract snapshot(): Promise<Snapshot>;
  abstract revupFrom(time: TreeClock): Promise<Observable<DeltaMessage> | undefined>;

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

export function comesAlive(meld: Pick<Meld, 'live'>, expected: boolean | null = true): Promise<boolean | null> {
  return meld.live.pipe(first(live => live === expected)).toPromise();
};