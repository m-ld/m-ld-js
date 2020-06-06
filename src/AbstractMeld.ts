import { Meld, Snapshot, DeltaMessage } from './m-ld';
import { TreeClock } from './clocks';
import { Observable, Subject as Source, BehaviorSubject, asapScheduler } from 'rxjs';
import { observeOn, tap, distinctUntilChanged, first, skip } from 'rxjs/operators';
import { LogLevelDesc, Logger } from 'loglevel';
import { getIdLogger, check } from './util';
import { MeldError, IS_OFFLINE, IS_CLOSED } from './m-ld/MeldError';

export abstract class AbstractMeld implements Meld {
  protected static checkOnline =
    check((m: AbstractMeld) => m.isOnline() === true, () => new MeldError(IS_OFFLINE));
  protected static checkNotClosed =
    check((m: AbstractMeld) => !m.closed, () => new MeldError(IS_CLOSED));

  readonly updates: Observable<DeltaMessage>;
  readonly online: Observable<boolean | null>;

  private readonly updateSource: Source<DeltaMessage> = new Source;
  private readonly onlineSource: BehaviorSubject<boolean | null> = new BehaviorSubject(null);
  private closed = false;

  protected readonly log: Logger;

  constructor(readonly id: string, logLevel: LogLevelDesc = 'info') {
    this.log = getIdLogger(this.constructor, id, logLevel);

    // Update notifications are delayed to ensure internal processing has priority
    this.updates = this.updateSource.pipe(observeOn(asapScheduler),
      tap(msg => this.log.debug('has update', msg)));

    // Online notifications are distinct, so only transitions are notified
    this.online = this.onlineSource.pipe(distinctUntilChanged());
    this.online.pipe(skip(1)).subscribe(online =>
      this.log.debug('is', online ? 'online' : 'offline'));
  }

  protected nextUpdate = (update: DeltaMessage) => this.updateSource.next(update);
  protected setOnline = (online: boolean | null) => this.onlineSource.next(online);
  protected isOnline = (): boolean | null => this.onlineSource.value;
  protected warnError = (err: any) => this.log.warn(err);

  abstract newClock(): Promise<TreeClock>;
  abstract snapshot(): Promise<Snapshot>;
  abstract revupFrom(time: TreeClock): Promise<Observable<DeltaMessage> | undefined>;

  close(err?: any) {
    if (err) {
      this.updateSource.error(err);
      this.onlineSource.error(err);
    } else {
      this.updateSource.complete();
      this.onlineSource.complete();
    }
    this.closed = true;
  }
}

export function isOnline(meld: Pick<Meld, 'online'>): Promise<boolean | null> {
  return meld.online.pipe(first()).toPromise();
};

export function comesOnline(meld: Meld, expected: boolean | null = true): Promise<unknown> {
  return meld.online.pipe(first(online => online === expected)).toPromise();
};