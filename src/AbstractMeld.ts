import { Meld, Snapshot, DeltaMessage } from './m-ld';
import { TreeClock } from './clocks';
import { Observable, Subject as Source, BehaviorSubject, asapScheduler } from 'rxjs';
import { observeOn, tap, distinctUntilChanged, first, skip } from 'rxjs/operators';
import { LogLevelDesc, Logger } from 'loglevel';
import { getIdLogger } from './util';

export abstract class AbstractMeld implements Meld {
  readonly updates: Observable<DeltaMessage>;
  readonly online: Observable<boolean | null>;

  private readonly updateSource: Source<DeltaMessage> = new Source;
  private readonly onlineSource: Source<boolean | null> = new BehaviorSubject(null);

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
  protected isOnline = (): Promise<boolean | null> => isOnline(this);
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
  }
}

export function isOnline(meld: Meld): Promise<boolean | null> {
  return meld.online.pipe(first()).toPromise();
};

export function comesOnline(meld: Meld, expected: boolean | null = true): Promise<unknown> {
  return meld.online.pipe(first(online => online === expected)).toPromise();
};