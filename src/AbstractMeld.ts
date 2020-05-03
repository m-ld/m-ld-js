import { Meld, Snapshot, DeltaMessage, MeldJournalEntry } from './m-ld';
import { TreeClock } from './clocks';
import { Hash } from './hash';
import { Observable, Subject as Source, BehaviorSubject, asapScheduler } from 'rxjs';
import { observeOn, tap, distinctUntilChanged, first, skip } from 'rxjs/operators';
import { LogLevelDesc, Logger, getLogger } from 'loglevel';

export abstract class AbstractMeld<M extends DeltaMessage> implements Meld {
  readonly updates: Observable<M>;
  readonly online: Observable<boolean>;

  private readonly updateSource: Source<M> = new Source;
  private readonly onlineSource: Source<boolean> = new BehaviorSubject(false);
  protected readonly log: Logger;

  constructor(readonly id: string, logLevel: LogLevelDesc = 'info') {
    // Update notifications are delayed to ensure internal processing has priority
    this.updates = this.updateSource.pipe(observeOn(asapScheduler),
      tap(msg => this.log.debug(`${this.id}: ${this.constructor.name} sending ${msg}`)));

    // Online notifications are distinct, so only transitions are notified
    this.online = this.onlineSource.pipe(distinctUntilChanged(),
      tap(online => this.log.debug(`${this.id}: ${this.constructor.name} is ${online ? 'online' : 'offline'}`)));

    this.log = getLogger(this.id);
    this.log.setLevel(logLevel);
  }

  protected nextUpdate(update: M) {
    this.updateSource.next(update);
  }

  protected setOnline(online: boolean) {
    this.onlineSource.next(online);
  }

  protected isOnline(): Promise<boolean> {
    return AbstractMeld.isOnline(this);
  }

  protected static isOnline(meld: Meld): Promise<boolean> {
    return meld.online.pipe(first()).toPromise();
  }

  abstract newClock(): Promise<TreeClock>;
  abstract snapshot(): Promise<Snapshot>;
  abstract revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined>;

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