import { Meld, Snapshot, DeltaMessage, MeldJournalEntry } from './m-ld';
import { TreeClock } from './clocks';
import { Hash } from './hash';
import { Observable, Subject as Source, BehaviorSubject, asapScheduler } from 'rxjs';
import { observeOn, tap, distinctUntilChanged, first, skip, filter } from 'rxjs/operators';
import { LogLevelDesc, Logger, getLogger, getLoggers } from 'loglevel';

export abstract class AbstractMeld<M extends DeltaMessage> implements Meld {
  readonly updates: Observable<M>;
  readonly online: Observable<boolean | null>;

  private readonly updateSource: Source<M> = new Source;
  private readonly onlineSource: Source<boolean | null> = new BehaviorSubject(null);
  protected readonly log: Logger;

  constructor(readonly id: string, logLevel: LogLevelDesc = 'info') {
    const loggerName = `${this.constructor.name}.${this.id}`;
    const loggerInitialised = loggerName in getLoggers();
    this.log = getLogger(loggerName);
    if (!loggerInitialised) {
      const originalFactory = this.log.methodFactory;
      this.log.methodFactory = (methodName, logLevel, loggerName) => {
        const method = originalFactory(methodName, logLevel, loggerName);
        return (...msg: any[]) => method.apply(undefined, [this.id, this.constructor.name].concat(msg));
      };
    }
    this.log.setLevel(logLevel);

    // Update notifications are delayed to ensure internal processing has priority
    this.updates = this.updateSource.pipe(observeOn(asapScheduler),
      tap(msg => this.log.debug('sending', msg)));

    // Online notifications are distinct, so only transitions are notified
    this.online = this.onlineSource.pipe(distinctUntilChanged());
    this.online.pipe(skip(1)).subscribe(online =>
      this.log.debug('is', online ? 'online' : 'offline'));
  }

  protected nextUpdate(update: M) {
    this.updateSource.next(update);
  }

  protected setOnline(online: boolean | null) {
    this.onlineSource.next(online);
  }

  protected isOnline(): Promise<boolean | null> {
    return isOnline(this);
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

export function isOnline(meld: Meld): Promise<boolean | null> {
  return meld.online.pipe(first()).toPromise();
};

export function comesOnline(meld: Meld, expected: boolean | null = true): Promise<unknown> {
  return meld.online.pipe(first(online => online === expected)).toPromise();
};