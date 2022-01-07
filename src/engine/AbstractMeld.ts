import { Meld, OperationMessage, Revup, Snapshot } from '.';
import { LiveValue } from './LiveValue';
import { TreeClock } from './clocks';
import { asapScheduler, BehaviorSubject, firstValueFrom, Observable, of } from 'rxjs';
import { catchError, distinctUntilChanged, filter, observeOn, skip, tap } from 'rxjs/operators';
import { Logger } from 'loglevel';
import { check, getIdLogger, PauseableSource } from './util';
import { MeldError } from './MeldError';
import { MeldReadState } from '../api';
import { MeldConfig } from '../config';

export abstract class AbstractMeld implements Meld {
  protected static checkLive =
    check((m: AbstractMeld) => m.live.value === true,
      () => new MeldError('Meld is offline'));
  protected static checkNotClosed =
    check((m: AbstractMeld) => !m.closed,
      () => new MeldError('Clone has closed'));

  readonly operations: Observable<OperationMessage>;
  readonly live: LiveValue<boolean | null>;

  private readonly operationSource = new PauseableSource<OperationMessage>();
  private readonly liveSource: BehaviorSubject<boolean | null> = new BehaviorSubject(null);

  private _closed = false;
  protected readonly log: Logger;

  readonly id: string;
  readonly domain: string;

  protected constructor(config: MeldConfig) {
    this.id = config['@id'];
    this.domain = config['@domain'];
    this.log = getIdLogger(this.constructor, this.id, config.logLevel ?? 'info');

    // Operation notifications are delayed to ensure internal processing has priority
    this.operations = this.operationSource.pipe(observeOn(asapScheduler), tap(msg => {
      this.log.debug('has operation', msg.toString(this.log.getLevel()));
    }));

    // Live notifications are distinct, only transitions are notified; an error
    // indicates a return to undecided liveness followed by completion.
    this.live = Object.defineProperties(
      this.liveSource.pipe(catchError(() => of(null)), distinctUntilChanged()),
      { value: { get: () => this.liveSource.value } }) as LiveValue<boolean | null>;

    // Log liveness
    this.live.pipe(skip(1)).subscribe(
      live => this.log.debug('is', live == null ? 'gone' : live ? 'live' : 'dead'));
  }

  protected nextOperation = (op: OperationMessage) => this.operationSource.next(op);
  protected pauseOperations = (until: PromiseLike<unknown>) => this.operationSource.pause(until);

  protected warnError = (err: any) => this.log.warn(err);

  protected setLive(live: boolean | null) {
    return this.liveSource.next(live);
  };

  protected get closed() {
    return this._closed;
  }

  abstract newClock(): Promise<TreeClock>;
  abstract snapshot(state: MeldReadState): Promise<Snapshot>;
  abstract revupFrom(time: TreeClock, state: MeldReadState): Promise<Revup | undefined>;

  close(err?: any) {
    this._closed = true;
    if (err) {
      this.operationSource.error(err);
      this.liveSource.error(err);
    } else {
      this.operationSource.complete();
      this.liveSource.complete();
    }
  }
}

export function comesAlive(
  meld: Pick<Meld, 'live'>, expected: boolean | null | 'notNull' = true): Promise<boolean | null> {
  return firstValueFrom(meld.live.pipe(filter(
    expected === 'notNull' ? (live => live != null) : (live => live === expected))));
}