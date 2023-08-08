import { Meld, OperationMessage, Revup, Snapshot } from '.';
import { LiveValue } from './api-support';
import { TreeClock } from './clocks';
import {
  asapScheduler, BehaviorSubject, concat, firstValueFrom, Observable, of, throwError
} from 'rxjs';
import { catchError, distinctUntilChanged, filter, observeOn, skip } from 'rxjs/operators';
import { Logger } from 'loglevel';
import { PauseableSource, throwOnComplete } from './util';
import { MeldError, MeldReadState } from '../api';
import { MeldConfig } from '../config';
import { MeldOperationMessage } from './MeldOperationMessage';
import { check } from './check';
import { getIdLogger } from './logging';

export const checkLive =
  check((m: Meld) => m.live.value === true,
    () => new MeldError('Meld is offline'));

export abstract class AbstractMeld implements Meld {
  readonly operations: Observable<OperationMessage>;
  readonly live: LiveValue<boolean | null>;

  private readonly operationSource = new PauseableSource<OperationMessage>();
  private readonly liveSource: BehaviorSubject<boolean | null> = new BehaviorSubject(null);

  private _closed = false;
  protected readonly log: Logger;
  protected errorIfClosed: Observable<never>;

  readonly id: string;
  readonly domain: string;

  protected constructor(config: MeldConfig) {
    this.id = config['@id'];
    this.domain = config['@domain'];
    this.log = getIdLogger(this.constructor, this.id, config.logLevel ?? 'info');

    // Operation notifications are delayed to ensure internal processing has priority
    this.operations = this.operationSource.pipe(observeOn(asapScheduler));

    // Live notifications are distinct, only transitions are notified; an error
    // indicates a return to undecided liveness followed by completion.
    this.live = Object.defineProperties(
      this.liveSource.pipe(catchError(() => of(null)), distinctUntilChanged()),
      { value: { get: () => this.liveSource.value } }) as LiveValue<boolean | null>;

    // Log liveness
    this.live.pipe(skip(1)).subscribe(
      live => this.log.debug('is', live == null ? 'gone' : live ? 'live' : 'dead'));

    this.errorIfClosed = throwOnComplete(this.live,
      () => new MeldError('Clone has closed'));
  }

  protected nextOperation = (op: OperationMessage, reason: string) => {
    this.log.debug('has operation', reason, this.msgString(op));
    this.operationSource.next(op);
  };
  protected pauseOperations = (until: PromiseLike<unknown>) => this.operationSource.pause(until);

  protected setLive(live: boolean | null) {
    return this.liveSource.next(live);
  };

  protected get closed() {
    return this._closed;
  }

  abstract newClock(): Promise<TreeClock>;
  abstract snapshot(state: MeldReadState): Promise<Snapshot>;
  abstract revupFrom(time: TreeClock, state: MeldReadState): Promise<Revup | undefined>;

  protected msgString(msg: OperationMessage) {
    return MeldOperationMessage.toString(msg, this.log.getLevel());
  }

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
  meld: Pick<Meld, 'live'>,
  expected: boolean | null | 'notNull' = true
): Promise<boolean | null> {
  return firstValueFrom(concat(
    meld.live.pipe(
      filter(expected === 'notNull' ? (live => live != null) : (live => live === expected))
    ),
    throwError(() => new MeldError('Clone has closed'))
  ));
}