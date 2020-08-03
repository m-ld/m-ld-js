import { Snapshot, DeltaMessage, MeldRemotes, MeldUpdate, MeldLocal, MeldClone, LiveStatus, MeldStatus } from '../m-ld';
import { liveRollup } from "../LiveValue";
import { Pattern, Subject, isRead, isSubject, isGroup, isUpdate } from './jrql-support';
import {
  Observable, merge, from, defer, EMPTY,
  concat, BehaviorSubject, Subscription, throwError, identity, interval, of, Subscriber
} from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import {
  publishReplay, refCount, filter, ignoreElements, takeUntil, tap,
  finalize, flatMap, toArray, map, debounceTime,
  distinctUntilChanged, expand, delayWhen, take, skipWhile
} from 'rxjs/operators';
import { delayUntil, Future, tapComplete, SharableLock, fromArrayPromise } from '../util';
import { levels } from 'loglevel';
import { MeldError } from '../m-ld/MeldError';
import { AbstractMeld, comesAlive } from '../AbstractMeld';
import { MeldConfig } from '..';
import { RemoteUpdates } from './RemoteUpdates';

enum ConnectStyle {
  SOFT, HARD
}

export class DatasetClone extends AbstractMeld implements MeldClone, MeldLocal {
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: DeltaMessage[] = [];
  private readonly remotes: Omit<MeldRemotes, 'updates'>;
  private readonly remoteUpdates: RemoteUpdates;
  private subs = new Subscription;
  private readonly liveLock = new SharableLock;
  private newClone: boolean = false;
  private readonly latestTicks = new BehaviorSubject<number>(NaN);
  private readonly networkTimeout: number;
  private readonly genesisClaim: boolean;

  constructor(
    dataset: Dataset,
    remotes: MeldRemotes,
    config: MeldConfig) {
    super(config['@id'], config.logLevel);
    this.dataset = new SuSetDataset(dataset, config);
    this.subs.add(this.dataset.updates
      .pipe(map(update => update['@ticks']))
      .subscribe(this.latestTicks));
    this.remotes = remotes;
    this.remotes.setLocal(this);
    this.remoteUpdates = new RemoteUpdates(remotes);
    this.networkTimeout = config.networkTimeout ?? 5000;
    this.genesisClaim = config.genesis;
    this.subs.add(this.status.subscribe(status => this.log.debug(status)));
  }

  /**
   * Must be called prior to making transactions against this clone. The
   * returned promise does not guarantee that the clone is live or up-to-date,
   * because it may be disconnected or still receiving recent updates from a
   * collaborator.
   *
   * An application may choose to delay its own initialisation until the latest
   * updates have either been received, using the {@link #latest} method.
   *
   * @return resolves when the clone can accept transactions
   */
  @AbstractMeld.checkNotClosed.async
  async initialise(): Promise<void> {
    await this.dataset.initialise();
    // Establish a clock for this clone
    let time = await this.dataset.loadClock();
    if (!time) {
      time = this.genesisClaim ? TreeClock.GENESIS : await this.remotes.newClock();
      this.newClone = !this.genesisClaim; // New clone means non-genesis
      await this.dataset.saveClock(time, true);
    }
    this.log.info(`has time ${time}`);
    this.messageService = new TreeClockMessageService(time);
    this.latestTicks.next(time.ticks);

    // Revving-up will inject missed messages so the ordering buffer is
    // redundant, even if the remotes were previously attached.
    this.subs.add(this.remoteUpdates.state.subscribe(next => {
      if (next.revvingUp && this.orderingBuffer.length > 0) {
        this.log.info(`Discarding ${this.orderingBuffer.length} items from ordering buffer`);
        this.orderingBuffer.length = 0;
      }
    }));

    // Create a stream of 'opportunities' to decide our liveness.
    // Each opportunity is a ConnectStyle: HARD to force re-connect.
    // The stream errors/completes with the remote updates.
    this.subs.add(merge(
      // 1. Changes to the liveness of the remotes (never hard). This emits the
      //    current liveness, but we don't use it because the value might have
      //    changed by the time we get the lock.
      this.remotes.live.pipe(map(() => ConnectStyle.SOFT)),
      // 2. Chronic buffering of messages (always hard)
      this.bufferingDeltas().pipe(map(() => ConnectStyle.HARD))
    ).pipe(
      // 3. Last attempt can generate more attempts (delay if soft)
      expand(style => this.decideLive(style).pipe(
        delayWhen(retry => retry == ConnectStyle.HARD ?
          of(0) : interval(this.networkTimeout))))
    ).subscribe({
      error: err => this.close(err),
      complete: () => this.close()
    }));

    if (this.newClone)
      // For a new non-genesis clone, the first connect is essential.
      await comesAlive(this);
    else
      // For any other clone, just wait for decided liveness.
      await comesAlive(this, 'notNull');
  }

  private get isGenesis(): boolean {
    return this.localTime.isId;
  }

  private get localTime() {
    return this.messageService.peek();
  }

  /**
   * @returns an observable that emits if deltas are being chronically buffered.
   * That is, if the buffer has been filling for longer than the network timeout.
   */
  private bufferingDeltas(): Observable<unknown> {
    return this.remoteUpdates.receiving.pipe(
      map(delta => !this.acceptRemoteDelta(delta)),
      // From this point we are only interested in buffered messages
      filter(identity),
      // Wait for the network timeout in case the buffer clears
      debounceTime(this.networkTimeout),
      // After the debounce, check if still buffering
      filter(() => {
        if (this.orderingBuffer.length > 0) {
          // We're missing messages that have been received by others.
          // Let's re-connect to see if we can get back on track.
          this.log.warn('Messages are out of order and backing up. Re-connecting.');
          return true;
        }
        else {
          this.log.debug('Messages were out of order, but now cleared.');
          return false;
        }
      }));
  }

  private acceptRemoteDelta(delta: DeltaMessage) {
    const logBody = this.log.getLevel() < levels.DEBUG ? delta : `tid: ${delta.data.tid}`;
    this.log.debug('Receiving', logBody, '@', this.localTime);
    // If we buffer a message, return true to signal we might need a re-connect
    return this.messageService.receive(delta, this.orderingBuffer, msg => {
      this.log.debug('Accepting', logBody);
      this.dataset.apply(msg.data, msg.time, this.localTime)
        .then(msg.delivered.resolve)
        .catch(err => this.close(err));
    });
  }

  setLive(live: boolean) {
    if (live)
      this.remoteUpdates.attach();
    else
      this.remoteUpdates.detach();

    super.setLive(live);
  }

  /**
   * @param style `ConnectStyle.HARD` to force a re-connect regardless of
   * liveness
   * @returns Zero or one retry indication. If zero, success. If a value is
   * emitted, it indicates failure, and the value is whether the re-connect
   * should be hard.
   */
  private decideLive(style: ConnectStyle): Observable<ConnectStyle> {
    return new Observable(retrySubscriber => {
      // Block transactions, revups and other connect attempts while handling
      // liveness change.
      this.liveLock.acquire(this.id, async () => {
        const remotesLive = this.remotes.live.value;
        if (remotesLive === true) {
          if (this.isGenesis)
            throw new Error('Genesis clone trying to join a live domain.');
          // Connect in the live lock
          this.connect(style, retrySubscriber);
        } else if (remotesLive === false) {
          if (this.newClone)
            throw new Error('New clone is siloed.');
          // We are a silo, the last survivor. Stay live for any newcomers.
          this.setLive(true);
          retrySubscriber.complete();
        } else if (remotesLive === null) {
          // We are partitioned from the domain.
          this.setLive(false);
          retrySubscriber.complete();
        }
      }).catch(err => retrySubscriber.error(err));
    });
  }

  /**
   * @param retrySubscriber to be notified of collaboration completion
   * @see decideLive return value
   * @param style `ConnectStyle.HARD` to force the connect even if already live
   */
  private async connect(style: ConnectStyle, retrySubscriber: Subscriber<ConnectStyle>) {
    this.log.info('Connecting to remotes');
    try {
      // If silo (already live), no rev-up to do
      if (style == ConnectStyle.SOFT && this.live.value) {
        this.log.info('Silo attaching to domain');
        this.remoteUpdates.attach();
      } else if (this.newClone) {
        this.log.info('New clone requesting snapshot');
        await this.requestSnapshot(retrySubscriber);
      } else {
        await this.tryRevup(retrySubscriber);
      }
      this.log.info('connected.');
      this.setLive(true);
    } catch (err) {
      // This could indicate that the remotes have gone offline during our
      // connection attempt. If they have reconnected, another attempt will have
      // already been queued on the connect lock.
      this.log.info('Cannot connect to remotes due to', err);
      retrySubscriber.next(ConnectStyle.SOFT); // Schedule re-connect, but don't force
      retrySubscriber.complete();
    }
  }

  /**
   * This method returns async as soon as the revup has started. There may
   * still be updates incoming from the collaborator.
   * @param retrySubscriber to be notified of collaboration completion
   * @see decideLive return value
   */
  private async tryRevup(retrySubscriber: Subscriber<ConnectStyle>) {
    // For a new clone, last hash is random so this will return undefined
    const revup = await this.remotes.revupFrom(this.localTime);
    if (revup) {
      this.log.info('revving-up from collaborator');
      this.remoteUpdates.injectRevups(revup).then(async lastRevup => {
        // Emit anything in our journal that post-dates the last revup
        const recent = lastRevup && await this.dataset.operationsSince(lastRevup.time);
        if (recent)
          this.subs.add(recent.subscribe(this.nextUpdate, this.warnError));
        // Done revving-up
        retrySubscriber.complete();
      }).catch(err => this.onRevupFailed(retrySubscriber, err));
    } else {
      this.log.info('cannot rev-up, requesting snapshot');
      await this.requestSnapshot(retrySubscriber);
    }
  }

  /**
   * This method returns async as soon as the snapshot is delivered. There may
   * still be updates incoming from the collaborator.
   * @param retrySubscriber to be notified of collaboration completion
   * @see decideLive return value
   */
  private async requestSnapshot(retrySubscriber: Subscriber<ConnectStyle>) {
    const snapshot = await this.remotes.snapshot();
    // We contractually have to subscribe to the snapshot streams on this tick,
    // so no awaits allowed intil we injectRevups below.

    this.messageService.join(snapshot.lastTime);
    // If we have any operations since the snapshot: re-emit them now and
    // re-apply them to our own dataset when the snapshot is applied.
    const reEmits = this.dataset.operationsSince(snapshot.lastTime)
      .then(recent => (recent ?? EMPTY).pipe(tap(this.nextUpdate), toArray()).toPromise());
    // Start delivering the snapshot when we have done re-emitting
    const delivered = reEmits.then(() =>
      this.dataset.applySnapshot(snapshot, this.localTime));
    // Delay all updates until the snapshot has been fully delivered
    // This is because a snapshot is applied in multiple transactions
    const updates = snapshot.updates.pipe(delayUntil(from(delivered)));
    this.remoteUpdates.injectRevups(concat(updates, fromArrayPromise(reEmits)))
      .then(() => {
        // If we were a new clone, we're up-to-date now
        this.newClone = false;
        retrySubscriber.complete();
      })
      .catch(err => this.onRevupFailed(retrySubscriber, err));
    return delivered; // We can go live as soon as the snapshot is delivered
  }

  /**
   * @param retrySubscriber to be notified of collaboration completion
   * @param err the error that occured during rev-up
   * @see decideLive return value
   */
  private onRevupFailed(retrySubscriber: Subscriber<ConnectStyle>, err: any) {
    // If rev-ups fail (for example, if the collaborator goes offline)
    // it's not a catastrophe but we do need to enqueue a retry
    this.log.warn('Rev-up did not complete due to', err);
    retrySubscriber.next(ConnectStyle.HARD); // Force re-connect
    retrySubscriber.complete();
  };

  @AbstractMeld.checkNotClosed.async
  async newClock(): Promise<TreeClock> {
    const newClock = this.messageService.fork();
    // Forking is a mutation operation, need to save the new clock state
    await this.dataset.saveClock(this.localTime);
    return newClock;
  }

  @AbstractMeld.checkLive.async
  async snapshot(): Promise<Snapshot> {
    return this.liveLock.acquire(this.id, async () => {
      this.log.info('Compiling snapshot');
      const sentSnapshot = new Future;
      const updates = this.remoteUpdatesBeforeNow(sentSnapshot);
      const snapshot = await this.dataset.takeSnapshot();
      return {
        ...snapshot, updates,
        // Snapshotting holds open a transaction, so buffer/replay triples
        quads: snapshot.quads.pipe(publishReplay(), refCount(), tapComplete(sentSnapshot))
      };
    });
  }

  @AbstractMeld.checkLive.async
  async revupFrom(time: TreeClock): Promise<Observable<DeltaMessage> | undefined> {
    return this.liveLock.acquire(this.id, async () => {
      const sentOperations = new Future;
      const maybeMissed = this.remoteUpdatesBeforeNow(sentOperations);
      const operations = await this.dataset.operationsSince(time);
      if (operations)
        return merge(
          operations.pipe(tapComplete(sentOperations), tap(msg =>
            this.log.debug('Sending rev-up', msg))),
          maybeMissed.pipe(delayUntil(from(sentOperations))));
    });
  }

  private remoteUpdatesBeforeNow(until: PromiseLike<void>): Observable<DeltaMessage> {
    if (this.orderingBuffer.length)
      this.log.info(`Emitting ${this.orderingBuffer.length} from ordering buffer`);
    const now = this.localTime;
    return merge(
      // #1 Anything currently in our ordering buffer
      from(this.orderingBuffer),
      // #2 Anything that arrives stamped prior to now
      this.remoteUpdates.receiving.pipe(
        filter(message => message.time.anyLt(now, 'includeIds')),
        takeUntil(from(until)))).pipe(tap(msg =>
          this.log.debug('Sending update', msg)));
  }

  @AbstractMeld.checkNotClosed.rx
  transact(request: Pattern): Observable<Subject> {
    if (isRead(request)) {
      // For a read, every subscriber re-runs the query
      // TODO: Wire up unsubscribe (cancel cursor)
      return defer(() => this.liveLock.enter(this.id))
        .pipe(flatMap(() => this.dataset.read(request)),
          finalize(() => this.liveLock.leave(this.id)));
    } else if (isSubject(request) || isGroup(request) || isUpdate(request)) {
      // For a write, execute immediately.
      return from(this.liveLock.enter(this.id)
        .then(() => {
          // Take the send timestamp just before enqueuing the transaction. This
          // ensures that transaction stamps increase monotonically.
          const sendTime = this.messageService.send();
          return this.dataset.transact(async () => {
            const patch = await this.dataset.write(request);
            return [sendTime, patch];
          });
        })
        // Publish the delta
        .then(this.nextUpdate)
        .finally(() => this.liveLock.leave(this.id)))
        .pipe(ignoreElements()); // Ignores the void promise result
    } else {
      return throwError(new MeldError('Pattern is not read or writeable'));
    }
  }

  get status(): Observable<MeldStatus> & LiveStatus {
    const stateRollup = liveRollup({
      live: this.live,
      remotesLive: this.remotes.live,
      remoteState: this.remoteUpdates.state,
      ticks: this.latestTicks
    });
    const toStatus = (state: typeof stateRollup['value']): MeldStatus => {
      const silo = state.live === true && state.remotesLive === false;
      return ({
        online: state.remotesLive != null,
        // If genesis, never outdated.
        // If revving-up, always outdated.
        // If detached, outdated if not silo.
        outdated: !this.isGenesis &&
          (state.remoteState.revvingUp ||
            (!state.remoteState.attached && !silo)),
        silo, ticks: state.ticks
      });
    };
    const matchStatus = (status: MeldStatus, match?: Partial<MeldStatus>) =>
      (match?.online === undefined || match.online === status.online) &&
      (match?.outdated === undefined || match.outdated === status.outdated) &&
      (match?.silo === undefined || match.silo === status.silo);
    const values = stateRollup.pipe(
      skipWhile(() => this.messageService == null),
      map(toStatus),
      distinctUntilChanged<MeldStatus>(matchStatus));
    const becomes = async (match?: Partial<MeldStatus>) =>
      values.pipe(filter(status => matchStatus(status, match)), take(1)).toPromise();
    return Object.defineProperties(values, {
      becomes: { value: becomes },
      value: { get: () => toStatus(stateRollup.value) }
    });
  }

  follow(): Observable<MeldUpdate> {
    return this.dataset.updates;
  }

  @AbstractMeld.checkNotClosed.async
  async close(err?: any) {
    if (err)
      this.log.warn('Shutting down due to', err);
    else
      this.log.info('Shutting down normally');

    // Make sure we never receive another remote update
    this.subs.unsubscribe();
    this.remoteUpdates.close(err);
    this.remotes.setLocal(null);

    if (this.orderingBuffer.length) {
      this.log.warn(`closed with ${this.orderingBuffer.length} items in ordering buffer
      first: ${this.orderingBuffer[0]}
      time: ${this.localTime}`);
    }
    super.close(err);
    await this.dataset.close(err);
  }
}
