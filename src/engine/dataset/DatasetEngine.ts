import { LiveStatus, MeldStatus, MeldConstraint } from '../../api';
import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal, Revup, Recovery } from '..';
import { liveRollup } from "../LiveValue";
import { Subject, Read, Write } from '../../jrql-support';
import {
  Observable, merge, from, EMPTY,
  concat, BehaviorSubject, Subscription, interval, of, Subscriber, OperatorFunction
} from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import {
  publishReplay, refCount, filter, takeUntil, tap,
  finalize, toArray, map, debounceTime,
  distinctUntilChanged, expand, delayWhen, take, skipWhile, ignoreElements
} from 'rxjs/operators';
import { delayUntil, Future, tapComplete, fromArrayPromise } from '../util';
import { LockManager } from "../locks";
import { levels } from 'loglevel';
import { AbstractMeld, comesAlive } from '../AbstractMeld';
import { MeldConfig } from '../..';
import { RemoteUpdates } from './RemoteUpdates';
import { NO_CONSTRAINT } from '../../constraints';
import { CloneEngine } from '../StateEngine';
import { MeldError } from '../MeldError';

enum ConnectStyle {
  SOFT, HARD
}

export class DatasetEngine extends AbstractMeld implements CloneEngine, MeldLocal {
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: DeltaMessage[] = [];
  private readonly bufferingDeltas: Observable<unknown>;
  private readonly remotes: Omit<MeldRemotes, 'updates'>;
  private readonly remoteUpdates: RemoteUpdates;
  private subs = new Subscription;
  readonly lock = new LockManager<'live' | 'state'>();
  // FIXME: New clone flag should be inferred from the journal (e.g. tail has no
  // delta) in case of crash between new clock and first snapshot
  private newClone: boolean = false;
  private readonly latestTicks = new BehaviorSubject<number>(NaN);
  private readonly networkTimeout: number;
  private readonly genesisClaim: boolean;

  constructor({ dataset, remotes, constraint, config }: {
    dataset: Dataset;
    remotes: MeldRemotes;
    constraint?: MeldConstraint;
    config: MeldConfig;
  }) {
    super(config['@id'], config.logLevel);
    this.dataset = new SuSetDataset(dataset, constraint ?? NO_CONSTRAINT, config);
    this.subs.add(this.dataUpdates
      .pipe(map(update => update['@ticks']))
      .subscribe(this.latestTicks));
    this.remotes = remotes;
    this.remotes.setLocal(this);
    this.remoteUpdates = new RemoteUpdates(remotes);
    this.networkTimeout = config.networkTimeout ?? 5000;
    this.genesisClaim = config.genesis;
    this.subs.add(this.status.subscribe(status => this.log.debug(status)));
    // Create an observable that emits if deltas are being chronically buffered.
    // That is, if the buffer has been filling for longer than the network
    // timeout. This observable is subscribed in the initialise() method.
    this.bufferingDeltas = this.remoteUpdates.receiving.pipe(
      map(delta => this.acceptRemoteDelta(delta)),
      // From this point we are only interested in buffered messages
      filter(accepted => !accepted),
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
      this.newClone = !this.genesisClaim; // New clone means non-genesis
      time = await this.dataset.saveClock(async () =>
        this.genesisClaim ? TreeClock.GENESIS : await this.remotes.newClock(), true);
    }
    this.log.info('has time', time);
    this.messageService = new TreeClockMessageService(time);
    this.latestTicks.next(time.ticks);

    // Revving-up will inject missed messages so the ordering buffer is
    // redundant when outdated, even if the remotes were previously attached.
    this.subs.add(this.remoteUpdates.outdated.subscribe(outdated => {
      if (outdated && this.orderingBuffer.length > 0) {
        this.log.info(`Discarding ${this.orderingBuffer.length} items from ordering buffer`);
        this.orderingBuffer.length = 0;
      }
    }));

    // Create a stream of 'opportunities' to decide our liveness.
    // Each opportunity is a ConnectStyle: HARD to force re-connect.
    // The stream errors/completes with the remote updates.
    this.subs.add(merge(
      // 1. Changes to the liveness of the remotes. This emits the current
      //    liveness, but we don't use it because the value might have changed
      //    by the time we get the lock.
      this.remotes.live,
      // 2. Chronic buffering of messages
      this.bufferingDeltas
    ).pipe(
      // 3. Last attempt can generate more attempts (delay if soft)
      expand(() => this.decideLive()
        .pipe(delayWhen(this.reconnectDelayer)))
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

  private reconnectDelayer = (style: ConnectStyle): Observable<number> => {
    switch (style) {
      case ConnectStyle.HARD:
        // Hard retry is immediate
        return of(0);
      case ConnectStyle.SOFT:
        // Soft retry is a distribution around ~2 * network timeout
        let delayMultiple = 0; // will be >= 1, most probably around 2
        for (let p = 1.0; p > 0.3; p *= Math.random())
          delayMultiple++;
        return interval(delayMultiple * Math.random() * this.networkTimeout);
    }
  }

  get dataUpdates() {
    return this.dataset.updates;
  }

  private get isGenesis(): boolean {
    return this.localTime.isId;
  }

  private get localTime() {
    return this.messageService.peek();
  }

  private acceptRemoteDelta(delta: DeltaMessage): boolean {
    const logBody = this.log.getLevel() < levels.DEBUG ? delta : `tid: ${delta.data[1]}`;
    this.log.debug('Receiving', logBody);
    // If we buffer a message, return false to signal we might need a re-connect
    return this.messageService.receive(delta, this.orderingBuffer, (msg, prevTime) => {
      // Check that we have the previous message from this clock ID
      const expectedPrev = prevTime.getTicks(msg.time);
      // TODO if (journalPrevTicks >= msg.time.ticks) we've seen this before
      if (msg.prev !== expectedPrev)
        throw new MeldError('Update out of order', `
        Update claims prev is ${msg.prev} @ ${msg.time},
        but local clock was ${expectedPrev} @ ${prevTime}`);
      this.log.debug('Accepting', logBody);
      // Need exclusive access to the state, per CloneEngine contract
      this.lock.exclusive('state', async () => {
        // Get the event time just before transacting the change
        const arrivalTime = this.messageService.event();
        // Make an extra clock tick available for constraints.
        this.messageService.event();
        const cxUpdate = await this.dataset.apply(msg, arrivalTime, this.localTime);
        if (cxUpdate != null)
          this.nextUpdate(cxUpdate);
        msg.delivered.resolve();
      }).catch(err => this.close(err));
    });
  }

  /**
   * @param style `ConnectStyle.HARD` to force a re-connect regardless of
   * liveness
   * @returns Zero or one retry indication. If zero, success. If a value is
   * emitted, it indicates failure, and the value is whether the re-connect
   * should be hard.
   */
  private decideLive(): Observable<ConnectStyle> {
    return new Observable(retry => {
      // Block transactions, revups and other connect attempts while handling
      // liveness change.
      this.lock.exclusive('live', async () => {
        const remotesLive = this.remotes.live.value;
        if (remotesLive === true) {
          if (this.isGenesis)
            throw new Error('Genesis clone trying to join a live domain.');
          // Connect in the live lock
          await this.connect(retry);
          this.setLive(true);
        } else {
          // Stop receiving updates until re-connect.
          this.remoteUpdates.detach();
          if (remotesLive === false) {
            // We are the silo, the last survivor.
            if (this.newClone)
              throw new Error('New clone is siloed.');
            // Stay live for any newcomers to rev-up from us.
            this.setLive(true);
            retry.complete();
          } else if (remotesLive === null) {
            // We are partitioned from the domain.
            this.setLive(false);
            retry.complete();
          }
        }
      }).catch(err => retry.error(err));
    });
  }

  /**
   * @param retry to be notified of collaboration completion
   * @see decideLive return value
   * @param style `ConnectStyle.HARD` to force the connect even if already live
   */
  private async connect(retry: Subscriber<ConnectStyle>) {
    this.log.info(this.newClone ? 'new clone' :
      this.live.value === true && this.remotes.live.value === false ? 'silo' : 'clone',
      'connecting to remotes');
    try {
      if (this.newClone || !(await this.requestRevup(retry)))
        await this.requestSnapshot(retry);
    } catch (err) {
      this.log.info('Cannot connect to remotes due to', err);
      /*
      An error could indicate that:
      1. The remotes have gone offline during our connection attempt. If they
         have reconnected, another attempt will have already been queued on the
         connect lock.
      2. A candidate collaborator timed-out. This could happen if we are
         mutually requesting rev-ups, for example if we both believe we are the
         silo. Hence the jitter on the soft reconnect, see this.softDelay.
      */
      retry.next(ConnectStyle.SOFT);
      retry.complete();
    }
  }

  /**
   * This method returns async as soon as the revup has started. There may
   * still be updates incoming from the collaborator.
   * @param retry to be notified of collaboration completion
   * @see decideLive return value
   * @returns `true` if the rev-up request found a collaborator
   */
  private async requestRevup(retry: Subscriber<ConnectStyle>): Promise<boolean> {
    const revup = await this.remotes.revupFrom(this.localTime);
    if (revup) {
      this.log.info('revving-up from collaborator');
      // We don't wait until rev-ups have been completely delivered
      this.acceptRecoveryUpdates(revup.updates, retry);
      // Is there anything in our journal that post-dates the last revup?
      // Wait until those have been delivered, to preserve fifo.
      await this.emitOpsSince(revup);
      return true;
    }
    return false;
  }

  /**
   * This method returns async as soon as the snapshot is delivered. There may
   * still be updates incoming from the collaborator.
   * @param retry to be notified of collaboration completion
   * @see decideLive return value
   */
  private async requestSnapshot(retry: Subscriber<ConnectStyle>): Promise<unknown> {
    const snapshot = await this.remotes.snapshot();
    // We contractually have to subscribe to the snapshot streams on this tick,
    // so no awaits allowed until we injectRevups
    return this.acceptSnapshot(snapshot, retry);
  }

  // This helper method must not be async, see comment in requestSnapshot
  private acceptSnapshot(snapshot: Snapshot, retry: Subscriber<ConnectStyle>): Promise<unknown> {
    this.messageService.join(snapshot.lastTime);
    // If we have any operations since the snapshot: re-emit them now and
    // re-apply them to our own dataset when the snapshot is applied.
    /*
    FIXME: Holding this stuff in memory during a potentially long snapshot
    application is not scalable or safe.
    */
    const reEmits = this.emitOpsSince(snapshot, toArray());
    // Start applying the snapshot when we have done re-emitting
    const snapshotApplied = reEmits.then(() =>
      this.dataset.applySnapshot(snapshot, this.localTime));
    // Delay all updates until the snapshot has been fully applied
    // This is because a snapshot is applied in multiple transactions
    const updates = concat(
      snapshot.updates.pipe(delayUntil(from(snapshotApplied))),
      fromArrayPromise(reEmits));
    this.acceptRecoveryUpdates(updates, retry);
    return snapshotApplied; // We can go live as soon as the snapshot is applied
  }

  private async emitOpsSince<T = never>(
    recovery: Recovery, ret: OperatorFunction<DeltaMessage, T> = ignoreElements()): Promise<T> {
    if (this.newClone) {
      return EMPTY.pipe(ret).toPromise();
    } else {
      const recent = await this.dataset.operationsSince(recovery.lastTime);
      // If we don't have journal from our ticks on the collaborator's clock, this
      // will lose data! – Close and let the app decide what to do.
      if (recent == null)
        throw new MeldError('Clone outdated');
      else
        return recent.pipe(tap(this.nextUpdate), ret).toPromise();
    }
  }

  private acceptRecoveryUpdates(updates: Observable<DeltaMessage>, retry: Subscriber<ConnectStyle>) {
    this.remoteUpdates.attach(updates).then(() => {
      // If we were a new clone, we're up-to-date now
      this.log.info('connected');
      this.newClone = false;
      retry.complete();
    }, (err: any) => {
      // If rev-ups fail (for example, if the collaborator goes offline)
      // it's not a catastrophe but we do need to enqueue a retry
      this.log.warn('Rev-up did not complete due to', err);
      retry.next(ConnectStyle.HARD); // Force re-connect
      retry.complete();
    });
  }

  @AbstractMeld.checkNotClosed.async
  async newClock(): Promise<TreeClock> {
    const newClock = new Future<TreeClock>();
    await this.dataset.saveClock(gwc => {
      const lastPublicTick = gwc.getTicks(this.localTime);
      // Back-date the clock to the last public tick before forking
      const fork = this.localTime.forked(lastPublicTick);
      newClock.resolve(fork.right);
      // And re-apply the ticks to our local clock
      const localClock = fork.left.ticked(this.localTime.ticks - lastPublicTick);
      this.messageService.push(localClock);
      return localClock
    });
    return newClock;
  }

  @AbstractMeld.checkLive.async
  async snapshot(): Promise<Snapshot> {
    return this.lock.exclusive('live', async () => {
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
  async revupFrom(time: TreeClock): Promise<Revup | undefined> {
    return this.lock.exclusive('live', async () => {
      const sentOperations = new Future;
      const maybeMissed = this.remoteUpdatesBeforeNow(sentOperations);
      const lastTime = new Future<TreeClock>();
      const operations = await this.dataset.operationsSince(time, lastTime);
      if (operations)
        return {
          lastTime: await lastTime,
          updates: merge(
            operations.pipe(tapComplete(sentOperations), tap(msg =>
              this.log.debug('Sending rev-up', msg))),
            maybeMissed.pipe(delayUntil(from(sentOperations))))
        };
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
  read(request: Read): Observable<Subject> {
    return new Observable<Subject>(subs => {
      this.lock.share('live', () => new Promise(resolve =>
        // Only leave the live-lock when the results have been fully streamed
        this.dataset.read(request).pipe(finalize(resolve)).subscribe(subs)))
        .catch(err => subs.error(err)); // Only if lock fails
    });
  }

  @AbstractMeld.checkNotClosed.async
  async write(request: Write): Promise<unknown> {
    // For a write, execute immediately.
    return this.lock.share('live', async () => {
      // Take the send timestamp just before enqueuing the transaction. This
      // ensures that transaction stamps increase monotonically.
      const sendTime = this.messageService.event();
      const update = await this.dataset.transact(async () =>
        [sendTime, await this.dataset.write(request)]);
      // Publish the delta
      if (update != null)
        this.nextUpdate(update);
    });
  }

  get status(): Observable<MeldStatus> & LiveStatus {
    const stateRollup = liveRollup({
      live: this.live,
      remotesLive: this.remotes.live,
      outdated: this.remoteUpdates.outdated,
      ticks: this.latestTicks
    });
    const toStatus = (state: typeof stateRollup['value']): MeldStatus => ({
      online: state.remotesLive != null,
      // If genesis, never outdated.
      outdated: !this.isGenesis && state.outdated,
      silo: state.live === true && state.remotesLive === false,
      ticks: state.ticks
    });
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