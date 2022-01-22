import {
  GraphSubject, LiveStatus, MeldExtensions, MeldReadState, MeldStatus, StateProc
} from '../../api';
import { MeldLocal, MeldRemotes, OperationMessage, Recovery, Revup, Snapshot } from '..';
import { liveRollup } from '../LiveValue';
import { Context, Pattern, Query, Read, Write } from '../../jrql-support';
import {
  BehaviorSubject, concat, concatMap, defaultIfEmpty, EMPTY, firstValueFrom, from, interval, merge,
  Observable, of, OperatorFunction, partition, Subscriber, Subscription
} from 'rxjs';
import { GlobalClock, TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import {
  debounceTime, delayWhen, distinctUntilChanged, expand, filter, finalize, ignoreElements, map,
  share, skipWhile, takeUntil, tap, toArray
} from 'rxjs/operators';
import { check, delayUntil, Future, inflateFrom, poisson, Stopwatch, tapComplete } from '../util';
import { LockManager } from '../locks';
import { levels } from 'loglevel';
import { AbstractMeld, comesAlive } from '../AbstractMeld';
import { RemoteOperations } from './RemoteOperations';
import { CloneEngine } from '../StateEngine';
import { MeldError, MeldErrorStatus } from '../MeldError';
import { AsyncIterator, TransformIterator, wrap } from 'asynciterator';
import { BaseStream } from '../../rdfjs-support';
import { Consumable } from 'rx-flowable';
import { MeldConfig } from '../../config';

enum ConnectStyle {
  SOFT, HARD
}

enum OperationOutcome {
  /** Operation was accepted (and may have precipitated un-buffering) */
  ACCEPTED,
  /** Operation was buffered in expectation of causal operations */
  BUFFERED,
  /** Operation was unacceptable due to missing prior operations */
  DISORDERED
}

export class DatasetEngine extends AbstractMeld implements CloneEngine, MeldLocal {
  protected static checkStateLocked =
    check((m: DatasetEngine) => m.lock.state('state') !== null,
      () => new MeldError('Unknown error', 'Clone state not locked'));

  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: OperationMessage[] = [];
  private readonly remotes: Omit<MeldRemotes, 'operations'>;
  private readonly remoteOps: RemoteOperations;
  private subs = new Subscription;
  /**
   * Lock ordering matters to prevent deadlock. If both keys are required,
   * 'state' must be acquired first.
   */
  readonly lock: LockManager<'state' | 'live'>;
  // FIXME: New clone flag should be inferred from the journal (e.g. tail has no
  // operation) in case of crash between new clock and first snapshot
  private newClone: boolean = false;
  private readonly latestTicks = new BehaviorSubject<number>(NaN);
  private readonly networkTimeout: number;
  private readonly genesisClaim: boolean;
  readonly status: LiveStatus;
  /*readonly*/
  match: CloneEngine['match'];
  /*readonly*/
  query: CloneEngine['query'];

  constructor({ dataset, remotes, extensions, config, context }: {
    dataset: Dataset;
    remotes: MeldRemotes;
    extensions: MeldExtensions;
    config: MeldConfig;
    context?: Context;
  }) {
    super(config);
    this.dataset = new SuSetDataset(dataset, context ?? {}, extensions, config);
    this.lock = dataset.lock;
    this.subs.add(this.dataUpdates
      .pipe(map(update => update['@ticks']))
      .subscribe(this.latestTicks));
    this.remotes = remotes;
    this.remoteOps = new RemoteOperations(remotes);
    this.networkTimeout = config.networkTimeout ?? 5000;
    this.genesisClaim = config.genesis;
    this.status = this.createStatus();
    this.subs.add(this.status.subscribe(status => this.log.debug(status)));
  }

  /**
   * Must be called prior to making transactions against this clone. The
   * returned promise does not guarantee that the clone is live or up-to-date,
   * because it may be disconnected or still receiving recent updates from a
   * collaborator.
   *
   * An application may choose to delay its own initialisation until the latest
   * updates have been received, using the {@link status} field.
   *
   * @return resolves when the clone can accept transactions
   */
  @DatasetEngine.checkNotClosed.async
  async initialise(sw?: Stopwatch): Promise<void> {
    try {
      sw?.next('init');
      await this.initDataset();

      this.remotes.setLocal(this);
      // Establish a clock for this clone
      sw?.next('load-clock');
      let time = await this.dataset.loadClock();
      if (!time) {
        this.newClone = !this.genesisClaim; // New clone means non-genesis
        sw?.next('reset-clock');
        time = this.genesisClaim ? TreeClock.GENESIS : await this.remotes.newClock();
        await this.dataset.resetClock(time);
      }
      this.log.info('has time', time);
      this.messageService = new TreeClockMessageService(time);
      this.latestTicks.next(time.ticks);

      // Revving-up will inject missed messages so the ordering buffer is
      // redundant when outdated, even if the remotes were previously attached.
      this.subs.add(this.remoteOps.outdated.subscribe(outdated => {
        if (outdated && this.orderingBuffer.length > 0) {
          this.log.info(`Discarding ${this.orderingBuffer.length} items from ordering buffer`);
          this.orderingBuffer.length = 0;
        }
      }));

      // Create a stream of 'opportunities' to decide our liveness, i.e.
      // re-connect. The stream errors/completes with the remote updates.
      this.subs.add(merge(
        // 1. Changes to the liveness of the remotes. This emits the current
        //    liveness, but we don't use it because the value might have changed
        //    by the time we get the lock.
        this.remotes.live,
        // 2. Chronic buffering of operations
        // 3. Disordered operations
        this.operationProblems
      ).pipe(
        // 4. Last attempt to connect can generate more attempts (delay if soft)
        expand(() => this.decideLive()
          .pipe(delayWhen(this.reconnectDelayer)))
      ).subscribe({
        error: err => this.close(err),
        complete: () => this.close()
      }));

      if (this.newClone) {
        sw?.next('comes-alive');
        // For a new non-genesis clone, the first connect is essential.
        await comesAlive(this);
      }
    } catch (e) {
      // Failed to initialise somehow – this is fatal
      await this.close(e);
      throw e;
    }
  }

  private async initDataset() {
    await this.dataset.initialise();
    const rdfSrc = this.dataset.readState;
    // Raw RDF methods just pass through to the dataset when its initialised
    this.match = this.wrapStreamFn(rdfSrc.match.bind(rdfSrc));
    // @ts-ignore - TS can't cope with overloaded query method
    this.query = this.wrapStreamFn(rdfSrc.query.bind(rdfSrc));
  }

  private reconnectDelayer = (style: ConnectStyle): Observable<number> => {
    switch (style) {
      case ConnectStyle.HARD:
        // Hard retry is immediate
        return of(0);
      case ConnectStyle.SOFT:
        // Soft retry is a distribution ~(>=0 mean 2) * network timeout
        return interval((poisson(2) + 1) * Math.random() * this.networkTimeout);
    }
  };

  get dataUpdates() {
    return this.dataset.updates;
  }

  private get isGenesis(): boolean {
    return this.localTime.isId;
  }

  private get localTime() {
    return this.messageService.peek();
  }

  /**
   * Creates observables that emit if
   * - operations are being chronically buffered or
   * - a operation is received out of order.
   *
   * The former emits if the buffer has been filling for longer than the network
   * timeout. This observables are subscribed in the initialise() method.
   */
  private get operationProblems(): Observable<OperationOutcome> {
    const acceptOutcomes = this.remoteOps.receiving.pipe(
      // Only try and accept one operation at a time
      concatMap(op => this.acceptRemoteOperation(op)),
      // Ensure that the merge below does not incur two subscribes
      share());
    const [disordered, maybeBuffering] = partition(acceptOutcomes,
      outcome => outcome === OperationOutcome.DISORDERED);
    // Disordered messages are an immediate problem, buffering only if chronic
    return merge(disordered, maybeBuffering.pipe(
      // Accepted messages need no action, we are only interested in buffered
      filter(outcome => outcome === OperationOutcome.BUFFERED),
      // Wait for the network timeout in case the buffer clears
      debounceTime(this.networkTimeout),
      // After the debounce, check if still buffering
      filter(() => {
        if (this.orderingBuffer.length > 0) {
          // We're missing messages that have been received by others.
          // Let's re-connect to see if we can get back on track.
          this.log.warn('Messages are out of order and backing up. Re-connecting.');
          return true;
        } else {
          this.log.debug('Messages were out of order, but now cleared.');
          return false;
        }
      }))).pipe(tap(() => this.remoteOps.detach('outdated')));
  }

  private async acceptRemoteOperation(op: OperationMessage): Promise<OperationOutcome> {
    const logBody = this.log.getLevel() < levels.DEBUG ? op : `${op.time}`;
    this.log.debug('Receiving', logBody);
    // Grab the state lock, per CloneEngine contract and to ensure that all
    // clock ticks are immediately followed by their respective transactions.
    return this.lock.exclusive('state', 'remote operation', async () => {
      try {
        const startTime = this.localTime;
        // Synchronously gather ticks for transaction applications
        const applications: [OperationMessage, TreeClock, TreeClock][] = [];
        const accepted = this.messageService.receive(op, this.orderingBuffer,
          (msg, prevTime) => {
            // Check that we have the previous message from this clock ID
            const ticksSeen = prevTime.getTicks(msg.time);
            if (msg.time.ticks <= ticksSeen) {
              // Already had this message.
              this.log.debug('Ignoring outdated', logBody);
              msg.delivered.resolve();
            } else if (msg.prev > ticksSeen) {
              // We're missing a message. Reset the clock and trigger a re-connect.
              this.messageService.push(startTime);
              const outOfOrder = new MeldError('Update out of order', `
              Update claims prev is ${msg.prev} (${msg.time}),
              but local clock was ${ticksSeen} (${prevTime})`);
              msg.delivered.reject(outOfOrder);
              throw outOfOrder;
            } else {
              this.log.debug('Accepting', logBody);
              // Get the event time just before transacting the change, making an
              // extra clock tick available for constraints.
              applications.push([msg, this.messageService.event(), this.messageService.event()]);
            }
          });
        // The applications will enqueue in order on the dataset's transaction lock
        await Promise.all(applications.map(
          async ([msg, localTime, cxnTime]) => {
            const cxOp = await this.dataset.apply(msg, localTime, cxnTime);
            if (cxOp != null)
              this.nextOperation(cxOp);
            msg.delivered.resolve();
          }));
        return accepted ? OperationOutcome.ACCEPTED : OperationOutcome.BUFFERED;
      } catch (err) {
        if (err instanceof MeldError && err.status === MeldErrorStatus['Update out of order']) {
          this.log.info(err.message);
          return OperationOutcome.DISORDERED;
        } else {
          throw err;
        }
      }
    });
  }

  /**
   * @returns Zero or one retry indication. If zero, success. If a value is
   * emitted, it indicates failure, and the value is whether the re-connect
   * should be hard. Emission of an error is catastrophic.
   */
  private decideLive(): Observable<ConnectStyle> {
    return new Observable(retry => {
      // As soon as a decision on liveness needs to be made, pause output
      // operations to mitigate against breaking fifo with emitOpsSince().
      this.pauseOperations(
        // Also block transactions, recovery requests and other connect attempts.
        this.lock.acquire('state', 'decide live', 'share').then(release =>
          this.lock.exclusive('live', 'decide live', async () => {
            const remotesLive = this.remotes.live.value;
            if (remotesLive === true) {
              if (this.isGenesis)
                throw new Error('Genesis clone trying to join a live domain.');
              // Connect in the live lock
              await this.connect(retry, release);
              this.setLive(true);
            } else {
              // Stop receiving operations until re-connect, do not change outdated
              this.remoteOps.detach();
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
          }).finally(release)).catch(err => retry.error(err)));
    });
  }

  /**
   * @param retry to be notified of collaboration completion
   * @param releaseState to be called when the locked state is no longer needed
   * @see decideLive return value
   */
  @DatasetEngine.checkStateLocked.async
  private async connect(retry: Subscriber<ConnectStyle>, releaseState: () => void) {
    this.log.info(this.newClone ? 'new clone' :
        this.live.value === true && this.remotes.live.value === false ? 'silo' : 'clone',
      'connecting to remotes');
    try {
      if (!this.newClone) {
        const revup = await this.remotes.revupFrom(this.localTime, this.dataset.readState);
        if (revup != null) {
          releaseState();
          await this.processRevup(revup, retry);
          return;
        }
        // Otherwise fall through to snapshot recovery
      }
      const snapshot = await this.remotes.snapshot(this.dataset.readState);
      releaseState();
      await this.processSnapshot(snapshot, retry);
    } catch (err) {
      this.log.info('Cannot connect to remotes due to', err);
      /*
      An error could indicate that:
      1. The remotes have gone offline during our connection attempt. If they
         have reconnected, another attempt will have already been queued on the
         connect lock.
      2. A candidate collaborator timed-out. This could happen if we are
         mutually requesting rev-ups, for example if we both believe we are the
         silo. Hence the jitter on the soft reconnect, see
         this.reconnectDelayer.
      */
      retry.next(ConnectStyle.SOFT);
      retry.complete();
    }
  }

  /**
   * @param revup the revup recovery to process
   * @param retry to be notified of collaboration completion
   * @see decideLive return value
   */
  @DatasetEngine.checkNotClosed.async
  private async processRevup(revup: Revup, retry: Subscriber<ConnectStyle>) {
    this.log.info('revving-up from collaborator');
    // We don't wait until rev-ups have been completely delivered
    this.acceptRecoveryUpdates(revup.updates, retry);
    // Is there anything in our journal that post-dates the last revup?
    // Wait until those have been delivered, to preserve fifo.
    await this.emitOpsSince(revup);
  }

  /**
   * This method returns async when the snapshot is delivered. There may still
   * be operations incoming from the collaborator.
   *
   * @param snapshot the snapshot to process
   * @param retry to be notified of collaboration completion
   * @see decideLive return value
   */
  @DatasetEngine.checkNotClosed.async
  private async processSnapshot(snapshot: Snapshot, retry: Subscriber<ConnectStyle>) {
    this.messageService.join(snapshot.gwc);
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
      snapshot.updates.pipe(delayUntil(snapshotApplied)),
      inflateFrom(reEmits));
    this.acceptRecoveryUpdates(updates, retry);
    return snapshotApplied; // We can go live as soon as the snapshot is applied
  }

  private async emitOpsSince<T = never>(
    recovery: Recovery,
    ret: OperatorFunction<OperationMessage, T[]> = ignoreElements()
  ): Promise<T[]> {
    const toReturn = (ops: Observable<OperationMessage>) =>
      firstValueFrom(ops.pipe(ret, defaultIfEmpty([])));
    if (this.newClone) {
      return toReturn(EMPTY);
    } else {
      const recent = await this.dataset.operationsSince(recovery.gwc);
      // If we don't have journal from our ticks on the collaborator's clock, this
      // will lose data! – Close and let the app decide what to do.
      if (recent == null)
        throw new MeldError('Clone outdated', `Missing local ticks since ${recovery.gwc}`);
      else
        return toReturn(recent.pipe(tap(this.nextOperation)));
    }
  }

  private acceptRecoveryUpdates(
    updates: Observable<OperationMessage>, retry: Subscriber<ConnectStyle>) {
    this.remoteOps.attach(updates).then(() => {
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

  @DatasetEngine.checkNotClosed.async
  async newClock(): Promise<TreeClock> {
    const newClock = new Future<TreeClock>();
    await this.dataset.saveClock(gwc => {
      // TODO: This should really be encapsulated in the causal clock
      const lastPublicTick = gwc.getTicks(this.localTime);
      // Back-date the clock to the last public tick before forking
      const fork = this.localTime.ticked(lastPublicTick).forked();
      newClock.resolve(fork.right);
      // And re-apply the ticks to our local clock
      const localClock = fork.left.ticked(this.localTime.ticks);
      this.messageService.push(localClock);
      return localClock;
    });
    return newClock;
  }

  @DatasetEngine.checkLive.async
  @DatasetEngine.checkStateLocked.async
  snapshot(): Promise<Snapshot> {
    return this.lock.exclusive('live', 'snapshot', async () => {
      this.log.info('Compiling snapshot');
      const sentSnapshot = new Future;
      const updates = this.remoteUpdatesBeforeNow(sentSnapshot);
      const snapshot = await this.dataset.takeSnapshot();
      return {
        ...snapshot, updates,
        // Snapshot data is a flowable, so no need to buffer it
        data: snapshot.data.pipe(tapComplete(sentSnapshot))
      };
    });
  }

  @DatasetEngine.checkLive.async
  @DatasetEngine.checkStateLocked.async
  revupFrom(time: TreeClock): Promise<Revup | undefined> {
    return this.lock.exclusive('live', 'revup', async () => {
      const operationsSent = new Future;
      const maybeMissed = this.remoteUpdatesBeforeNow(operationsSent);
      const gwc = new Future<GlobalClock>();
      const operations = await this.dataset.operationsSince(time, gwc);
      if (operations)
        return {
          gwc: await gwc,
          updates: merge(
            operations.pipe(tapComplete(operationsSent), tap(msg =>
              this.log.debug('Sending rev-up', msg.toString(this.log.getLevel())))),
            maybeMissed.pipe(delayUntil(operationsSent)))
        };
    });
  }

  private remoteUpdatesBeforeNow(until: PromiseLike<void>): Observable<OperationMessage> {
    if (this.orderingBuffer.length)
      this.log.info(`Emitting ${this.orderingBuffer.length} from ordering buffer`);
    const now = this.localTime;
    return merge(
      // #1 Anything currently in our ordering buffer
      from(this.orderingBuffer),
      // #2 Anything that arrives stamped prior to now
      this.remoteOps.receiving.pipe(
        filter(message => message.time.anyLt(now)),
        takeUntil(from(until)))
    ).pipe(tap((msg: OperationMessage) => {
      this.log.debug('Forwarding update', msg.toString(this.log.getLevel()));
    }));
  }

  @DatasetEngine.checkNotClosed.async
  countQuads(...args: Parameters<CloneEngine['match']>): Promise<number> {
    return this.dataset.readState.countQuads(...args);
  }

  private wrapStreamFn<P extends any[], T>(
    fn: (...args: P) => BaseStream<T>): ((...args: P) => AsyncIterator<T>) {
    return (...args) => {
      return new TransformIterator<T>(this.closed ?
        Promise.reject(new MeldError('Clone has closed')) :
        this.lock.share('live', 'sparql', async () => wrap(fn(...args))));
    };
  }

  @DatasetEngine.checkNotClosed.rx
  @DatasetEngine.checkStateLocked.rx
  read(request: Read): Consumable<GraphSubject> {
    this.logRequest('read', request);
    // Extend the 'state' lock until the read actually happens, which is when
    // the 'live' lock is acquired. The read may also choose to extend the
    // 'state' lock while results are streaming.
    const results = new Future<Consumable<GraphSubject>>();
    this.lock.share('live', 'read', () => new Promise<void>(exitLiveLock => {
      // Only exit the live-lock when the results have been fully streamed
      results.resolve(this.dataset.read(request).pipe(finalize(exitLiveLock)));
    })).then(
      () => this.log.debug('read complete'),
      err => results.reject(err)); // Only if lock fails
    return inflateFrom(this.lock.extend('state', 'read', results));
  }

  @DatasetEngine.checkNotClosed.async
  @DatasetEngine.checkStateLocked.async
  async write(request: Write): Promise<this> {
    await this.lock.share('live', 'write', async () => {
      this.logRequest('write', request);
      // Take the send timestamp just before enqueuing the transaction. This
      // ensures that transaction stamps increase monotonically.
      const sendTime = this.messageService.event();
      const update = await this.dataset.transact(async () =>
        [sendTime, await this.dataset.write(request)]);
      // Publish the operation
      if (update != null)
        this.nextOperation(update);
    });
    return this;
  }

  @DatasetEngine.checkNotClosed.async
  @DatasetEngine.checkStateLocked.async
  ask(pattern: Query): Promise<boolean> {
    return this.lock.extend('state', 'ask',
      this.lock.share('live', 'ask',
        () => this.dataset.ask(pattern)));
  }

  @DatasetEngine.checkNotClosed.async
  withLocalState<T>(procedure: StateProc<MeldReadState, T>): Promise<T> {
    return this.lock.share('state', 'protocol',
      () => procedure(this.dataset.readState));
  }

  private logRequest(type: 'read' | 'write', request: Pattern) {
    if (this.log.getLevel() <= levels.DEBUG)
      this.log.debug(type, 'request', JSON.stringify(request));
  }

  private createStatus(): LiveStatus {
    let remotesEverLive = false;
    const stateRollup = liveRollup({
      live: this.live,
      remotesLive: this.remotes.live,
      outdated: this.remoteOps.outdated,
      ticks: this.latestTicks
    });
    const toStatus = (state: typeof stateRollup['value']): MeldStatus => {
      if (state.remotesLive === true)
        remotesEverLive = true;
      const silo = state.live === true && state.remotesLive === false;
      return ({
        online: state.remotesLive != null,
        // If genesis, never outdated.
        // If we have never had live remotes and siloed, not outdated
        outdated: !this.isGenesis && state.outdated && (remotesEverLive || !silo),
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
    const becomes = async (match?: Partial<MeldStatus>) => firstValueFrom(
      values.pipe(filter(status => matchStatus(status, match)),
        defaultIfEmpty(undefined)));
    return Object.defineProperties(values, {
      becomes: { value: becomes },
      value: { get: () => toStatus(stateRollup.value) }
    }) as unknown as LiveStatus;
  }

  @DatasetEngine.checkNotClosed.async
  async close(err?: any) {
    if (err)
      this.log.warn('Shutting down due to', err);
    else
      this.log.info('Shutting down normally');

    // Make sure we never receive another remote update
    this.subs.unsubscribe();
    this.remoteOps.close(err);
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
