import {
  GraphSubject, LiveStatus, MeldContext, MeldError, MeldErrorStatus, MeldReadState, MeldStatus,
  StateProc
} from '../../api';
import { MeldLocal, MeldRemotes, OperationMessage, Recovery, Revup, Snapshot } from '..';
import { liveRollup } from '../api-support';
import { Query, Read } from '../../jrql-support';
import {
  BehaviorSubject, concat, concatMap, debounce, defaultIfEmpty, EMPTY, firstValueFrom, from,
  interval, merge, Observable, of, OperatorFunction, partition, race, Subscriber, Subscription
} from 'rxjs';
import { GlobalClock, TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import {
  delayWhen, distinctUntilChanged, expand, filter, finalize, ignoreElements, map, share, skipWhile,
  takeUntil, tap, toArray
} from 'rxjs/operators';
import { delayUntil, inflateFrom, poisson, settled, takeUntilComplete } from '../util';
import { checkLocked, LockManager, SHARED, SHARED_REENTRANT } from '../locks';
import { levels } from 'loglevel';
import { AbstractMeld, checkLive } from '../AbstractMeld';
import { RemoteOperations } from './RemoteOperations';
import { CloneEngine, EngineWrite } from '../StateEngine';
import async from '../async';
import { BaseStream } from '../../rdfjs-support';
import { Consumable } from 'rx-flowable';
import { MeldConfig } from '../../config';
import { MeldOperationMessage } from '../MeldOperationMessage';
import { Stopwatch } from '../Stopwatch';
import { checkNotClosed } from '../check';
import { Future, tapComplete } from '../Future';

enum Reconnect {
  SOFT = 'RECONNECT_SOFT',
  HARD = 'RECONNECT_HARD'
}

enum OperationOutcome {
  /** Operation was accepted (and may have precipitated un-buffering) */
  ACCEPTED = 'OPERATION_ACCEPTED',
  /** Operation was buffered in expectation of causal operations */
  BUFFERED = 'OPERATION_BUFFERED',
  /** Operation was unacceptable due to missing prior operations */
  DISORDERED = 'OPERATION_DISORDERED',
  /** Operation could not be processed due to missing history */
  UNBASED = 'OPERATION_UNBASED'
}

function isAcuteOutcome(outcome: OperationOutcome) {
  return outcome === OperationOutcome.DISORDERED ||
    outcome === OperationOutcome.UNBASED;
}

enum RemotesLive {
  LIVE = 'REMOTES_LIVE',
  DEAD = 'REMOTES_DEAD',
  GONE = 'REMOTES_GONE'
}

/** @see Meld.live */
function remotesLive(live: null | boolean) {
  return live == null ? RemotesLive.GONE :
    live ? RemotesLive.LIVE : RemotesLive.DEAD;
}

type ConnectReason = RemotesLive | OperationOutcome;

type Operations = Observable<OperationMessage>;

export class DatasetEngine extends AbstractMeld implements CloneEngine, MeldLocal {
  private readonly suset: SuSetDataset;
  private messageService: undefined | TreeClockMessageService;
  private readonly processingBuffer = new Set<MeldOperationMessage>();
  private readonly orderingBuffer: MeldOperationMessage[] = [];
  private readonly remotes: Omit<MeldRemotes, 'operations'>;
  private readonly remoteOps: RemoteOperations;
  private subs = new Subscription;
  private readonly latestTicks = new BehaviorSubject<number>(NaN);
  private readonly networkTimeout: number;
  private readonly genesisClaim: boolean;
  readonly status: LiveStatus;
  /*readonly*/
  context: MeldContext;
  /*readonly*/
  match: CloneEngine['match'];
  /*readonly*/
  query: CloneEngine['query'];

  /**
   * @param suset injected SU-Set (should NOT be initialised)
   * @param remotes injected remotes
   * @param config m-ld configuration
   */
  constructor(
    suset: SuSetDataset,
    remotes: MeldRemotes,
    config: MeldConfig
  ) {
    super(config);
    this.suset = suset;
    this.subs.add(this.dataUpdates
      .pipe(map(update => update['@ticks']))
      .subscribe(this.latestTicks));
    this.remotes = remotes;
    this.remoteOps = new RemoteOperations(remotes);
    this.networkTimeout = config.networkTimeout ?? 5000;
    this.genesisClaim = config.genesis;
    this.status = this.createStatus();
    this.subs.add(this.status.subscribe({
      next: status => this.log.debug(status),
      error: () => {} // Else unhandled
    }));
  }

  /**
   * Lock ordering matters to prevent deadlock. If both keys are required,
   * 'state' must be acquired first.
   */
  get lock(): LockManager<'state' | 'live'> {
    return this.suset.lock;
  }

  set clock(time: TreeClock | undefined) {
    if (time != null) {
      this.log.info('has time', time);
      this.messageService = new TreeClockMessageService(time);
      this.latestTicks.next(time.ticks);
    } else {
      throw new RangeError('Clock cannot be unset');
    }
  }

  get clock() {
    return this.messageService?.peek();
  }

  get isGenesis() {
    // Note this is only valid after initialisation
    return this.clock?.isId;
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
  @checkNotClosed.async
  async initialise(sw?: Stopwatch): Promise<void> {
    try {
      sw?.next('init');
      await this.initDataset();

      this.remotes.setLocal(this);
      // Load the clock for this clone; if a new clone, it will be requested later
      sw?.next('load-clock');
      const clock = await this.suset.loadClock();
      if (clock != null || this.genesisClaim)
        this.clock = clock ?? await this.suset.resetClock();

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
        this.remotes.live.pipe(map(remotesLive)),
        // 2. Chronic buffering of operations
        // 3. Disordered operations
        this.operationProblems
      ).pipe(
        takeUntilComplete(this.remotes.live),
        // 4. Last attempt to connect can generate more attempts
        expand(reason => this.decideLive(reason).pipe(
          delayWhen(this.reconnectDelayer), // delay if soft reconnect
          map(() => reason) // recurse with original reason
        ))
      ).subscribe({
        error: err => this.close(err),
        complete: () => this.close()
      }));

      if (clock == null && !this.genesisClaim) {
        sw?.next('comes-alive');
        // For a new non-genesis clone, the first connect is essential.
        await this.comesAlive();
      }
      // Inform the dataset that we're open for business
      await this.lock.share('state', 'initialised',
        () => this.suset.allowTransact());
    } catch (e) {
      // Failed to initialise somehow – this is fatal
      if (!this.closed) // That might be the problem
        await this.close(e);
      throw e;
    }
  }

  private async initDataset() {
    await this.suset.initialise();
    this.context = this.suset.userCtx;
    const rdfSrc = this.suset.readState;
    // Raw RDF methods just pass through to the dataset when its initialised
    this.match = this.wrapReadStreamFn(rdfSrc.match.bind(rdfSrc));
    // @ts-ignore - TS can't cope with overloaded query method
    this.query = this.wrapReadStreamFn(rdfSrc.query.bind(rdfSrc));
  }

  private reconnectDelayer = (style: Reconnect) =>
    style === Reconnect.HARD ?
      // Hard retry is immediate
      of(0) :
      // Soft retry is delayed
      interval(this.getRetryDelay());

  /** @returns millis from distribution ~(>=0 mean 2) * network timeout */
  private getRetryDelay(): number {
    return (poisson(2) + 1) * Math.random() * this.networkTimeout;
  }

  get dataUpdates() {
    return this.suset.updates;
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
      // Only try and accept one operation at a time. If the operation no longer
      // belongs to the remotes' active 'period', discard it.
      concatMap(([op, period]) =>
        period === this.remoteOps.period && this.messageService != null ?
          this.receiveRemoteOperation(op, this.messageService) : EMPTY),
      // Ensure that the merge below does not incur two subscribes
      share()
    );
    // Disordered or unbased messages are an acute problem, buffering only if chronic
    const [acute, maybeBuffering] = partition(acceptOutcomes, outcome =>
      isAcuteOutcome(outcome));
    const isBuffering = maybeBuffering.pipe(
      // Accepted messages need no action, we are only interested in buffered
      filter(outcome => outcome === OperationOutcome.BUFFERED),
      // Wait for the network timeout in case the buffer clears.
      // If an acute problem occurs in the meantime, that takes precedence.
      debounce(() => race(interval(this.networkTimeout), acute)),
      // After the debounce, check if still buffering
      filter(outcome => {
        if (isAcuteOutcome(outcome)) {
          return false; // Will be handled by merge below
        } else if (this.orderingBuffer.length > 0) {
          // We're missing messages that have been received by others.
          // Let's re-connect to see if we can get back on track.
          this.log.warn('Messages are out of order and backing up. Re-connecting.');
          return true;
        } else {
          this.log.debug('Messages were out of order, but now cleared.');
          return false;
        }
      }));
    return merge(acute, isBuffering).pipe(
      tap(() => this.remoteOps.detach('outdated')));
  }

  private async receiveRemoteOperation(
    op: MeldOperationMessage,
    messageService: TreeClockMessageService
  ): Promise<OperationOutcome> {
    const logBody = this.log.getLevel() < levels.DEBUG ? op : `${op.time}`;
    this.log.debug('Receiving', logBody);
    this.processingBuffer.add(op);
    settled(op.delivered).then(() => this.processingBuffer.delete(op));
    // Grab the state lock, per CloneEngine contract and to ensure that all
    // clock ticks are immediately followed by their respective transactions.
    return this.lock.exclusive('state', 'remote operation', async () => {
      try {
        const startTime = messageService.peek();
        return await messageService.receive(op, this.orderingBuffer,
          async (msg, prevTime) => {
            // Check that we have the previous message from this clock ID
            const ticksSeen = prevTime.getTicks(msg.time);
            if (msg.time.ticks <= ticksSeen) {
              // Already had this message.
              this.log.debug('Ignoring outdated', logBody);
              msg.delivered.resolve();
            } else if (msg.prev > ticksSeen) {
              // We're missing a message. Reset the clock and trigger a re-connect.
              messageService.push(startTime);
              const outOfOrder = new MeldError('Update out of order', `
              Update claims prev is ${msg.prev} (${msg.time}),
              but local clock was ${ticksSeen} (${prevTime})`);
              msg.delivered.reject(outOfOrder);
              throw outOfOrder;
            } else {
              this.log.debug('Accepting', logBody);
              await this.suset.apply(msg, messageService)
                .then(cxOp => cxOp && this.nextOperation(cxOp, 'constraint'))
                .then(msg.delivered.resolve)
                .catch(err => {
                  msg.delivered.reject(err);
                  throw err;
                });
            }
          }) ? OperationOutcome.ACCEPTED : OperationOutcome.BUFFERED;
      } catch (err) {
        if (err instanceof MeldError) {
          if (err.status === MeldErrorStatus['Update out of order']) {
            this.log.info(err.message);
            return OperationOutcome.DISORDERED;
          } else if (err.status === MeldErrorStatus['Updates unavailable']) {
            // The clone doesn't have enough history to process the message.
            // The only choice is to rebase to a snapshot.
            this.log.info(err.message);
            return OperationOutcome.UNBASED;
          }
        }
        throw err;
      }
    });
  }

  /**
   * @returns Zero or one retry indication. If zero, success. If a value is
   * emitted, it indicates failure, and the value is whether the re-connect
   * should be hard. Emission of an error is catastrophic.
   */
  private decideLive(reason: ConnectReason): Observable<Reconnect> {
    return new Observable(retry => {
      // As soon as a decision on liveness needs to be made, pause output
      // operations to mitigate against breaking fifo with emitOpsSince().
      this.pauseOperations(
        // Also block transactions, recovery requests and other connect attempts.
        this.lock.acquire('state', 'decide live', SHARED).then(release =>
          this.lock.exclusive('live', 'decide live', async () => {
            // Not using LiveValue.value because it doesn't detect closed remotes
            const remotesLive = await firstValueFrom(this.remotes.live);
            if (remotesLive === true) {
              if (this.isGenesis)
                throw new Error('Genesis clone trying to join a live domain.');
              // Connect in the live lock
              const connected = await this.connect(reason, retry, release);
              // New clones cannot provide recovery (are 'live') until first connected
              if (connected || this.clock != null)
                this.setLive(true);
            } else {
              // Stop receiving operations until re-connect, do not change outdated
              this.remoteOps.detach();
              if (remotesLive === false) {
                // We are the silo, the last survivor.
                if (this.clock == null)
                  throw new Error('New clone is siloed.'); // TODO make a MeldError
                // Stay live for any newcomers to rev-up from us.
                this.setLive(true);
                retry.complete();
              } else if (remotesLive === null) {
                // We are partitioned from the domain.
                this.setLive(false);
                retry.complete();
              }
            }
          }).finally(release)
        ).catch(err => retry.error(err))
      );
    });
  }

  /**
   * @param reason the reason for the connection attempt
   * @param retry to be notified of collaboration completion
   * @param releaseState to be called when the locked state is no longer needed
   * @returns `true` if connected
   * @see {@link decideLive} return value
   */
  @checkLocked('state').async
  private async connect(
    reason: ConnectReason,
    retry: Subscriber<Reconnect>,
    releaseState: () => void
  ): Promise<boolean> {
    this.log.info(
      this.clock == null ? 'new clone' :
        this.live.value === true && this.remotes.live.value === false ? 'silo' : 'clone',
      'connecting to remotes, because',
      reason
    );
    let recovery: Recovery | undefined;
    const updates = new Future<Operations>();
    // Start listening for remote updates as early as possible
    this.acceptRecoveryUpdates(inflateFrom(updates), retry);
    const processRecovery = async (process: Function) => {
      if (recovery != null) {
        releaseState();
        await process.call(this, recovery, updates);
      }
    };
    try {
      const clock = this.clock;
      if (clock != null && reason !== OperationOutcome.UNBASED) {
        recovery = await this.remotes.revupFrom(clock, this.suset.readState);
        await processRecovery(this.processRevup);
      }
      if (recovery == null) {
        recovery = await this.remotes.snapshot(
          this.clock == null, this.suset.readState);
        await processRecovery(this.processSnapshot);
      }
      return true;
    } catch (err) {
      this.log.info('Cannot connect to remotes due to', err);
      updates.reject(err); // Ensure we release the remoteOps
      recovery?.cancel(err); // Tell the collaborator we're giving up
      // noinspection JSUnusedAssignment – recovery may well be undefined
      if (recovery == null && this.clock == null) {
        // For a new clone, a failure to obtain a recovery is terminal
        this.log.warn('New clone aborting initialisation');
        retry.error(err);
      } else {
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
        retry.next(Reconnect.SOFT);
        retry.complete();
      }
      return false;
    }
  }

  /**
   * @param revup the revup recovery to process
   * @param updates to fill in with updates from the collaborator
   * @see decideLive return value
   */
  @checkNotClosed.async
  private async processRevup(revup: Revup, updates: Future<Operations>) {
    this.log.info('revving-up from collaborator');
    // We don't wait until rev-ups have been completely delivered
    updates.resolve(revup.updates);
    // Is there anything in our journal that post-dates the last revup?
    // Wait until those have been delivered, to preserve fifo.
    await this.emitOpsSince(revup);
  }

  /**
   * This method returns async when the snapshot is delivered. There may still
   * be operations incoming from the collaborator.
   *
   * @param snapshot the snapshot to process
   * @param updates to fill in with updates from the collaborator
   * @see decideLive return value
   */
  @checkNotClosed.async
  private processSnapshot(snapshot: Snapshot, updates: Future<Operations>) {
    this.log.info('processing snapshot from collaborator');
    const time = this.clock ?? snapshot.clock;
    if (time == null)
      throw new MeldError('Bad response', 'No clock in snapshot');
    // If we have any operations since the snapshot: re-emit them now and
    // re-apply them to our own dataset when the snapshot is applied.
    /*
    FIXME: Holding this stuff in memory during a potentially long snapshot
    application is not scalable or safe.
    */
    const reEmits = !this.clock ? Promise.resolve([]) :
      this.emitOpsSince(snapshot, toArray());
    // Start applying the snapshot when we have done re-emitting
    const snapshotApplied = reEmits
      .then(() => this.suset.applySnapshot(snapshot, time))
      .then(() => {
        this.clock ??= time;
        this.messageService?.join(snapshot.gwc);
      });
    // Delay all updates until the snapshot has been fully applied
    // This is because a snapshot is applied in multiple transactions
    updates.resolve(concat(
      snapshot.updates.pipe(delayUntil(snapshotApplied)),
      inflateFrom(reEmits)
    ));
    return snapshotApplied; // We can go live as soon as the snapshot is applied
  }

  private async emitOpsSince<T = never>(
    recovery: Recovery,
    ret: OperatorFunction<OperationMessage, T[]> = ignoreElements()
  ): Promise<T[]> {
    const toReturn = (ops: Operations) =>
      firstValueFrom(ops.pipe(ret, defaultIfEmpty([])));
    if (this.clock == null) {
      return toReturn(EMPTY);
    } else {
      const recent = await this.suset.operationsSince(recovery.gwc);
      // If we don't have journal from our ticks on the collaborator's clock, this
      // will lose data! – Close and let the app decide what to do.
      if (recent == null)
        throw new MeldError('Clone outdated', `Missing local ticks since ${recovery.gwc}`);
      else
        return toReturn(recent.pipe(tap(op => this.nextOperation(op, 'post-recovery'))));
    }
  }

  private acceptRecoveryUpdates(
    updates: Operations,
    retry: Subscriber<Reconnect>
  ) {
    this.remoteOps.attach(updates).then(() => {
      // If we were a new clone, we're up-to-date now
      this.log.info('connected');
      retry.complete();
    }, err => {
      // If rev-ups fail (for example, if the collaborator goes offline)
      // it's not a catastrophe but we do need to enqueue a retry
      this.log.warn('Rev-up did not complete due to', err);
      retry.next(Reconnect.HARD); // Force re-connect
      retry.complete();
    });
  }

  @checkLive.async
  @checkLocked('state').async
  snapshot(newClock: boolean): Promise<Snapshot> {
    return this.lock.exclusive('live', 'snapshot', async () => {
      this.log.info('Compiling snapshot');
      const sentSnapshot = new Future;
      const updates = this.remoteUpdatesToForward(sentSnapshot);
      const snapshot = await this.suset.takeSnapshot(newClock, this.messageService!);
      return {
        ...snapshot, updates,
        // Snapshot data is a flowable, so no need to buffer it
        data: snapshot.data.pipe(tapComplete(sentSnapshot))
      };
    });
  }

  @checkLive.async
  @checkLocked('state').async
  revupFrom(time: TreeClock): Promise<Revup | undefined> {
    return this.lock.exclusive('live', 'revup', async () => {
      const operationsSent = new Future;
      const maybeMissed = this.remoteUpdatesToForward(operationsSent);
      const gwc = new Future<GlobalClock>();
      const operations = await this.suset.operationsSince(time, gwc);
      if (operations)
        return {
          gwc: await gwc,
          updates: merge(
            operations.pipe(tapComplete(operationsSent), tap(msg =>
              this.log.debug('Sending rev-up', this.msgString(msg)))),
            maybeMissed.pipe(delayUntil(operationsSent))),
          cancel: (cause?: Error) => this.log.debug('Recovery cancelled', cause)
        };
    });
  }

  private remoteUpdatesToForward(until: PromiseLike<void>): Operations {
    if (this.processingBuffer.size)
      this.log.info(`Emitting ${this.processingBuffer.size} from processing buffer`);
    return concat(
      // #1 Anything we have yet to process at the moment we are subscribed
      from(this.processingBuffer),
      // #2 Anything that arrives while we are forwarding
      this.remoteOps.receiving.pipe(map(([op]) => op), takeUntil(until))
    ).pipe(
      tap((msg: OperationMessage) => {
        this.log.debug('Forwarding update', this.msgString(msg));
      }),
      takeUntil(this.errorIfClosed)
    );
  }

  @checkNotClosed.async
  countQuads(...args: Parameters<CloneEngine['match']>): Promise<number> {
    return this.suset.readState.countQuads(...args);
  }

  private wrapReadStreamFn<F extends ((...args: unknown[]) => BaseStream<T>), T>(
    fn: F
  ): ((...args: Parameters<F>) => async.AsyncIterator<T>) {
    return (...args) =>
      new async.TransformIterator<T>(this.closed ?
        Promise.reject(new MeldError('Clone has closed')) :
        // Note reads can be re-entrant during streaming.
        this.lock.acquire('live', 'sparql', SHARED_REENTRANT)
          .then(releaseLive => async.wrap(fn(...args)).on('end', releaseLive))
      );
  }

  @checkNotClosed.rx
  @checkLocked('state').rx
  read(request: Read): Consumable<GraphSubject> {
    this.logRequest('read', request);
    // Extend the 'state' lock until the read actually happens, which is when
    // the 'live' lock is acquired. The read may also choose to extend the
    // 'state' lock while results are streaming.
    // Note reads can be reentrant during streaming. The flag prevents a
    // deadlock if the live lock is concurrently acquired exclusively, as by a
    // connect.
    return inflateFrom(this.lock.extend('state', 'read',
      this.lock.acquire('live', 'read', SHARED_REENTRANT)
        .then(releaseLive => this.suset.read(request).pipe(finalize(releaseLive)))));
  }

  @checkNotClosed.async
  @checkLocked('state').async
  async write(request: EngineWrite): Promise<this> {
    await this.lock.share('live', 'write', async () => {
      this.logRequest('write', request);
      // Take the send timestamp just before enqueuing the transaction. This
      // ensures that transaction stamps increase monotonically.
      const update = await this.suset.transact(this.messageService!.event(), request);
      // Publish the operation
      if (update != null)
        this.nextOperation(update, 'write');
    });
    return this;
  }

  @checkNotClosed.async
  @checkLocked('state').async
  ask(pattern: Query): Promise<boolean> {
    return this.lock.extend('state', 'ask',
      this.lock.share('live', 'ask',
        () => this.suset.ask(pattern)));
  }

  @checkNotClosed.async
  latch<T>(procedure: StateProc<MeldReadState, T>): Promise<T> {
    return this.lock.share('state', 'protocol',
      () => procedure(this.suset.readState));
  }

  private logRequest(type: 'read' | 'write', request: {}) {
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

  @checkNotClosed.async
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
      time: ${this.clock}`);
    }
    super.close(err);
    await this.suset.close(err);
  }
}
