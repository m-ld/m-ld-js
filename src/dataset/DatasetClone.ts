import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldUpdate, MeldStatus, LiveStatus } from '../m-ld';
import { Pattern, Subject, isRead, isSubject, isGroup, isUpdate } from './jrql-support';
import {
  Observable, merge, from, defer, EMPTY,
  concat, BehaviorSubject, Subscription, throwError, identity
} from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import {
  publishReplay, refCount, filter, ignoreElements, takeUntil, tap,
  isEmpty, finalize, flatMap, toArray, first, map, debounceTime, distinctUntilChanged, scan, takeWhile
} from 'rxjs/operators';
import { delayUntil, Future, tapComplete, tapCount, SharableLock } from '../util';
import { levels } from 'loglevel';
import { MeldError } from '../m-ld/MeldError';
import { AbstractMeld, comesAlive } from '../AbstractMeld';
import { MeldConfig } from '..';
import { RemoteUpdates } from './RemoteUpdates';

export class DatasetClone extends AbstractMeld implements MeldClone {
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: DeltaMessage[] = [];
  private readonly remotes: Omit<MeldRemotes, 'updates'>;
  private readonly remoteUpdates: RemoteUpdates;
  private remoteSub = new Subscription;
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
    this.dataset = new SuSetDataset(config['@id'], dataset, config.logLevel);
    this.dataset.updates.subscribe(next => this.latestTicks.next(next['@ticks']));
    this.remotes = remotes;
    this.remotes.setLocal(this);
    this.remoteUpdates = new RemoteUpdates(remotes);
    this.networkTimeout = config.networkTimeout ?? 5000;
    this.genesisClaim = config.genesis;
    this.status.subscribe(status => this.log.debug(JSON.stringify(status)));
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

    this.remoteSub.add(this.remoteUpdates.receiving.pipe(
      map(delta => {
        const logBody = this.log.getLevel() < levels.DEBUG ? delta : `tid: ${delta.data.tid}`;
        this.log.debug('Receiving', logBody, '@', this.localTime);
        // If we buffer a message, return true to signal we might need a re-connect
        return !this.messageService.receive(delta, this.orderingBuffer, msg => {
          this.log.debug('Accepting', logBody);
          this.dataset.apply(msg.data, msg.time, this.localTime)
            .then(msg.delivered.resolve)
            .catch(err => this.close(err)); // Catastrophe
        });
      }),
      filter(identity), // From this point we are only interested in buffered messages
      debounceTime(this.networkTimeout)).subscribe({
        next: () => {
          if (this.orderingBuffer.length) {
            // We're missing messages that have been receieved by others.
            // Let's re-connect to see if we can get back on track.
            this.log.warn('Messages are out of order and backing up. Re-connecting.');
            // At this point we will be live if the connect was OK
            this.forceReconnect().catch(this.warnError);
          } else {
            this.log.debug('Messages were out of order, but now cleared.');
          }
        },
        error: err => this.close(err),
        complete: () => this.close()
      }))

    await new Promise((resolve, reject) => {
      // Subscribe will synchronously receive the current value, but we don't
      // use it because the value might have changed when we get the lock.
      this.remoteSub.add(this.remotes.live.subscribe(() =>
        this.decideLive().then(resolve, reject)));
    });
    // For a new non-genesis clone, the first connect is essential.
    if (this.newClone)
      await comesAlive(this);
  }

  get isGenesis(): boolean {
    return this.localTime.isId;
  }

  setLive(live: boolean) {
    // We may already be revving-up
    if (live && !this.remoteUpdates.state.value.attached)
      this.remoteUpdates.attach();
    else if (!live)
      this.remoteUpdates.detach(this.isGenesis);
    super.setLive(live);
  }

  private decideLive(): Promise<void> {
    // Block transactions, revups and other connect attempts while handling
    // liveness change.
    return this.liveLock.acquire(this.id, async () => {
      const remotesLive = this.remotes.live.value;
      if (remotesLive === false && this.newClone) {
        throw new Error('New clone is siloed.');
      } else if (remotesLive === true) {
        // Connect in the live lock
        return this.connect();
      } else if (remotesLive === null) {
        // We are partitioned from the domain.
        this.setLive(false);
      } else if (remotesLive === false) {
        // We are a silo, the last survivor. Stay live for any newcomers.
        this.setLive(true);
      }
    });
  }

  private async connect(): Promise<void> {
    this.log.info('Connecting to remotes');
    try {
      // At this point we are uncertain whether we will fully attach
      this.remoteUpdates.setOutdated();
      await this.flushUndeliveredOperations();
      // If silo (already live), or top-level is Id (never been forked), no rev-up to do
      if (this.live.value || this.isGenesis) {
        this.remoteUpdates.attach();
      } else if (this.newClone) {
        this.log.info('New clone requesting snapshot');
        await this.requestSnapshot();
      } else {
        await this.tryRevup();
      }
      this.log.info('connected.');
      this.setLive(true);
    } catch (err) {
      // This usually indicates that the remotes have gone offline during
      // our connection attempt. If they have reconnected, another attempt
      // will have already been queued on the connect lock.
      this.log.info('Cannot connect to remotes due to', err);
    }
  }

  private onRevupFailed = (err: any) => {
    // If rev-ups fail (for example, if the collaborator goes offline)
    // it's not a catastrophe but we do need to enqueue a retry
    this.log.warn('Rev-up did not complete due to', err);
    return this.forceReconnect();
  };

  private forceReconnect(): Promise<void> {
    // At this point we may already be live
    this.setLive(false);
    return this.decideLive();
  }

  private async tryRevup() {
    // For a new clone, last hash is random so this will return undefined
    const revup = await this.remotes.revupFrom(this.localTime);
    if (revup) {
      this.log.info('revving-up from collaborator');
      this.remoteUpdates.injectRevups(revup).then(async lastRevup => {
        // Emit anything in our journal that post-dates the last revup
        const recent = lastRevup && await this.dataset.operationsSince(lastRevup.time);
        recent && recent.subscribe(this.nextUpdate);
        return lastRevup;
      }).catch(this.onRevupFailed);
    } else {
      this.log.info('cannot rev-up, requesting snapshot');
      await this.requestSnapshot();
    }
  }

  private async requestSnapshot() {
    // If there are unsent operations, we would lose data
    if (!(await this.dataset.undeliveredLocalOperations().pipe(isEmpty()).toPromise()))
      throw new MeldError('Unsent updates');

    const snapshot = await this.remotes.snapshot();
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
    this.remoteUpdates.injectRevups(concat(updates, from(reEmits).pipe(flatMap(from))))
      .catch(this.onRevupFailed);
    return delivered; // We can go live as soon as the snapshot is delivered
  }

  private flushUndeliveredOperations() {
    return new Promise<void>((resolve, reject) => {
      const counted = new Future<number>();
      counted.then(n => n && this.log.info(`Emitted ${n} undelivered operations`));
      this.dataset.undeliveredLocalOperations().pipe(tapCount(counted))
        .subscribe(this.nextUpdate, reject, resolve);
    });
  }

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

  private get localTime() {
    return this.messageService.peek();
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

  get status(): LiveStatus {
    const getValue: () => MeldStatus = () => ({
      online: this.remotes.live.value != null,
      outdated: this.remoteUpdates.state.value.outdated,
      ticks: this.latestTicks.value
    });
    const matchStatus = (status: MeldStatus, match?: Partial<MeldStatus>) =>
      (match?.online == null || match.online === status.online) &&
      (match?.outdated == null || match.outdated === status.outdated);
    // First value must be deferred until the observable is subscribed
    const values = defer(() => merge(
      this.remotes.live.pipe(map(live => ({ online: live != null }))),
      this.remoteUpdates.state.pipe(map(state => ({ outdated: state.outdated })))
    ).pipe(
      scan((prev, next) => ({ ...prev, ...next, ticks: this.latestTicks.value }), getValue()),
      distinctUntilChanged<MeldStatus>(matchStatus)));
    const becomes = async (match?: Partial<MeldStatus>) =>
      values.pipe(first(status => matchStatus(status, match))).toPromise();
    return Object.defineProperties(values, {
      becomes: { value: becomes },
      value: { get: getValue }
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
    this.remoteSub.unsubscribe();
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