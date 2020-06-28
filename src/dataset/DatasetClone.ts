import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldUpdate } from '../m-ld';
import { Pattern, Subject, isRead, isSubject, isGroup, isUpdate } from './jrql-support';
import {
  Observable, Subject as Source, merge, from, defer, NEVER, EMPTY,
  concat, BehaviorSubject, Subscription, throwError, identity
} from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import {
  publishReplay, refCount, filter, ignoreElements, takeUntil, tap,
  isEmpty, finalize, flatMap, switchAll, toArray, first, map, debounceTime
} from 'rxjs/operators';
import { delayUntil, Future, tapComplete, tapCount, SharableLock, tapLast, onErrorNever } from '../util';
import { levels } from 'loglevel';
import { MeldError } from '../m-ld/MeldError';
import { AbstractMeld, isOnline, comesOnline } from '../AbstractMeld';
import { MeldConfig } from '..';

class RemoteUpdates {
  readonly received: Observable<DeltaMessage>;
  private readonly remoteUpdates: Source<Observable<DeltaMessage>> = new Source;

  constructor(
    private readonly remotes: MeldRemotes) {
    this.received = this.remoteUpdates.pipe(switchAll());
  }

  attach = () => this.remoteUpdates.next(this.remotes.updates);
  detach = () => this.remoteUpdates.next(NEVER);

  inject(revups: Observable<DeltaMessage>): Promise<DeltaMessage | undefined> {
    const lastRevup = new Future<DeltaMessage | undefined>();
    // Updates must be paused during revups because the collaborator might
    // send an update while also sending revups of its own prior updates.
    // That would break the ordering guarantee.
    this.remoteUpdates.next(merge(
      // Errors should be handled in the returned promise
      onErrorNever(revups.pipe(tapLast(lastRevup))),
      this.remotes.updates.pipe(delayUntil(onErrorNever(lastRevup)))));
    return Promise.resolve(lastRevup);
  }
}

export class DatasetClone extends AbstractMeld implements MeldClone {
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: DeltaMessage[] = [];
  private readonly remotes: Omit<MeldRemotes, 'updates'>;
  private readonly remoteUpdates: RemoteUpdates;
  private remoteUpdatesSub: Subscription;
  private readonly onlineLock = new SharableLock;
  private newClone: boolean = false;
  private readonly latestTicks = new BehaviorSubject<number>(NaN);
  private readonly revvingUp = new BehaviorSubject<boolean>(false);
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
  }

  /**
   * Must be called prior to making transactions against this clone. The
   * returned promise does not guarantee that the clone is online or up-to-date,
   * because it may be disconnected or still receiving recent updates from a
   * collaborator.
   * 
   * An application may choose to delay its own initialisation until the latest
   * updates have either been received or the clone is confirmed to be offline,
   * using the {@link #latest} method.
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

    this.remoteUpdatesSub = this.remoteUpdates.received.pipe(
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
            // At this point we will be online if the connect was OK
            this.forceReconnect().catch(this.warnError);
          } else {
            this.log.debug('Messages were out of order, but now cleared.');
          }
        },
        error: err => this.close(err),
        complete: () => this.close()
      })

    await new Promise((resolve, reject) => {
      // Subscribe will synchronously receive the current value, but we don't
      // use it because the value might have changed when we get the lock.
      this.remotes.online.subscribe(() =>
        this.decideOnline().then(resolve, reject));
    });
    // For a new non-genesis clone, the first connect is essential.
    if (this.newClone)
      await comesOnline(this);
  }

  private decideOnline(): Promise<void> {
    // Block transactions, revups and other connect attempts while handling
    // online change.
    return this.onlineLock.acquire(this.id, async () => {
      const remotesOnline = await isOnline(this.remotes);
      if (remotesOnline === false && this.newClone) {
        throw new Error('New clone is siloed.');
      } else if (remotesOnline === true) {
        // Connect in the online lock
        return this.connect();
      } else if (remotesOnline === null) {
        // We are partitioned from the domain.
        this.remoteUpdates.detach();
        this.setOnline(false);
      } else if (remotesOnline === false) {
        // We are a silo, the last survivor. Stay online for any newcomers.
        this.remoteUpdates.attach();
        this.setOnline(true);
      }
    });
  }

  private async connect(): Promise<void> {
    this.log.info('Connecting to remotes');
    try {
      // At this point we are uncertain what the 'latest' delta will be
      this.revvingUp.next(true);
      await this.flushUndeliveredOperations();
      // If silo (already online), or top-level is Id (never been forked), no rev-up to do
      if (this.isOnline() || this.localTime.isId) {
        this.remoteUpdates.attach();
        this.revvingUp.next(false);
      } else if (this.newClone) {
        this.log.info('New clone requesting snapshot');
        await this.requestSnapshot();
      } else {
        await this.tryRevup();
      }
      this.log.info('connected.');
      this.setOnline(true);
    } catch (err) {
      // This usually indicates that the remotes have gone offline during
      // our connection attempt. If they have reconnected, another attempt
      // will have already been queued on the connect lock.
      this.log.info('Cannot connect to remotes due to', err);
    }
  }

  private onRevupComplete = async (lastRevup: DeltaMessage | undefined) => {
    // Here, we are definitely before the first post-revup update, but
    // the actual last revup might not yet have been applied to the dataset.
    if (lastRevup != null)
      await lastRevup.delivered;
    this.revvingUp.next(false);
  }

  private onRevupFailed = (err: any) => {
    // If rev-ups fail (for example, if the collaborator goes offline)
    // it's not a catastrophe but we do need to enqueue a retry
    this.log.warn('Rev-up did not complete due to', err);
    return this.forceReconnect();
  };

  private forceReconnect(): Promise<void> {
    // At this point we may be online
    this.remoteUpdates.detach();
    this.setOnline(false);
    return this.decideOnline();
  }

  private async tryRevup() {
    // For a new clone, last hash is random so this will return undefined
    const revup = await this.remotes.revupFrom(this.localTime);
    if (revup) {
      this.log.info('revving-up from collaborator');
      this.remoteUpdates.inject(revup).then(async lastRevup => {
        // Emit anything in our journal that post-dates the last revup
        const recent = lastRevup && await this.dataset.operationsSince(lastRevup.time);
        recent && recent.subscribe(this.nextUpdate);
        return lastRevup;
      }).then(this.onRevupComplete).catch(this.onRevupFailed);
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
    this.remoteUpdates.inject(concat(updates, from(reEmits).pipe(flatMap(from))))
      .then(this.onRevupComplete).catch(this.onRevupFailed);
    return delivered; // We can go online as soon as the snapshot is delivered
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

  @AbstractMeld.checkOnline.async
  async snapshot(): Promise<Snapshot> {
    return this.onlineLock.acquire(this.id, async () => {
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

  @AbstractMeld.checkOnline.async
  async revupFrom(time: TreeClock): Promise<Observable<DeltaMessage> | undefined> {
    return this.onlineLock.acquire(this.id, async () => {
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
      this.remoteUpdates.received.pipe(
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
      return defer(() => this.onlineLock.enter(this.id))
        .pipe(flatMap(() => this.dataset.read(request)),
          finalize(() => this.onlineLock.leave(this.id)));
    } else if (isSubject(request) || isGroup(request) || isUpdate(request)) {
      // For a write, execute immediately.
      // Take the timestamp as soon as we have the transaction. This prevents a
      // concurrent remote update from causing a clock reversal when it commits.
      const sendTime = this.messageService.send();
      return from(this.onlineLock.enter(this.id)
        .then(() => this.dataset.transact(async () => {
          const patch = await this.dataset.write(request);
          return [sendTime, patch];
        }))
        // Publish the delta
        .then(this.nextUpdate)
        .finally(() => this.onlineLock.leave(this.id)))
        .pipe(ignoreElements()); // Ignores the void promise result
    } else {
      return throwError(new MeldError('Pattern is not read or writeable'));
    }
  }

  async latest(): Promise<number> {
    // If we're revving up, wait until we aren't
    return this.revvingUp.pipe(first(ru => !ru)).toPromise()
      .then(() => this.latestTicks.pipe(first()).toPromise());
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
    this.remoteUpdatesSub.unsubscribe();

    if (this.orderingBuffer.length) {
      this.log.warn(`closed with ${this.orderingBuffer.length} items in ordering buffer
      first: ${this.orderingBuffer[0]}
      time: ${this.localTime}`);
    }
    super.close(err);
    this.remotes.setLocal(null);
    await this.dataset.close(err);
  }
}