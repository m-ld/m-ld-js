import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldUpdate } from '../m-ld';
import { Pattern, Subject, isRead } from '../m-ld/jsonrql';
import { Observable, Subject as Source, merge, from, Observer, defer, NEVER, EMPTY, concat, BehaviorSubject, Subscription, onErrorResumeNext } from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import { publishReplay, refCount, filter, ignoreElements, takeUntil, tap, isEmpty, finalize, flatMap, switchAll, toArray, catchError, first } from 'rxjs/operators';
import { delayUntil, Future, tapComplete, tapCount, SharableLock, tapLast } from '../util';
import { LogLevelDesc, levels } from 'loglevel';
import { MeldError, HAS_UNSENT } from '../m-ld/MeldError';
import { AbstractMeld, isOnline, comesOnline } from '../AbstractMeld';

export interface DatasetCloneOpts {
  logLevel?: LogLevelDesc; // = 'info'
  reconnectDelayMillis?: number; // = 1000
}

class RemoteUpdates {
  readonly received: Observable<DeltaMessage>;
  private readonly remoteUpdates: Source<Observable<DeltaMessage>> = new Source;

  constructor(
    private readonly remotes: MeldRemotes) {
    this.received = this.remoteUpdates.pipe(switchAll());
  }

  attach = () => this.remoteUpdates.next(this.remotes.updates);
  detach = () => this.remoteUpdates.next(NEVER);

  inject(revups: Observable<DeltaMessage>): PromiseLike<DeltaMessage | undefined> {
    const lastRevup = new Future<DeltaMessage | undefined>();
    // Updates must be paused during revups because the collaborator might
    // send an update while also sending revups of its own prior updates.
    // That would break the ordering guarantee.
    this.remoteUpdates.next(merge(
      // Errors should be handled in the returned promise
      onErrorResumeNext(revups.pipe(tapLast(lastRevup)), NEVER),
      this.remotes.updates.pipe(delayUntil(onErrorResumeNext(from(lastRevup), NEVER)))));
    return lastRevup;
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

  constructor(
    id: string,
    dataset: Dataset,
    remotes: MeldRemotes,
    logLevel: LogLevelDesc = 'info') {
    super(id, logLevel);
    this.dataset = new SuSetDataset(id, dataset, logLevel);
    this.dataset.updates.subscribe(next => this.latestTicks.next(next['@ticks']));
    this.remotes = remotes;
    this.remotes.setLocal(this);
    this.remoteUpdates = new RemoteUpdates(remotes);
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
      time = await this.remotes.newClock();
      this.newClone = !time.isId; // New clone means non-genesis
      await this.dataset.saveClock(time, true);
    }
    this.log.info(`has time ${time}`);
    this.messageService = new TreeClockMessageService(time);
    this.latestTicks.next(time.getTicks());

    this.remoteUpdatesSub = this.remoteUpdates.received.subscribe({
      next: delta => {
        const logBody = this.log.getLevel() < levels.DEBUG ? delta : `tid: ${delta.data.tid}`;
        this.log.debug('Receiving', logBody, '@', this.localTime);
        this.messageService.receive(delta, this.orderingBuffer, msg => {
          this.log.debug('Accepting', logBody);
          this.dataset.apply(msg.data, msg.time, () => this.localTime)
            .then(msg.delivered.resolve)
            .catch(err => this.close(err)); // Catastrophe
        });
      },
      error: err => this.close(err),
      complete: () => this.close()
    });

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
      } else {
        const revups = this.setupRevups();
        if (this.newClone) {
          this.log.info('New clone requesting snapshot');
          await this.requestSnapshot(revups);
        } else {
          await this.tryRevup(revups);
        }
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

  private setupRevups(): Observer<DeltaMessage> {
    // TODO: Change this source to a new Observable(subs)
    const revups = new Source<DeltaMessage>();
    this.remoteUpdates.inject(revups).then(async last => {
      // Here, we are definitely before the first post-revup update, but
      // the actual last revup might not yet have been applied to the dataset.
      if (last != null)
        await last.delivered;
      this.revvingUp.next(false);
    }, err => {
      // If rev-ups fail (for example, if the collaborator goes offline)
      // it's not a catastrophe but we do need to enqueue a retry
      this.log.warn('Rev-up did not complete due to', err);
      // At this point we will be online if the connect was OK
      this.setOnline(false);
      return this.decideOnline();
    }).then(undefined, this.warnError); // TODO: Check data integrity;
    return revups;
  }

  private async tryRevup(consumeRevups: Observer<DeltaMessage>) {
    // For a new clone, last hash is random so this will return undefined
    const revup = await this.remotes.revupFrom(this.localTime);
    if (revup) {
      this.log.info('revving-up from collaborator');
      const lastRevup = new Future<DeltaMessage | undefined>();
      revup.pipe(tapLast(lastRevup)).subscribe(consumeRevups);
      // Emit anything in our journal that post-dates the last revup
      lastRevup
        .then(lastEntry => lastEntry && this.dataset.operationsSince(lastEntry.time))
        .then(recent => recent && recent.subscribe(this.nextUpdate))
        .catch(this.warnError); // TODO Check for data risk
    } else {
      this.log.info('cannot rev-up, requesting snapshot');
      await this.requestSnapshot(consumeRevups);
    }
  }

  private flushUndeliveredOperations() {
    return new Promise<void>((resolve, reject) => {
      const counted = new Future<number>();
      counted.then(n => n && this.log.info(`Emitted ${n} undelivered operations`));
      this.dataset.undeliveredLocalOperations().pipe(tapCount(counted))
        .subscribe(this.nextUpdate, reject, resolve);
    });
  }

  private async requestSnapshot(consumeRevups: Observer<DeltaMessage>) {
    // If there are unsent operations, we would lose data
    if (!(await this.dataset.undeliveredLocalOperations().pipe(isEmpty()).toPromise()))
      throw new MeldError(HAS_UNSENT);

    const snapshot = await this.remotes.snapshot();
    this.messageService.join(snapshot.lastTime);

    // If we have any operations since the snapshot, re-emit them
    const reEmits = this.dataset.operationsSince(snapshot.lastTime)
      .then(recent => (recent ?? EMPTY).pipe(tap(this.nextUpdate), toArray()).toPromise());
    // Start delivering the snapshot when we have done re-emitting
    const delivered = reEmits.then(() =>
      this.dataset.applySnapshot(snapshot, this.localTime));
    // Delay all updates until the snapshot has been fully delivered
    // This is because a snapshot is applied in multiple transactions
    const updates = snapshot.updates.pipe(delayUntil(from(delivered)));
    concat(updates, from(reEmits).pipe(flatMap(from))).subscribe(consumeRevups);
    return delivered; // We can go online as soon as the snapshot is delivered
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
    } else {
      // For a write, execute immediately
      return from(this.onlineLock.enter(this.id)
        .then(() => this.dataset.transact(async () => {
          const patch = await this.dataset.write(request);
          return [this.messageService.send(), patch];
        }))
        // Publish the delta
        .then(this.nextUpdate)
        .finally(() => this.onlineLock.leave(this.id)))
        .pipe(ignoreElements()); // Ignores the void promise result
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
    this.dataset.close(err);
    this.remotes.setLocal(null);
  }
}