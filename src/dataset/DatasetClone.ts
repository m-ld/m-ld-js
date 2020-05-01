import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldJournalEntry } from '../m-ld';
import { Pattern, Subject, isRead, Group, DeleteInsert } from '../m-ld/jsonrql';
import { Observable, Subject as Source, PartialObserver, merge, from, asapScheduler, Observer, EMPTY, Subscription } from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import { publishReplay, refCount, filter, ignoreElements, observeOn, takeUntil, tap, isEmpty, mergeAll, finalize, debounceTime } from 'rxjs/operators';
import { Hash } from '../hash';
import { delayUntil, Future, tapComplete, tapCount, ReentrantLock } from '../util';
import { LogLevelDesc, getLogger, Logger } from 'loglevel';
import { MeldError, HAS_UNSENT } from '../m-ld/MeldError';

export interface DatasetCloneOpts {
  logLevel?: LogLevelDesc; // = 'info'
  reconnectDelayMillis?: number; // = 1000
}

export class DatasetClone implements MeldClone {
  readonly updates: Observable<MeldJournalEntry>;
  private readonly updateSource: Source<MeldJournalEntry> = new Source;
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: DeltaMessage[] = [];
  private readonly updateReceiver: PartialObserver<DeltaMessage> = {
    next: delta => {
      this.log.debug(`${this.id}: Receiving ${delta} @ ${this.localTime}`);
      this.messageService.receive(delta, this.orderingBuffer, msg => {
        this.log.debug(`${this.id}: Accepting ${delta}`);
        this.dataset.apply(msg.data, msg.time, this.localTime);
      });
    },
    error: err => {
      // TODO: https://github.com/gsvarovsky/m-ld-spec/issues/8
      this.log.warn(`${this.id}: ${err}`);
      this.reconnect.next();
    },
    complete: () => {
      this.log.debug(`${this.id}: Connection to remotes lost`);
      this.reconnect.next();
    }
  };
  private connectLock = new ReentrantLock;
  private readonly reconnect = new Source<boolean>();
  private readonly reconnectSub: Subscription;
  private updatesSub: Subscription | undefined;
  private readonly log: Logger;

  constructor(dataset: Dataset,
    private readonly remotes: MeldRemotes,
    opts: DatasetCloneOpts) {
    this.dataset = new SuSetDataset(dataset, opts.logLevel);
    // Update notifications are delayed to ensure internal processing has priority
    this.updates = this.updateSource.pipe(observeOn(asapScheduler),
      tap(msg => this.log.debug(`${this.id}: Sending ${msg}`)));
    // Reconnections are debounced due to the number of ways they can happen
    // TODO: Configure reconnect delay
    this.reconnectSub = this.reconnect
      .pipe(debounceTime(opts.reconnectDelayMillis ?? 1000))
      .subscribe(newClone => this.connect(newClone));
    this.log = getLogger(this.id);
    this.log.setLevel(opts.logLevel ?? 'info');
  }

  get id() {
    return this.dataset.id;
  }

  async initialise(): Promise<void> {
    await this.dataset.initialise();
    // Establish a clock for this clone
    let newClone = false, time = await this.dataset.loadClock();
    if (!time) {
      newClone = true;
      time = await this.remotes.newClock();
      await this.dataset.saveClock(time, true);
    }
    this.log.info(`${this.id}: has time ${time}`);
    this.messageService = new TreeClockMessageService(time);
    return this.connect(newClone);
  }

  /**
   * @param newClone if {@code true} this is definitely a new clone (if undefined, may still be)
   */
  private async connect(newClone?: boolean): Promise<void> {
    if (this.updatesSub != null)
      this.updatesSub.unsubscribe();
    // Don't bother trying to connect if we're partitioned
    if (this.remotes.connected) {
      // Block transactions while connecting
      return this.connectLock.acquire(this.id, () => {
        this.remotes.setLocal(this);
        return this.doConnect(newClone).then(() => {
          this.remotes.localReady = true;
        }, err => {
          this.log.info(`${this.id}: Can't connect to remotes due to ${err}`);
          // Try again later
          this.reconnect.next(newClone);
        });
      });
    } else {
      this.reconnect.next(newClone);
    }
  }

  private async doConnect(newClone?: boolean) {
    this.log.info(`${this.id}: Connecting to remotes`);
    await this.flushUnsentOperations();
    if (this.localTime.isId) { // Top-level is Id, never been forked
      // No rev-up to do, so just subscribe to updates from later clones
      this.log.info(`${this.id}: Genesis clone subscribing to updates`);
      this.subscribeToUpdates();
    } else {
      const revups = new Source<DeltaMessage>();
      this.subscribeToUpdates(revups);
      if (newClone) {
        this.log.info(`${this.id}: New clone requesting snapshot`);
        await this.requestSnapshot(revups);
      } else {
        await this.tryRevup(revups);
      }
    }
    this.log.info(`${this.id}: connected.`);
  }

  private subscribeToUpdates(revups?: Observable<DeltaMessage>) {
    this.log.info(`${this.id}: Subscribing to updates`);
    let remoteUpdates: Observable<DeltaMessage>;
    if (revups != null) {
      const revvedUp = new Future;
      remoteUpdates = merge(revups.pipe(tapComplete(revvedUp)),
        // Allow through updates when the rev-up is complete
        this.remotes.updates.pipe(delayUntil(from(revvedUp))))
    } else {
      remoteUpdates = this.remotes.updates;
    }
    this.updatesSub = remoteUpdates.subscribe(this.updateReceiver);
  }

  private async tryRevup(consumeRevups: Observer<DeltaMessage>) {
    // For a new clone, last hash is random so this will return undefined
    const revup = await this.remotes.revupFrom(await this.dataset.lastHash());
    if (revup) {
      this.log.info(`${this.id}: revving-up from collaborator`);
      revup.subscribe(consumeRevups);
    } else {
      this.log.info(`${this.id}: cannot rev-up, requesting snapshot`);
      await this.requestSnapshot(consumeRevups);
    }
  }

  private flushUnsentOperations() {
    return new Promise<void>((resolve, reject) => {
      const counted = new Future<number>();
      counted.then(n => n && this.log.info(`${this.id}: Emitting ${n} unsent operations`));
      this.dataset.unsentLocalOperations().pipe(tapCount(counted))
        .subscribe(entry => this.updateSource.next(entry), reject, resolve);
    });
  }

  private async requestSnapshot(consumeRevups: Observer<DeltaMessage>) {
    // If there are unsent operations, we would lose data
    if (!(await this.dataset.unsentLocalOperations().pipe(isEmpty()).toPromise()))
      throw new MeldError(HAS_UNSENT);

    const snapshot = await this.remotes.snapshot();
    this.messageService.join(snapshot.time);
    const delivered = this.dataset.applySnapshot(
      snapshot.data, snapshot.lastHash, snapshot.time, this.localTime);
    // Delay all updates until the snapshot has been fully delivered
    // This is because a snapshot is applied in multiple transactions
    snapshot.updates.pipe(delayUntil(from(delivered))).subscribe(consumeRevups);
    return delivered;
  }

  async newClock(): Promise<TreeClock> {
    const newClock = this.messageService.fork();
    // Forking is a mutation operation, need to save the new clock state
    await this.dataset.saveClock(this.localTime);
    return newClock;
  }

  async snapshot(): Promise<Snapshot> {
    this.log.info(`${this.id}: Compiling snapshot`);
    const sentSnapshot = new Future;
    const updates = this.remoteUpdatesBefore(this.localTime, sentSnapshot);
    const { time, data, lastHash } = await this.dataset.takeSnapshot();
    return {
      time, lastHash, updates,
      // Snapshotting holds open a transaction, so buffer/replay triples
      data: data.pipe(publishReplay(), refCount(), tapComplete(sentSnapshot))
    };
  }

  private remoteUpdatesBefore(now: TreeClock, until: PromiseLike<void>): Observable<DeltaMessage> {
    if (this.orderingBuffer.length)
      this.log.info(`${this.id}: Emitting ${this.orderingBuffer.length} from ordering buffer`);
    return merge(
      // #1 Anything currently in our ordering buffer
      from(this.orderingBuffer),
      // #2 Anything that arrives stamped prior to now
      // FIXME: but only if we accept it! It could be a duplicate.
      this.remotes.updates.pipe(
        filter(message => message.time.anyLt(now)),
        takeUntil(from(until)))).pipe(tap(msg =>
          this.log.debug(`${this.id}: Sending update ${msg}`)));
  }

  private get localTime() {
    return this.messageService.peek();
  }

  async revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined> {
    const sentOperations = new Future;
    const maybeMissed = this.remoteUpdatesBefore(this.localTime, sentOperations);
    const operations = await this.dataset.operationsSince(lastHash);
    if (operations)
      return merge(
        operations.pipe(tapComplete(sentOperations), tap(msg =>
          this.log.debug(`${this.id}: Sending rev-up ${msg}`))),
        maybeMissed.pipe(delayUntil(from(sentOperations))));
  }

  transact(request: Pattern): Observable<Subject> {
    // Block connect while transacting
    return from(this.connectLock.enter(this.id).then(() => {
      if (isRead(request)) {
        return this.dataset.read(request);
      } else {
        return from(this.dataset.transact(async () => {
          const patch = await this.dataset.write(request);
          return [this.messageService.send(), patch];
        }).then(journalEntry => {
          // Publish the MeldJournalEntry
          this.updateSource.next(journalEntry);
        })).pipe(ignoreElements()); // Ignores the void promise result
      }
    })).pipe(mergeAll(), finalize(() => this.connectLock.leave(this.id)));
  }

  follow(): Observable<DeleteInsert<Group>> {
    return this.dataset.updates;
  }

  close(err?: any) {
    this.log.info(`${this.id}: Shutting down clone ${err ? 'due to ' + err : 'normally'}`);
    if (this.orderingBuffer.length) {
      this.log.warn(`${this.id}: closed with ${this.orderingBuffer.length} items in ordering buffer
      first: ${this.orderingBuffer[0]}
      time: ${this.localTime}`);
    }
    if (err)
      this.updateSource.error(err);
    else
      this.updateSource.complete();
    this.reconnectSub.unsubscribe();
    if (this.updatesSub != null)
      this.updatesSub.unsubscribe();
    return this.dataset.close(err);
  }
}