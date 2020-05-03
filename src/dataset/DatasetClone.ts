import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldJournalEntry } from '../m-ld';
import { Pattern, Subject, isRead, Group, DeleteInsert } from '../m-ld/jsonrql';
import { Observable, Subject as Source, PartialObserver, merge, from, Observer, Subscription } from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import { publishReplay, refCount, filter, ignoreElements, takeUntil, tap, isEmpty, mergeAll, finalize, first } from 'rxjs/operators';
import { Hash } from '../hash';
import { delayUntil, Future, tapComplete, tapCount, ReentrantLock } from '../util';
import { LogLevelDesc } from 'loglevel';
import { MeldError, HAS_UNSENT } from '../m-ld/MeldError';
import { AbstractMeld } from '../AbstractMeld';

export interface DatasetCloneOpts {
  logLevel?: LogLevelDesc; // = 'info'
  reconnectDelayMillis?: number; // = 1000
}

export class DatasetClone extends AbstractMeld<MeldJournalEntry> implements MeldClone {
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: DeltaMessage[] = [];

  private readonly remoteUpdateReceiver: PartialObserver<DeltaMessage> = {
    next: delta => {
      this.log.debug(`${this.id}: Receiving ${delta} @ ${this.localTime}`);
      this.messageService.receive(delta, this.orderingBuffer, msg => {
        this.log.debug(`${this.id}: Accepting ${delta}`);
        this.dataset.apply(msg.data, msg.time, this.localTime);
      });
    },
    error: err => this.close(err),
    complete: () => this.close()
  };
  private remoteUpdatesSub: Subscription | null = null;
  private connectLock = new ReentrantLock;
  private newClone: boolean = false;

  constructor(dataset: Dataset,
    private readonly remotes: MeldRemotes,
    logLevel: LogLevelDesc = 'info') {
    super(dataset.id, logLevel);
    this.dataset = new SuSetDataset(dataset, logLevel);
    this.remotes.setLocal(this);
  }

  async initialise(): Promise<void> {
    await this.dataset.initialise();
    // Establish a clock for this clone
    let time = await this.dataset.loadClock();
    if (!time) {
      time = await this.remotes.newClock();
      this.newClone = !time.isId; // New clone means non-genesis
      await this.dataset.saveClock(time, true);
    }
    this.log.info(`${this.id}: has time ${time}`);
    this.messageService = new TreeClockMessageService(time);
    this.remotes.online.subscribe(online => {
      if (online) {
        this.connect().then(() => this.setOnline(true), err => {
          // This usually indicates that the remotes have gone offline during
          // our connection attempt. If they have reconnected, another attempt
          // will have already been queued on the connect lock.
          this.log.info(`${this.id}: Can't connect to remotes due to ${err}`);
        });
      }
    });
    if (this.newClone) {
      // For a new non-genesis clone, the first connect is essential
      await this.online.pipe(filter(online => online), first()).toPromise();
    } else {
      // Allow an existing or genesis clone to be online with offline remotes
      if (!await AbstractMeld.isOnline(this.remotes))
        this.setOnline(true);
    }
  }

  private connect(): Promise<void> {
    if (this.remoteUpdatesSub != null)
      this.remoteUpdatesSub.unsubscribe();
    // Block transactions and other connect attempts while connecting
    return this.connectLock.acquire(this.id, async () => {
      this.log.info(`${this.id}: Connecting to remotes`);
      await this.flushUnsentOperations();
      if (this.localTime.isId) { // Top-level is Id, never been forked
        // No rev-up to do, so just subscribe to updates from later clones
        this.log.info(`${this.id}: Genesis clone subscribing to updates`);
        this.subscribeToUpdates();
      } else {
        const revups = new Source<DeltaMessage>();
        this.subscribeToUpdates(revups);
        if (this.newClone) {
          this.log.info(`${this.id}: New clone requesting snapshot`);
          await this.requestSnapshot(revups);
        } else {
          await this.tryRevup(revups);
        }
      }
      this.log.info(`${this.id}: connected.`);
    });
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
    this.remoteUpdatesSub = remoteUpdates.subscribe(this.remoteUpdateReceiver);
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
        .subscribe(entry => this.nextUpdate(entry), reject, resolve);
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
          this.nextUpdate(journalEntry);
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
    super.close(err);
    if (this.remoteUpdatesSub != null)
      this.remoteUpdatesSub.unsubscribe();
    return this.dataset.close(err);
  }
}