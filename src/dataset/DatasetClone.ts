import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldJournalEntry } from '../m-ld';
import { Pattern, Subject, isRead, Group, DeleteInsert } from '../m-ld/jsonrql';
import { Observable, Subject as Source, PartialObserver, merge, from, concat, asapScheduler } from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import { publishReplay, refCount, filter, ignoreElements, observeOn, takeUntil } from 'rxjs/operators';
import { Hash } from '../hash';
import { delayUntil, Future, tapComplete } from '../util';

export class DatasetClone implements MeldClone {
  readonly updates: Observable<MeldJournalEntry>;
  private readonly updateSource: Source<MeldJournalEntry> = new Source;
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: DeltaMessage[] = [];
  private readonly updateReceiver: PartialObserver<DeltaMessage> = {
    next: delta => this.messageService.receive(delta, this.orderingBuffer, acceptedMsg =>
      this.dataset.apply(acceptedMsg.data, this.localTime)),
    error: err => this.close(err),
    complete: () => this.close()
  };

  constructor(dataset: Dataset,
    private readonly remotes: MeldRemotes) {
    this.dataset = new SuSetDataset(dataset);
    // Update notifications are delayed to ensure internal processing has priority
    this.updates = this.updateSource.pipe(observeOn(asapScheduler));
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
    console.info(`${this.id}: has time ${time}`);
    this.messageService = new TreeClockMessageService(time);
    // Flush unsent operations
    await new Promise<void>((resolve, reject) => {
      this.dataset.unsentLocalOperations().subscribe(
        entry => this.updateSource.next(entry), reject, resolve);
    });
    if (time.isId) { // Top-level is Id, never been forked
      // No rev-up to do, so just subscribe to updates from later clones
      console.info(`${this.id}: Genesis clone subscribing to updates`);
      this.remotes.updates.subscribe(this.updateReceiver);
    } else {
      console.info(`${this.id}: Subscribing to updates`);
      const remoteRevups = new Source<DeltaMessage>();
      const revvedUp = new Future<void>();
      concat(
        remoteRevups.pipe(tapComplete(revvedUp)),
        // Don't allow through updates until the rev-up is complete
        this.remotes.updates.pipe(delayUntil(from(revvedUp))))
        .subscribe(this.updateReceiver);
      if (newClone) {
        console.info(`${this.id}: New clone requesting snapshot`);
        await this.requestSnapshot(remoteRevups);
      } else {
        const revup = await this.remotes.revupFrom(await this.dataset.lastHash());
        if (revup) {
          console.info(`${this.id}: revving-up from collaborator`);
          revup.subscribe(remoteRevups);
        } else {
          console.info(`${this.id}: cannot rev-up, requesting snapshot`);
          await this.requestSnapshot(remoteRevups);
        }
      }
    }
    console.info(`${this.id}: started.`);
    this.remotes.connect(this);
  }

  private async requestSnapshot(remoteRevups: PartialObserver<DeltaMessage>) {
    const snapshot = await this.remotes.snapshot();
    this.messageService.join(snapshot.time);
    const delivered = this.dataset.applySnapshot(
      snapshot.data, snapshot.lastHash, snapshot.time, this.localTime);
    // Delay all updates until the snapshot has been fully delivered
    // This is because a snapshot is applied in multiple transactions
    snapshot.updates.pipe(delayUntil(from(delivered))).subscribe(remoteRevups);
    return delivered;
  }

  async newClock(): Promise<TreeClock> {
    const newClock = this.messageService.fork();
    // Forking is a mutation operation, need to save the new clock state
    await this.dataset.saveClock(this.localTime);
    return newClock;
  }

  async snapshot(): Promise<Snapshot> {
    console.info(`${this.id}: Compiling snapshot`);
    const sentSnapshot = new Future<void>();
    const updates = this.remoteUpdatesBefore(this.localTime, sentSnapshot);
    const { time, data, lastHash } = await this.dataset.takeSnapshot();
    return {
      time, lastHash, updates,
      // Snapshotting holds open a transaction, so buffer/replay triples
      data: data.pipe(
        publishReplay(), refCount(), tapComplete(sentSnapshot))
    };
  }

  private remoteUpdatesBefore(now: TreeClock, until: PromiseLike<void>): Observable<DeltaMessage> {
    return merge(
      // #1 Anything currently in our ordering buffer
      from(this.orderingBuffer),
      // #2 Anything that arrives stamped prior to now
      // FIXME: but only if we accept it! It could be a duplicate.
      this.remotes.updates.pipe(
        filter(message => message.time.anyLt(now)),
        takeUntil(from(until))));
  }

  private get localTime() {
    return this.messageService.peek();
  }

  async revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined> {
    const sentOperations = new Future<void>();
    const maybeMissed = this.remoteUpdatesBefore(this.localTime, sentOperations);
    const operations = await this.dataset.operationsSince(lastHash);
    if (operations)
      return concat(operations.pipe(tapComplete(sentOperations)), maybeMissed);
  }

  transact(request: Pattern): Observable<Subject> {
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
  }

  follow(): Observable<DeleteInsert<Group>> {
    return this.dataset.updates;
  }

  close(err?: any) {
    console.log(`${this.id}: Shutting down clone ${err ? 'due to ' + err : 'normally'}`);
    if (err)
      this.updateSource.error(err);
    else
      this.updateSource.complete();
    return this.dataset.close(err);
  }
}