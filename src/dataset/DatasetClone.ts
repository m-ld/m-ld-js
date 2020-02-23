import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldJournalEntry } from '../m-ld';
import { Pattern, Subject, Update, isRead } from '../m-ld/jsonrql';
import { Observable, Subject as Source, PartialObserver, merge, empty, from, concat } from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import { catchError, publishReplay, refCount, filter, ignoreElements, materialize, dematerialize } from 'rxjs/operators';
import { Hash } from '../hash';

export class DatasetClone implements MeldClone {
  readonly updates: Source<MeldJournalEntry> = new Source;
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private isGenesis: boolean = false;
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
  }

  set genesis(isGenesis: boolean) {
    this.isGenesis = isGenesis;
  }

  get id() {
    return this.dataset.id;
  }

  async initialise(): Promise<void> {
    await this.dataset.initialise();
    // Establish a clock for this clone
    let newClone: boolean = true, time: TreeClock | null;
    if (this.isGenesis) {
      time = TreeClock.GENESIS;
    } else if (newClone = !(time = await this.dataset.loadClock())) {
      time = await this.remotes.newClock();
    }
    if (newClone)
      this.dataset.saveClock(time, true);
    this.messageService = new TreeClockMessageService(time);
    // Flush unsent operations
    await new Promise<void>((resolve, reject) => {
      this.dataset.unsentLocalOperations().subscribe(
        entry => this.updates.next(entry), reject, resolve);
    });
    if (this.isGenesis) {
      // No rev-up to do, so just subscribe to updates from later clones
      this.remotes.updates.subscribe(this.updateReceiver);
    } else {
      const remoteRevups = new Source<DeltaMessage>();
      merge(this.remotes.updates, remoteRevups.pipe(catchError(this.revupLost)))
        .subscribe(this.updateReceiver);
      if (newClone) {
        await this.requestSnapshot(remoteRevups);
      } else {
        const revup = await this.remotes.revupFrom(await this.dataset.lastHash());
        if (revup) {
          revup.subscribe(remoteRevups);
        } else {
          await this.requestSnapshot(remoteRevups);
        }
      }
    }
    this.remotes.connect(this);
  }

  private revupLost(err: any) {
    // Not a catastrophe but may get ordering overflow later
    console.warn(`Revup lost with ${err}`);
    return empty();
  }

  private async requestSnapshot(remoteRevups: PartialObserver<DeltaMessage>) {
    const snapshot = await this.remotes.snapshot();
    // Deliver the message immediately through the message service
    const snapshotMsg = { time: snapshot.time } as DeltaMessage;
    await new Promise<void>((resolve, reject) => {
      this.messageService.deliver(snapshotMsg, this.orderingBuffer, acceptedMsg => {
        if (acceptedMsg == snapshotMsg) {
          this.dataset.applySnapshot(snapshot.data, snapshot.lastHash, snapshot.time, this.localTime)
            .then(resolve, reject);
        } else {
          this.dataset.apply(acceptedMsg.data, this.localTime);
        }
      });
      snapshot.updates.subscribe(remoteRevups);
    });
  }

  async newClock(): Promise<TreeClock> {
    const newClock = this.messageService.fork();
    // Forking is a mutation operation, need to save the new clock state
    await this.dataset.saveClock(this.localTime);
    return newClock;
  }

  async snapshot(): Promise<Snapshot> {
    const dataSnapshot = await this.dataset.takeSnapshot();
    return {
      time: dataSnapshot.time,
      // Snapshotting holds open a transaction, so buffer/replay triples
      data: dataSnapshot.data.pipe(publishReplay(), refCount()),
      lastHash: dataSnapshot.lastHash,
      updates: this.remoteUpdatesBeforeNow()
    };
  }

  private remoteUpdatesBeforeNow(): Observable<DeltaMessage> {
    const now = this.localTime;
    const updatesBeforeNow = merge(
      // #1 Anything that arrives stamped prior to now
      this.remotes.updates.pipe(filter(message => message.time.anyLt(now))),
      // #2 Anything currently in our ordering buffer
      from(this.orderingBuffer));
    // #3 terminate the flow when this process disposes
    return merge(
      updatesBeforeNow.pipe(materialize()),
      this.asCompletable().pipe(materialize()))
      .pipe(dematerialize());
  }

  private asCompletable(): Observable<never> {
    return this.updates.pipe(ignoreElements());
  }

  private get localTime() {
    return this.messageService.peek();
  }

  async revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined> {
    const operations = await this.dataset.operationsSince(lastHash);
    if (operations)
      return concat(operations, this.remoteUpdatesBeforeNow());
  }

  transact(request: Pattern): Observable<Subject> {
    if (isRead(request)) {
      return this.dataset.read(request);
    } else {
      return new Observable(subs => {
        this.dataset.transact(async () => {
          const patch = await this.dataset.write(request);
          return [this.messageService.send(), patch];
        }).then(journalEntry => {
          // Publish the MeldJournalEntry
          this.updates.next(journalEntry);
          subs.complete();
        }, err => subs.error(err));
      });
    }
  }

  follow(): Observable<Update> {
    return this.dataset.updates;
  }

  close(err?: any) {
    console.log('Shutting down clone ' + err ? 'due to ' + err : 'normally');
    this.dataset.close(err);
    if (err)
      this.updates.error(err);
    else
      this.updates.complete();
  }
}