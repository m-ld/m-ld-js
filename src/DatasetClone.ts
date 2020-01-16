import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldJournalEntry } from './meld';
import { Pattern, Subject, Update, isRead } from './jsonrql';
import { Observable, Subject as Source, PartialObserver, merge, empty } from 'rxjs';
import { TreeClock } from './clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from './messages';
import { Dataset } from './Dataset';
import { catchError } from 'rxjs/operators';

export class DatasetClone implements MeldClone {
  readonly updates: Source<MeldJournalEntry> = new Source;
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private isGenesis: boolean = false;
  private readonly orderingBuffer: DeltaMessage[] = [];
  private readonly updateReceiver: PartialObserver<DeltaMessage> = {
    next: delta => this.messageService.receive(delta, this.orderingBuffer, acceptedMsg =>
      this.dataset.apply(acceptedMsg.data, this.messageService.peek())),
    error: err => this.updates.error(err),
    complete: () => this.updates.complete()
  };

  constructor(dataset: Dataset,
    private readonly remotes: MeldRemotes) {
    this.dataset = new SuSetDataset(dataset);
  }

  set genesis(isGenesis: boolean) {
    this.isGenesis = isGenesis;
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
      merge(this.remotes.updates, remoteRevups.pipe(catchError(err => {
        // Not a catastrophe but may get ordering overflow later
        console.warn(`Revup lost with ${err}`);
        return empty();
      }))).subscribe(this.updateReceiver);
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

  private async requestSnapshot(remoteRevups: PartialObserver<DeltaMessage>) {
    const snapshot = await this.remotes.snapshot();
    // Deliver the message immediately through the message service
    const snapshotMsg = { time: snapshot.time } as DeltaMessage;
    await new Promise<void>((resolve, reject) => {
      this.messageService.deliver(snapshotMsg, this.orderingBuffer, acceptedMsg => {
        if (acceptedMsg == snapshotMsg) {
          this.dataset.applySnapshot(snapshot.data, snapshot.lastHash, snapshot.time, this.messageService.peek())
            .then(resolve, reject);
        } else {
          this.dataset.apply(acceptedMsg.data, this.messageService.peek());
        }
      });
      snapshot.updates.subscribe(remoteRevups);
    });
  }

  async newClock(): Promise<TreeClock> {
    const newClock = this.messageService.fork();
    // Forking is a mutation operation, need to save the new clock state
    await this.dataset.saveClock(this.messageService.peek());
    return newClock;
  }

  snapshot(): Promise<Snapshot> {
    throw new Error('Method not implemented.');
  }

  revupFrom(): Promise<Observable<DeltaMessage>> {
    throw new Error('Method not implemented.');
  }

  transact(request: Pattern): Observable<Subject> {
    if (isRead(request)) {
      return new Observable(subs => {
        this.dataset.read(request).then(subjects => {
          subjects.forEach(subject => subs.next(subject));
          subs.complete();
        }, err => subs.error(err));
      });
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
    throw new Error('Method not implemented.');
  }
}