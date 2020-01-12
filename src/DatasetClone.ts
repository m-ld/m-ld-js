import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldJournalEntry } from './meld';
import { Pattern, Subject, Update, isRead } from './jsonrql';
import { Observable, Subject as Source } from 'rxjs';
import { TreeClock } from './clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from './messages';
import { Dataset } from './Dataset';

export class DatasetClone implements MeldClone {
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  readonly updates: Source<MeldJournalEntry> = new Source;
  private isGenesis: boolean = false;

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
      // this.remotes.updates.subscribe({
      //   next: delta => { },
      //   error: err => { },
      //   complete: () => { }
      // });
    } else {

    }
  }

  newClock(): Promise<TreeClock> {
    throw new Error('Method not implemented.');
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