import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldJournalEntry } from './meld';
import { Pattern, Subject, Update, isRead } from './jsonrql';
import { Observable } from 'rxjs';
import { TreeClock } from './clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from './messages';
import { Dataset } from './Dataset';

export class DatasetClone implements MeldClone {
  private readonly dataset: SuSetDataset;
  private readonly messageService: TreeClockMessageService;
  private isGenesis: boolean = false;

  constructor(dataset: Dataset,
    private readonly remotes: MeldRemotes) {
    this.dataset = new SuSetDataset(dataset);
    // TODO
    this.messageService = new TreeClockMessageService(TreeClock.GENESIS);
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
    // Flush unsent operations
  }

  updates(): Observable<MeldJournalEntry> {
    throw new Error('Method not implemented.');
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
          // TODO publish the MeldJournalEntry as a delta message
          subs.complete();
        }, err => subs.error(err));
      });
    }
  }

  follow(): Observable<Update> {
    throw new Error('Method not implemented.');
  }
}