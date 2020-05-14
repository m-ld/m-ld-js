import { MeldClone, Snapshot, DeltaMessage, MeldRemotes, MeldJournalEntry } from '../m-ld';
import { Pattern, Subject, isRead, Group, DeleteInsert } from '../m-ld/jsonrql';
import { Observable, Subject as Source, merge, from, Observer, defer, NEVER, EMPTY, concat } from 'rxjs';
import { TreeClock } from '../clocks';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from '../messages';
import { Dataset } from '.';
import { publishReplay, refCount, filter, ignoreElements, takeUntil, tap, isEmpty, finalize, flatMap, switchAll, toArray, catchError } from 'rxjs/operators';
import { delayUntil, Future, tapComplete, tapCount, ReentrantLock, tapLast } from '../util';
import { LogLevelDesc, levels } from 'loglevel';
import { MeldError, HAS_UNSENT } from '../m-ld/MeldError';
import { AbstractMeld, isOnline, comesOnline } from '../AbstractMeld';

export interface DatasetCloneOpts {
  logLevel?: LogLevelDesc; // = 'info'
  reconnectDelayMillis?: number; // = 1000
}

export class DatasetClone extends AbstractMeld<MeldJournalEntry> implements MeldClone {
  private readonly dataset: SuSetDataset;
  private messageService: TreeClockMessageService;
  private readonly orderingBuffer: DeltaMessage[] = [];
  private readonly remoteUpdates: Source<Observable<DeltaMessage>> = new Source;
  private onlineLock = new ReentrantLock;
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
    this.log.info(`has time ${time}`);
    this.messageService = new TreeClockMessageService(time);
    this.remoteUpdates.pipe(switchAll()).subscribe({
      next: delta => {
        const logBody = this.log.getLevel() < levels.DEBUG ? delta : `tid: ${delta.data.tid}`;
        this.log.debug('Receiving', logBody, '@', this.localTime);
        this.messageService.receive(delta, this.orderingBuffer, msg => {
          this.log.debug('Accepting', logBody);
          this.dataset.apply(msg.data, msg.time, () => this.localTime);
        });
      },
      error: err => this.close(err),
      complete: () => this.close()
    });
    return new Promise((resolve, reject) => {
      this.remotes.online.subscribe(remotesOnline => {
        // Block transactions, revups and other connect attempts while handling online change
        this.onlineLock.acquire(this.id, () => {
          if (remotesOnline == null) {
            // We are partitioned from the domain.
            this.remoteUpdates.next(NEVER);
            this.setOnline(false);
          } else if (remotesOnline) {
            return this.connect();
          } else if (!this.newClone) {
            // We are a silo, the last survivor. Stay online for any newcomers.
            this.remoteUpdates.next(this.remotes.updates);
            this.setOnline(true);
          } else {
            reject(new Error('New clone is siloed.'));
          }
        }).catch(this.warnError); // Could not acquire lock
      });
      // For a new non-genesis clone, the first connect is essential
      if (this.newClone)
        comesOnline(this).then(resolve);
      else
        resolve();
    });
  }

  private async connect(): Promise<void> {
    this.log.info('Connecting to remotes');
    try {
      await this.flushUndeliveredOperations();
      const silo = await this.isOnline();
      // If top-level is Id, never been forked, no rev-up to do
      if (silo || this.localTime.isId) {
        this.remoteUpdates.next(this.remotes.updates);
      } else {
        // TODO: Change this source to a new Observable(subs)
        const revups = new Source<DeltaMessage>();
        // Updates must be paused during revups because the collaborator might
        // send an update while also sending revups of its own prior updates.
        // That would break the ordering guarantee.
        const revvedUp = new Future;
        this.remoteUpdates.next(merge(
          revups.pipe(tapComplete(revvedUp), catchError(() => NEVER)),
          this.remotes.updates.pipe(delayUntil(from(revvedUp)))));

        // If rev-ups fail later (for example, if the collaborator goes offline)
        // it's not a catastrophe but we do need to enqueue a retry
        revvedUp.then(null, async err => {
          this.log.warn('Rev-up did not complete due to', err);
          if (await isOnline(this.remotes))
            this.connect();
        }).catch(this.warnError);

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
    this.messageService.join(snapshot.time);

    // If we have any operations since the snapshot, re-emit them
    const reEmits = this.dataset.operationsSince(snapshot.time)
      .then(recent => (recent ?? EMPTY).pipe(tap(this.nextUpdate), toArray()).toPromise());
    // Start delivering the snapshot when we have done re-emitting
    const delivered = reEmits.then(() => this.dataset.applySnapshot(
      snapshot.data, snapshot.lastHash, snapshot.time, this.localTime));
    // Delay all updates until the snapshot has been fully delivered
    // This is because a snapshot is applied in multiple transactions
    const updates = snapshot.updates.pipe(delayUntil(from(delivered)));
    concat(updates, from(reEmits).pipe(flatMap(from))).subscribe(consumeRevups);
    return delivered; // We can go online as soon as the snapshot is delivered
  }

  async newClock(): Promise<TreeClock> {
    const newClock = this.messageService.fork();
    // Forking is a mutation operation, need to save the new clock state
    await this.dataset.saveClock(this.localTime);
    return newClock;
  }

  async snapshot(): Promise<Snapshot> {
    return this.onlineLock.acquire(this.id, async () => {
      this.log.info('Compiling snapshot');
      const sentSnapshot = new Future;
      const updates = this.remoteUpdatesBeforeNow(sentSnapshot);
      const { time, data, lastHash } = await this.dataset.takeSnapshot();
      return {
        time, lastHash, updates,
        // Snapshotting holds open a transaction, so buffer/replay triples
        data: data.pipe(publishReplay(), refCount(), tapComplete(sentSnapshot))
      };
    });
  }

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
      // FIXME: but only if we accept it! It could be a duplicate.
      this.remotes.updates.pipe(
        filter(message => message.time.anyLt(now, 'includeIds')),
        takeUntil(from(until)))).pipe(tap(msg =>
          this.log.debug('Sending update', msg)));
  }

  private get localTime() {
    return this.messageService.peek();
  }

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
        // Publish the MeldJournalEntry
        .then(this.nextUpdate)
        .finally(() => this.onlineLock.leave(this.id)))
        .pipe(ignoreElements()); // Ignores the void promise result
    }
  }

  follow(): Observable<DeleteInsert<Group>> {
    return this.dataset.updates;
  }

  close(err?: any) {
    if (err)
      this.log.warn('Shutting down due to', err);
    else
      this.log.info('Shutting down normally');
    
    // Make sure we never receive another remote update
    this.remoteUpdates.next(EMPTY);
    if (this.orderingBuffer.length) {
      this.log.warn(`closed with ${this.orderingBuffer.length} items in ordering buffer
      first: ${this.orderingBuffer[0]}
      time: ${this.localTime}`);
    }
    super.close(err);
    return this.dataset.close(err);
  }
}