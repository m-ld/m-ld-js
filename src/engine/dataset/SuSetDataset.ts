import { MeldUpdate, MeldConstraint, MeldReadState } from '../../api';
import { Snapshot, UUID, OperationMessage } from '..';
import { Quad } from 'rdf-js';
import { TreeClock } from '../clocks';
import { Context, Subject, Write } from '../../jrql-support';
import { Dataset, PatchQuads } from '.';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph } from './JrqlGraph';
import { MeldEncoding, MeldOperation, TriplesTids, unreify } from '../MeldEncoding';
import { Observable, from, Subject as Source, EMPTY } from 'rxjs';
import {
  bufferCount, mergeMap, map, filter, takeWhile, expand, toArray
} from 'rxjs/operators';
import { flatten, Future, tapComplete, getIdLogger, check } from '../util';
import { Logger } from 'loglevel';
import { MeldError } from '../MeldError';
import { LocalLock } from '../local';
import { SUSET_CONTEXT, tripleId } from './SuSetGraph';
import { SuSetJournalDataset, SuSetJournalEntry } from './SuSetJournal';
import { array, GraphSubject, MeldConfig, Read } from '../..';
import { QuadMap, TripleMap, Triple } from '../quads';
import { CheckList } from '../../constraints/CheckList';
import { DefaultList } from '../../constraints/DefaultList';
import { QS } from '../../ns';
import { InterimUpdatePatch } from './InterimUpdatePatch';
import { GraphState } from './GraphState';
import { ActiveContext } from 'jsonld/lib/context';
import { activeCtx } from '../jsonld';

interface HashTid extends Subject {
  '@id': Iri; // hash:<hashed triple id>
  tid: UUID; // Transaction ID
}

type DatasetSnapshot = Omit<Snapshot, 'updates'>;

function asTriplesTids(quadTidQuads: QuadMap<Quad[]>): TriplesTids {
  return [...quadTidQuads].map(([quad, tidQuads]) =>
    [quad, tidQuads.map(tidQuad => tidQuad.object.value)]);
}

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant operations.
 */
export class SuSetDataset {
  private static checkNotClosed =
    check((d: SuSetDataset) => !d.dataset.closed, () => new MeldError('Clone has closed'));

  readonly encoding: MeldEncoding;
  /** External context used for reads, writes and updates, but not for constraints. */
  /*readonly*/ userCtx: ActiveContext;

  private /*readonly*/ userGraph: JrqlGraph;
  private /*readonly*/ tidsGraph: JrqlGraph;
  private /*readonly*/ state: MeldReadState;
  private readonly journalData: SuSetJournalDataset;
  private readonly updateSource: Source<MeldUpdate> = new Source;
  private readonly datasetLock: LocalLock;
  private readonly maxOperationSize: number;
  private readonly log: Logger;
  private readonly constraint: CheckList;

  constructor(
    private readonly dataset: Dataset,
    private readonly context: Context,
    constraints: MeldConstraint[],
    config: Pick<MeldConfig, '@id' | '@domain' | 'maxOperationSize' | 'logLevel'>) {
    this.encoding = new MeldEncoding(config['@domain'], dataset.dataFactory);
    this.journalData = new SuSetJournalDataset(dataset);
    // Update notifications are strictly ordered but don't hold up transactions
    this.datasetLock = new LocalLock(config['@id'], dataset.location);
    this.maxOperationSize = config.maxOperationSize ?? Infinity;
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.constraint = new CheckList(constraints.concat(new DefaultList(config['@id'])));
  }

  @SuSetDataset.checkNotClosed.async
  async initialise() {
    this.userCtx = await activeCtx(this.context);
    this.userGraph = new JrqlGraph(this.dataset.graph());
    this.tidsGraph = new JrqlGraph(this.dataset.graph(
      this.dataset.dataFactory.namedNode(QS.tids)), await activeCtx(SUSET_CONTEXT));
    this.state = new GraphState(this.userGraph);
    await this.encoding.initialise();
    // Check for exclusive access to the dataset location
    try {
      await this.datasetLock.acquire();
    } catch (err) {
      throw new MeldError('Clone data is locked', err);
    }
    // Create the Journal if not exists
    return this.dataset.transact({
      id: 'suset-reset',
      prepare: async () => ({ kvps: await this.journalData.initialise() })
    });
  }

  get updates(): Observable<MeldUpdate> {
    return this.updateSource;
  }

  read<R extends Read>(request: R): Observable<GraphSubject> {
    return this.userGraph.read(request, this.userCtx);
  }

  write(request: Write): Promise<PatchQuads> {
    return this.userGraph.write(request, this.userCtx);
  }

  @SuSetDataset.checkNotClosed.async
  async close(err?: any) {
    if (err) {
      this.log.warn('Shutting down due to', err);
      this.updateSource.error(err);
    } else {
      this.log.info('Shutting down normally');
      this.updateSource.complete();
    }
    this.datasetLock.release();
    return this.dataset.close();
  }

  @SuSetDataset.checkNotClosed.async
  async loadClock(): Promise<TreeClock | null> {
    return (await this.journalData.journal()).time;
  }

  @SuSetDataset.checkNotClosed.async
  async saveClock(
    prepare: (gwc: TreeClock) => Promise<TreeClock> | TreeClock,
    newClone?: boolean): Promise<TreeClock> {
    return this.dataset.transact<TreeClock>({
      id: 'suset-save-clock',
      prepare: async () => {
        const journal = await this.journalData.journal(),
          tail = await journal.tail(),
          newClock = await prepare(tail.gwc);
        return {
          kvps: journal.setLocalTime(newClock, newClone),
          return: newClock
        };
      }
    });
  }

  /**
   * Accepts a remote or local clock and provides operations since the last tick
   * of this dataset that the given clock has 'seen'.
   *
   * To ensure we have processed any prior updates we always process an
   * operations request in a transaction lock.
   *
   * @returns entries from the journal since the given time (exclusive), or
   * `undefined` if the given time is not found in the journal
   */
  @SuSetDataset.checkNotClosed.async
  async operationsSince(time: TreeClock, lastTime?: Future<TreeClock>):
    Promise<Observable<OperationMessage> | undefined> {
    return this.dataset.transact<Observable<OperationMessage> | undefined>({
      id: 'suset-ops-since',
      prepare: async () => {
        const journal = await this.journalData.journal();
        // How many ticks of mine has the requester seen?
        const tick = time.getTicks(journal.safeTime);
        if (lastTime != null)
          journal.tail().then(tail => tail.gwc).then(...lastTime.settle);
        const found = tick != null ? await journal.entry(tick) : '';
        const nextEntry = async (entry: SuSetJournalEntry) => [entry, await entry.next()];
        return {
          return: !found ? undefined : from(nextEntry(found)).pipe(
            expand(([_, entry]) => {
              if (this.dataset.closed)
                throw new MeldError('Clone has closed');
              return entry != null ? nextEntry(entry) : EMPTY;
            }),
            takeWhile<[SuSetJournalEntry, SuSetJournalEntry]>(([_, entry]) => entry != null),
            // Don't emit an entry if it's all less than the requested time
            filter(([_, entry]) => time.anyLt(entry.time, 'includeIds')),
            map(([prev, entry]) => new OperationMessage(
              prev.gwc.getTicks(entry.time), entry.operation, entry.time)))
        };
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        const [time, patch] = await prepare();
        if (patch.isEmpty)
          return { return: null };

        txc.sw.next('check-constraints');
        const interim = new InterimUpdatePatch(this.userGraph, time, patch, 'mutable');
        await this.constraint.check(this.state, interim);
        const { update, entailments } = await interim.finalise(this.userCtx);

        txc.sw.next('find-tids');
        const deletedTriplesTids = await this.findTriplesTids(patch.deletes);
        const tid = time.hash();
        const op = await this.txnOperation(tid, time, patch.inserts, asTriplesTids(deletedTriplesTids));

        // Include tid changes in final patch
        txc.sw.next('new-tids');
        const tidPatch = await this.txnTidPatch(tid, patch.inserts, deletedTriplesTids);

        // Include journaling in final patch
        txc.sw.next('journal');
        const journal = await this.journalData.journal(), tail = await journal.tail();
        const journaling = tail.builder(journal, op, time);

        return {
          patch: this.transactionPatch(patch, entailments, tidPatch),
          kvps: journaling.commit,
          return: this.operationMessage(tail, time, op),
          after: () => this.emitUpdate(update)
        };
      }
    });
  }

  private emitUpdate(update: MeldUpdate) {
    if (update['@delete'].length || update['@insert'].length)
      this.updateSource.next(update);
  }

  private operationMessage(tail: SuSetJournalEntry, time: TreeClock, op: MeldOperation) {
    const prev = tail.gwc.getTicks(time);
    // Construct the operation message with the previous visible clock tick
    const operationMsg = new OperationMessage(prev, op.encoded, time);
    if (operationMsg.size > this.maxOperationSize)
      throw new MeldError('Delta too big');
    return operationMsg;
  }

  private async txnTidPatch(tid: string, insert: Iterable<Quad>, deletedTriplesTids: QuadMap<Quad[]>) {
    const tidPatch = await this.newTriplesTids([...insert].map(quad => [quad, [tid]]));
    tidPatch.append({ deletes: flatten([...deletedTriplesTids].map(([_, tids]) => tids)) });
    return tidPatch;
  }

  private txnOperation(tid: string, time: TreeClock,
    inserts: Iterable<Quad>, deletedTriplesTids: TriplesTids) {
    const insertedTriplesTids: TriplesTids = [...inserts].map(quad => [quad, [tid]]);
    return MeldOperation.fromOperation(this.encoding, time.ticks, time,
      deletedTriplesTids, insertedTriplesTids);
  }

  toUserQuad = (triple: Triple): Quad => this.encoding.rdf.quad(
    triple.subject,
    triple.predicate,
    triple.object,
    this.encoding.rdf.defaultGraph());

  @SuSetDataset.checkNotClosed.async
  async apply(msg: OperationMessage, localTime: TreeClock, cxnTime: TreeClock): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        // Check we haven't seen this transaction before
        txc.sw.next('find-tids');
        const op = await MeldOperation.fromEncoded(this.encoding, msg.data);
        this.log.debug(`Applying operation: ${op.time} @ ${localTime}`);

        txc.sw.next('apply-txn');
        // First delete triples and entries from fused transactions
        const patch = await this.spliceFusedTids(op.from, op.time);
        // Process deletions and inserts
        const insertTids = new TripleMap(op.inserts);
        const tidPatch = await this.processSuDeletions(op.deletes, patch);
        patch.append({ inserts: op.inserts.map(([triple]) => this.toUserQuad(triple)) });
        
        txc.sw.next('apply-cx'); // "cx" = constraint
        const interim = new InterimUpdatePatch(this.userGraph, cxnTime, patch);
        await this.constraint.apply(this.state, interim);
        const { update, assertions, entailments } = await interim.finalise(this.userCtx);
        const cxn = await this.constraintTxn(assertions, patch, insertTids, cxnTime);
        // After applying the constraint, some new quads might have been removed
        tidPatch.append(await this.newTriplesTids(
          [...patch.inserts].map(quad => [quad, array(insertTids.get(quad))])));

        // Done determining the applied operation patch. At this point we could
        // have an empty patch, but we still need to complete the journal
        // entry for it.
        txc.sw.next('journal');
        const journal = await this.journalData.journal(), tail = await journal.tail();
        const journaling = tail.builder(journal, op, localTime);

        // If the constraint has done anything, we need to merge its work
        if (cxn != null) {
          // update['@ticks'] = cxnTime.ticks;
          tidPatch.append(cxn.tidPatch);
          patch.append(assertions);
          // Also create a journal entry for the constraint "transaction"
          journaling.next(cxn.operation, cxnTime);
        }
        return {
          patch: this.transactionPatch(patch, entailments, tidPatch),
          kvps: journaling.commit,
          // FIXME: If this operation message exceeds max size, what to do?
          return: cxn?.operation != null ? this.operationMessage(tail, cxnTime, cxn.operation) : null,
          after: () => this.emitUpdate(update)
        };
      }
    });
  }

  // The operation's delete contains reifications of deleted triples.
  // This method adds the resolved deletions to the given transaction patch.
  private processSuDeletions(opDeletions: TriplesTids, patch: PatchQuads) {
    return opDeletions.reduce(async (tripleTidPatch, [triple, theirTids]) => {
      // For each unique deleted triple, subtract the claimed tids from the tids we have
      const ourTripleTids = await this.findTripleTids(tripleId(triple));
      const toRemove = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid.object.value));
      // If no tids are left, delete the triple in our graph
      if (toRemove.length > 0 && toRemove.length == ourTripleTids.length)
        patch.append({ deletes: [this.toUserQuad(triple)] });
      return (await tripleTidPatch).append({ deletes: toRemove });
    }, Promise.resolve(new PatchQuads()));
  }

  /**
   * Caution: mutates to.patch
   * @param to transaction details to apply the patch to
   * @param localTime local clock time
   */
  private async constraintTxn(
    assertions: PatchQuads, patch: PatchQuads, insertTids: TripleMap<UUID[]>, cxnTime: TreeClock) {
    if (!assertions.isEmpty) {
      // Triples that were inserted in the applied transaction may have been
      // deleted by the constraint - these need to be removed from the applied
      // transaction patch but still published in the constraint operation
      const deletedExistingTidQuads = await this.findTriplesTids(assertions.deletes);
      const deletedTriplesTids = new TripleMap(asTriplesTids(deletedExistingTidQuads));
      patch.remove('inserts', assertions.deletes)
        .forEach(delQuad => deletedTriplesTids.with(delQuad, () => [])
          .push(...array(insertTids.get(delQuad))));
      // Anything deleted by the constraint that did not exist before the
      // applied transaction can now be removed from the constraint patch
      assertions.remove('deletes', quad => deletedExistingTidQuads.get(quad) == null);
      const cxnId = cxnTime.hash();
      return {
        operation: await this.txnOperation(cxnId, cxnTime, assertions.inserts, [...deletedTriplesTids]),
        tidPatch: await this.txnTidPatch(cxnId, assertions.inserts, deletedExistingTidQuads)
      };
    }
  }

  /**
   * Rolls up the given transaction details into a single patch. This method is
   * just a type convenience for ensuring everything needed for a transaction is
   * present.
   * @param assertions the transaction data patch
   * @param tidPatch triple TID patch (inserts and deletes)
   */
  private transactionPatch(
    assertions: PatchQuads, entailments: PatchQuads, tidPatch: PatchQuads): PatchQuads {
    return new PatchQuads(assertions).append(entailments).append(tidPatch);
  }

  private newTriplesTids(tripleTids: [Triple, UUID[]][]): Promise<PatchQuads> {
    return this.tidsGraph.write({
      '@insert': flatten(tripleTids.map(([triple, tids]) => {
        const theTripleId = tripleId(triple);
        return tids.map<HashTid>(tid => ({ '@id': theTripleId, tid }));
      }))
    });
  }

  private async findTriplesTids(quads: Iterable<Quad>, includeEmpty?: 'includeEmpty'): Promise<QuadMap<Quad[]>> {
    const quadTriplesTids = new QuadMap<Quad[]>();
    await Promise.all([...quads].map(async quad => {
      const tripleTids = await this.findTripleTids(tripleId(quad));
      if (tripleTids.length || includeEmpty)
        quadTriplesTids.set(quad, tripleTids);
    }));
    return quadTriplesTids;
  }

  private findTripleTids(tripleId: string): PromiseLike<Quad[]> {
    return this.tidsGraph.findQuads({ '@id': tripleId } as Partial<HashTid>)
      .pipe(toArray()).toPromise();
  }

  private async spliceFusedTids(start: number, endTime: TreeClock): Promise<PatchQuads> {
    const tids: string[] = [];
    for (let time = endTime.ticked(start); time.ticks < endTime.ticks; time = time.ticked())
      tids.push(time.hash());
    // TODO
    return new PatchQuads();
  }

  /**
   * Applies a snapshot to this dataset.
   * Caution: uses multiple transactions, so the world must be held up by the caller.
   * @param snapshot snapshot with batches of quads and tids
   * @param localTime the time of the local process, to be saved
   */
  @SuSetDataset.checkNotClosed.async
  async applySnapshot(snapshot: DatasetSnapshot, localTime: TreeClock) {
    await this.dataset.clear();
    await this.dataset.transact({
      id: 'suset-reset',
      prepare: async () =>
        ({ kvps: this.journalData.reset(snapshot.lastTime, localTime) })
    });
    await snapshot.quads.pipe(
      // For each batch of reified quads with TIDs, first unreify
      mergeMap(async batch => this.dataset.transact({
        id: 'snapshot-batch',
        prepare: async () => {
          const triplesTids = unreify(batch);
          // For each triple in the batch, insert the TIDs into the tids graph
          const patch = await this.newTriplesTids(triplesTids);
          // And include the triples themselves
          patch.append({ inserts: triplesTids.map(([triple]) => this.toUserQuad(triple)) });
          return { patch };
        }
      }))).toPromise();
  }

  /**
   * Takes a snapshot of data, including transaction IDs. This requires a
   * consistent view, so a transaction lock is taken until all data has been
   * emitted. To avoid holding up the world, buffer the data.
   */
  @SuSetDataset.checkNotClosed.async
  async takeSnapshot(): Promise<DatasetSnapshot> {
    return new Promise((resolve, reject) => {
      this.dataset.transact({
        id: 'snapshot',
        prepare: async () => {
          const dataEmitted = new Future;
          const journal = await this.journalData.journal();
          const tail = await journal.tail();
          resolve({
            lastTime: tail.gwc,
            quads: this.userGraph.graph.match().pipe(
              bufferCount(10), // TODO batch size config
              mergeMap(async batch => this.encoding.reifyTriplesTids(
                asTriplesTids(await this.findTriplesTids(batch, 'includeEmpty')))),
              tapComplete(dataEmitted))
          });
          await dataEmitted; // If this rejects, data will error
          return {}; // No patch to apply
        }
      }).catch(reject);
    });
  }
}
