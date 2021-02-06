import {
  MeldUpdate, MeldConstraint, MeldReadState, Resource,
  ReadResult, InterimUpdate, readResult, DeleteInsert
} from '../../api';
import { Snapshot, UUID, DeltaMessage, MeldDelta } from '..';
import { Quad } from 'rdf-js';
import { TreeClock } from '../clocks';
import { Subject, Update } from '../../jrql-support';
import { Dataset, DefinitePatch, PatchQuads } from '.';
import { flatten as flatJsonLd } from 'jsonld';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph } from './JrqlGraph';
import { MeldEncoding, unreify } from '../MeldEncoding';
import { Observable, from, Subject as Source, EMPTY } from 'rxjs';
import {
  bufferCount, mergeMap, reduce, map, filter, takeWhile, expand, toArray
} from 'rxjs/operators';
import { flatten, Future, tapComplete, getIdLogger, check } from '../util';
import { Logger } from 'loglevel';
import { MeldError } from '../MeldError';
import { LocalLock } from '../local';
import { SUSET_CONTEXT, tripleId, txnId } from './SuSetGraph';
import { SuSetJournalDataset, SuSetJournalEntry } from './SuSetJournal';
import { MeldConfig, Read } from '../..';
import { QuadMap, TripleMap, Triple } from '../quads';
import { rdfToJson } from "../jsonld";
import { CheckList } from '../../constraints/CheckList';
import { DefaultList } from '../../constraints/DefaultList';
import { qs } from '../../ns';

interface HashTid extends Subject {
  '@id': Iri; // hash:<hashed triple id>
  tid: UUID; // Transaction ID
}

type DatasetSnapshot = Omit<Snapshot, 'updates'>;

function asTriplesTids(quadTidQuads: QuadMap<Quad[]>): TripleMap<UUID[]> {
  return new TripleMap<UUID[]>([...quadTidQuads].map(([quad, tids]) => {
    return [quad, tids.map(tidQuad => tidQuad.object.value)];
  }));
}

export class GraphState implements MeldReadState {
  constructor(
    readonly graph: JrqlGraph) {
  }

  read<S>(request: Read): ReadResult<Resource<S>> {
    return readResult(this.graph.read(request)
      .pipe(map(subject => <Resource<S>>subject)));
  }

  get<S = Subject>(id: string): Promise<Resource<S> | undefined> {
    return this.graph.describe1(id);
  }
}

class InterimUpdatePatch implements InterimUpdate {
  '@ticks': number;
  '@delete': Subject[];
  '@insert': Subject[];
  /** Assertions made by the constraint (not including the app patch) */
  assertions: PatchQuads;
  /** Entailments made by the constraint */
  entailments = new PatchQuads();

  ready: Promise<InterimUpdatePatch>;

  /**
   * @param patch the starting app patch (will not be mutated unless 'mutable')
   */
  constructor(
    readonly graph: SuSetDataset,
    time: TreeClock,
    readonly patch: PatchQuads,
    readonly mutable?: 'mutable') {
    // If mutable, we treat the app patch as assertions
    this.assertions = mutable ? patch : new PatchQuads();
    this['@ticks'] = time.ticks;
    this.ready = this.graph.asDeleteInsert(patch)
      .then(delIns => Object.assign(this, delIns));
  }

  private async update() {
    const newPatch = this.mutable ? this.assertions :
      new PatchQuads(this.patch).append(this.assertions);
    // FIXME: Inefficient, re-creates the whole update every time
    Object.assign(this, await this.graph.asDeleteInsert(newPatch));
    return this;
  }

  assert(update: Update) {
    this.ready = this.ready
      .then(() => this.graph.write(update))
      .then(patch => this.assertions.append(patch))
      .then(() => this.update());
  }

  entail(update: Update) {
    this.ready = this.ready
      .then(() => this.graph.write(update))
      .then(patch => this.entailments.append(patch))
      .then(() => this);
  }

  remove(key: keyof DeleteInsert<any>, pattern: Subject | Subject[]) {
    this.ready = this.ready
      .then(() => this.graph.definiteQuads(pattern))
      .then(toRemove => this.assertions.remove(
        key == '@delete' ? 'oldQuads' : 'newQuads', toRemove).length ? this.update() : this);
  }
}

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant deltas.
 */
export class SuSetDataset extends JrqlGraph {
  private static checkNotClosed =
    check((d: SuSetDataset) => !d.dataset.closed, () => new MeldError('Clone has closed'));

  private readonly tidsGraph: JrqlGraph;
  private readonly journalData: SuSetJournalDataset;
  private readonly updateSource: Source<MeldUpdate> = new Source;
  private readonly datasetLock: LocalLock;
  private readonly maxDeltaSize: number;
  private readonly log: Logger;
  private readonly state: MeldReadState;
  private readonly constraint: CheckList;

  constructor(
    private readonly dataset: Dataset,
    constraints: MeldConstraint[],
    private readonly encoding: MeldEncoding,
    config: Pick<MeldConfig, '@id' | 'maxDeltaSize' | 'logLevel'>) {
    super(dataset.graph(), {}, encoding.context['@base']);
    this.journalData = new SuSetJournalDataset(dataset);
    this.tidsGraph = new JrqlGraph(
      dataset.graph(encoding.dataFactory.namedNode(qs.tids)), SUSET_CONTEXT);
    // Update notifications are strictly ordered but don't hold up transactions
    this.datasetLock = new LocalLock(config['@id'], dataset.location);
    this.maxDeltaSize = config.maxDeltaSize ?? Infinity;
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.state = new GraphState(this);
    this.constraint = new CheckList(constraints.concat(new DefaultList(config['@id'])));
  }

  @SuSetDataset.checkNotClosed.async
  async initialise() {
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
  async saveClock(prepare: (gwc: TreeClock) => Promise<TreeClock> | TreeClock, newClone?: boolean): Promise<TreeClock> {
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
   * Emits entries from the journal since a time given as a clock or a tick.
   * The clock variant accepts a remote or local clock and provides operations
   * since the last tick of this dataset that the given clock has 'seen'.
   *
   * The ticks variant accepts a local clock tick, as recorded in the journal.
   *
   * To ensure we have processed any prior updates we always process an
   * operations request in a transaction lock.
   *
   * @returns entries from the journal since the given time (exclusive), or
   * `undefined` if the given time is not found in the journal
   */
  @SuSetDataset.checkNotClosed.async
  async operationsSince(time: TreeClock, lastTime?: Future<TreeClock>):
    Promise<Observable<DeltaMessage> | undefined> {
    return this.dataset.transact<Observable<DeltaMessage> | undefined>({
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
            map(([prev, entry]) => new DeltaMessage(
              prev.gwc.getTicks(entry.time), entry.time, entry.delta)))
        };
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<DeltaMessage | null> {
    return this.dataset.transact<DeltaMessage | null>({
      prepare: async txc => {
        const [time, patch] = await prepare();
        if (patch.isEmpty)
          return { return: null };

        txc.sw.next('check-constraints');
        const update = await new InterimUpdatePatch(this, time, patch, 'mutable').ready;
        await this.constraint.check(this.state, update);
        // Wait for all pending modifications
        await update.ready;

        txc.sw.next('find-tids');
        const deletedTriplesTids = await this.findTriplesTids(patch.oldQuads);
        const delta = await this.txnDelta(patch.newQuads, asTriplesTids(deletedTriplesTids));

        // Include tid changes in final patch
        txc.sw.next('new-tids');
        const tidPatch = await this.txnTidPatch(txnId(time), patch.newQuads, deletedTriplesTids);

        // Include journaling in final patch
        txc.sw.next('journal');
        const journal = await this.journalData.journal(), tail = await journal.tail();
        const journaling = tail.builder(journal, { delta, localTime: time });

        return {
          patch: this.transactionPatch(patch, update.entailments, tidPatch),
          kvps: journaling.commit,
          return: this.deltaMessage(tail, time, delta),
          after: () => this.emitUpdate(update)
        };
      }
    });
  }

  private emitUpdate(update: InterimUpdate) {
    if (update['@delete'].length || update['@insert'].length) {
      // De-clutter the update for sanity
      this.updateSource.next({
        '@delete': update['@delete'],
        '@insert': update['@insert'],
        '@ticks': update['@ticks']
      });
    }
  }

  private deltaMessage(tail: SuSetJournalEntry, time: TreeClock, delta: MeldDelta) {
    // Construct the delta message with the previous visible clock tick
    const deltaMsg = new DeltaMessage(tail.gwc.getTicks(time), time, delta.encoded);
    if (deltaMsg.size > this.maxDeltaSize)
      throw new MeldError('Delta too big');
    return deltaMsg;
  }

  private async txnTidPatch(tid: string, insert: Quad[], deletedTriplesTids: QuadMap<Quad[]>) {
    const tidPatch = await this.newTriplesTid(insert, tid);
    tidPatch.append({ oldQuads: flatten([...deletedTriplesTids].map(([_, tids]) => tids)) });
    return tidPatch;
  }

  private txnDelta(insert: Quad[], deletedTriplesTids: TripleMap<UUID[]>) {
    return this.encoding.newDelta({
      insert,
      // Delta has reifications of old quads, which we infer from found triple tids
      delete: this.encoding.reifyTriplesTids(deletedTriplesTids)
    });
  }

  @SuSetDataset.checkNotClosed.async
  async apply(msg: DeltaMessage, localTime: TreeClock, cxnTime: TreeClock): Promise<DeltaMessage | null> {
    return this.dataset.transact<DeltaMessage | null>({
      prepare: async txc => {
        // Check we haven't seen this transaction before
        txc.sw.next('find-tids');
        this.log.debug(`Applying delta: ${msg.time} @ ${localTime}`);

        txc.sw.next('unreify');
        const delta = await this.encoding.asDelta(msg.data);
        const patch = new PatchQuads({ newQuads: delta.insert.map(this.encoding.toDomainQuad) });
        const tidPatch = await this.processSuDeletions(delta.delete, patch);

        txc.sw.next('apply-cx'); // "cx" = constraint
        const tid = txnId(msg.time);
        const update = await new InterimUpdatePatch(this, cxnTime, patch).ready;
        await this.constraint.apply(this.state, update);
        const cxn = await this.constraintTxn((await update.ready).assertions, patch, tid, cxnTime);
        // After applying the constraint, patch new quads might have changed
        tidPatch.append(await this.newTriplesTid(patch.newQuads, tid));

        // Done determining the applied delta patch. At this point we could
        // have an empty patch, but we still need to complete the journal
        // entry for it.
        txc.sw.next('journal');
        const journal = await this.journalData.journal(), tail = await journal.tail();
        const journaling = tail.builder(
          journal, { delta, localTime, remoteTime: msg.time });

        // If the constraint has done anything, we need to merge its work
        if (cxn != null) {
          // update['@ticks'] = cxnTime.ticks;
          tidPatch.append(cxn.tidPatch);
          patch.append(update.assertions);
          // Also create a journal entry for the constraint "transaction"
          journaling.next({ delta: cxn.delta, localTime: cxnTime });
        }
        return {
          patch: this.transactionPatch(patch, update.entailments, tidPatch),
          kvps: journaling.commit,
          // FIXME: If this delta message exceeds max size, what to do?
          return: cxn?.delta != null ? this.deltaMessage(tail, cxnTime, cxn.delta) : null,
          after: () => this.emitUpdate(update)
        };
      }
    });
  }

  // The delta's delete contains reifications of deleted triples.
  // This method adds the resolved deletions to the given transaction patch.
  private processSuDeletions(deltaDeletions: Triple[], patch: PatchQuads) {
    return unreify(deltaDeletions)
      .reduce(async (tripleTidPatch, [triple, theirTids]) => {
        // For each unique deleted triple, subtract the claimed tids from the tids we have
        const ourTripleTids = await this.findTripleTids(tripleId(triple));
        const toRemove = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid.object.value));
        // If no tids are left, delete the triple in our graph
        if (toRemove.length > 0 && toRemove.length == ourTripleTids.length)
          patch.append({ oldQuads: [this.encoding.toDomainQuad(triple)] });
        return (await tripleTidPatch).append({ oldQuads: toRemove });
      }, Promise.resolve(new PatchQuads()));
  }

  /**
   * Caution: mutates to.patch
   * @param to transaction details to apply the patch to
   * @param localTime local clock time
   */
  private async constraintTxn(
    assertions: PatchQuads, patch: PatchQuads, tid: string, cxnTime: TreeClock) {
    if (!assertions.isEmpty) {
      // Triples that were inserted in the applied transaction may have been
      // deleted by the constraint - these need to be removed from the applied
      // transaction patch but still published in the constraint delta
      const deletedExistingTidQuads = await this.findTriplesTids(assertions.oldQuads);
      const deletedTriplesTids = asTriplesTids(deletedExistingTidQuads);
      patch.remove('newQuads', assertions.oldQuads)
        .forEach(delQuad => deletedTriplesTids.with(delQuad, () => []).push(tid));
      // Anything deleted by the constraint that did not exist before the
      // applied transaction can now be removed from the constraint patch
      assertions.remove('oldQuads', quad => deletedExistingTidQuads.get(quad) == null);
      return {
        delta: await this.txnDelta(assertions.newQuads, deletedTriplesTids),
        tidPatch: await this.txnTidPatch(
          txnId(cxnTime), assertions.newQuads, deletedExistingTidQuads)
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
    assertions: DefinitePatch, entailments: DefinitePatch, tidPatch: DefinitePatch): PatchQuads {
    return new PatchQuads(assertions).append(entailments).append(tidPatch);
  }

  async asDeleteInsert(patch: DefinitePatch) {
    return {
      '@delete': await this.toSubjects(patch.oldQuads ?? []),
      '@insert': await this.toSubjects(patch.newQuads ?? [])
    };
  }

  /**
   * @returns flattened subjects compacted with no context
   * @see https://www.w3.org/TR/json-ld11/#flattened-document-form
   */
  private async toSubjects(quads: Iterable<Quad>): Promise<Subject[]> {
    // The flatten function is guaranteed to create a graph object
    const graph: any = await flatJsonLd(await rdfToJson(quads), {});
    return graph['@graph'];
  }

  private newTriplesTid(triples: Triple[], tid: UUID): Promise<PatchQuads> {
    return this.tidsGraph.update({
      '@insert': triples.map<HashTid>(triple => ({ '@id': tripleId(triple), tid }))
    });
  }

  private newTripleTids(triple: Triple, tids: UUID[]): Promise<PatchQuads> {
    const theTripleId = tripleId(triple);
    return this.tidsGraph.update({
      '@insert': tids.map<HashTid>(tid => ({ '@id': theTripleId, tid }))
    });
  }

  private async findTriplesTids(quads: Quad[]): Promise<QuadMap<Quad[]>> {
    const quadTriplesTids = new QuadMap<Quad[]>();
    await Promise.all(quads.map(async quad => {
      const tripleTids = await this.findTripleTids(tripleId(quad));
      if (tripleTids.length)
        quadTriplesTids.set(quad, tripleTids);
    }));
    return quadTriplesTids;
  }

  private findTripleTids(tripleId: string): PromiseLike<Quad[]> {
    return this.tidsGraph.findQuads({ '@id': tripleId } as Partial<HashTid>)
      .pipe(toArray()).toPromise();
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
        prepare: async () => ({
          patch: await from(unreify(batch)).pipe(
            // For each triple in the batch, insert the TIDs into the tids graph
            mergeMap(async ([triple, tids]) => (await this.newTripleTids(triple, tids))
              // And include the triple itself
              .append({ newQuads: [this.encoding.toDomainQuad(triple)] })),
            // Concat all of the resultant batch patches together
            reduce((batchPatch, entryPatch) => batchPatch.append(entryPatch)))
            .toPromise()
        })
      }))).toPromise();
  }

  /**
   * Takes a snapshot of data, including transaction IDs.
   * This requires a consistent view, so a transaction lock is taken until all data has been emitted.
   * To avoid holding up the world, buffer the data.
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
            quads: this.graph.match().pipe(
              bufferCount(10), // TODO batch size config
              mergeMap(async batch => this.encoding.reifyTriplesTids(
                asTriplesTids(await this.findTriplesTids(batch)))),
              tapComplete(dataEmitted))
          });
          await dataEmitted; // If this rejects, data will error
          return {}; // No patch to apply
        }
      }).catch(reject);
    });
  }
}
