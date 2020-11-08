import { MeldUpdate, MeldConstraint, MeldReadState, readResult, Resource, ReadResult } from '../../api';
import { EncodedDelta, Snapshot, UUID, DeltaMessage } from '..';
import { Quad } from 'rdf-js';
import { TreeClock } from '../clocks';
import { Hash } from '../hash';
import { Subject } from '../../jrql-support';
import { Dataset, PatchQuads } from '.';
import { flatten as flatJsonLd } from 'jsonld';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph } from './JrqlGraph';
import { MeldEncoding, unreify, hashTriple, toDomainQuad, reifyTriplesTids } from '../MeldEncoding';
import { Observable, from, Subject as Source, EMPTY } from 'rxjs';
import { bufferCount, mergeMap, reduce, map, filter, takeWhile, expand } from 'rxjs/operators';
import { flatten, Future, tapComplete, getIdLogger, check } from '../util';
import { generate as uuid } from 'short-uuid';
import { Logger } from 'loglevel';
import { MeldError } from '../MeldError';
import { LocalLock } from '../local';
import { SUSET_CONTEXT, qsName, toPrefixedId } from './SuSetGraph';
import { SuSetJournal, SuSetJournalEntry } from './SuSetJournal';
import { MeldConfig, Read } from '../..';
import { QuadMap, TripleMap, Triple, rdfToJson } from '../quads';

interface HashTid extends Subject {
  '@id': Iri; // hash:<hashed triple id>
  tid: UUID; // Transaction ID
}

interface AllTids extends Subject {
  '@id': 'qs:all'; // Singleton object
  tid: UUID[];
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

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant deltas.
 */
export class SuSetDataset extends JrqlGraph {
  private static checkNotClosed =
    check((d: SuSetDataset) => !d.dataset.closed, () => new MeldError('Clone has closed'));

  private readonly meldEncoding: MeldEncoding;
  private readonly tidsGraph: JrqlGraph;
  private readonly journal: SuSetJournal;
  private readonly updateSource: Source<MeldUpdate> = new Source;
  private readonly datasetLock: LocalLock;
  private readonly maxDeltaSize: number;
  private readonly log: Logger;
  private readonly state: MeldReadState;

  constructor(
    private readonly dataset: Dataset,
    private readonly constraint: MeldConstraint,
    config: MeldConfig) {
    super(dataset.graph());
    this.meldEncoding = new MeldEncoding(config['@domain']);
    // Named graph for control quads e.g. Journal (note graph name is legacy)
    this.journal = new SuSetJournal(new JrqlGraph(
      dataset.graph(qsName('control')), SUSET_CONTEXT));
    this.tidsGraph = new JrqlGraph(
      dataset.graph(qsName('tids')), SUSET_CONTEXT);
    // Update notifications are strictly ordered but don't hold up transactions
    this.datasetLock = new LocalLock(config['@id'], dataset.location);
    this.maxDeltaSize = config.maxDeltaSize ?? Infinity;
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.state = new GraphState(this);
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
      prepare: async () => {
        const journalPatch = await this.journal.initialise();
        return journalPatch != null ? { patch: journalPatch } : {};
      }
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
    return (await this.journal.state()).maybeTime;
  }

  @SuSetDataset.checkNotClosed.async
  async saveClock(localTime: TreeClock, newClone?: boolean): Promise<void> {
    return this.dataset.transact({
      id: 'suset-save-clock',
      prepare: async () =>
        ({ patch: await (await this.journal.state()).setLocalTime(localTime, newClone) })
    });
  }

  /**
   * @return the last hash seen in the journal.
   */
  @SuSetDataset.checkNotClosed.async
  async lastHash(): Promise<Hash> {
    const journal = await this.journal.state();
    return (await journal.tail()).hash;
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
        const journal = await this.journal.state();
        // How many ticks of mine has the requester seen?
        const tick = time.getTicks(journal.time);
        if (lastTime != null)
          journal.tail().then(tail => tail.time).then(...lastTime.settle);
        const found = tick != null ? await journal.findEntry(tick) : '';
        return {
          value: !found ? undefined : from(found.next()).pipe(
            expand(entry => {
              if (this.dataset.closed)
                throw new MeldError('Clone has closed');
              return entry != null ? entry.next() : EMPTY;
            }),
            takeWhile<SuSetJournalEntry>(entry => entry != null),
            // Don't emit an entry if it's all less than the requested time
            filter(entry => time.anyLt(entry.time, 'includeIds')),
            map(entry => new DeltaMessage(entry.time, entry.delta)))
        };
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<DeltaMessage | null> {
    return this.dataset.transact<DeltaMessage | null>({
      id: uuid(), // New transaction ID
      prepare: async txc => {
        const [time, patch] = await prepare();
        if (patch.isEmpty)
          return { value: null };

        txc.sw.next('check-constraints');
        const update = await this.asUpdate(time, patch);
        await this.constraint.check(this.state, update);

        txc.sw.next('find-tids');
        const deletedTriplesTids = await this.findTriplesTids(patch.oldQuads);
        const delta = await this.txnDelta(txc.id, patch.newQuads, asTriplesTids(deletedTriplesTids));
        const deltaMessage = new DeltaMessage(time, delta.encoded);
        if (deltaMessage.size() > this.maxDeltaSize)
          throw new MeldError('Delta too big');

        // Include tid changes in final patch
        txc.sw.next('new-tids');
        const { allTidsPatch, tidPatch } =
          await this.txnTidPatches(txc.id, patch.newQuads, deletedTriplesTids);

        // Include journaling in final patch
        txc.sw.next('journal');
        const journal = await this.journal.state(), tail = await journal.tail();
        let { patch: journaling, tailId } = await tail.createNext({ delta, localTime: time });
        journaling = journaling.concat(await journal.setTail(tailId, time));
        return {
          patch: this.transactionPatch(patch, allTidsPatch, tidPatch, journaling),
          value: deltaMessage,
          after: () => this.updateSource.next(update)
        };
      }
    });
  }

  private async txnTidPatches(tid: string, insert: Quad[], deletedTriplesTids: QuadMap<Quad[]>) {
    const allTidsPatch = await this.newTid(tid);
    const tidPatch = (await this.newTriplesTid(insert, tid))
      .concat({ oldQuads: flatten([...deletedTriplesTids].map(([_, tids]) => tids)) });
    return { allTidsPatch, tidPatch };
  }

  private txnDelta(tid: string, insert: Quad[], deletedTriplesTids: TripleMap<UUID[]>) {
    return this.meldEncoding.newDelta({
      tid, insert,
      // Delta has reifications of old quads, which we infer from found triple tids
      delete: reifyTriplesTids(deletedTriplesTids)
    });
  }

  @SuSetDataset.checkNotClosed.async
  async apply(
    msgData: EncodedDelta, msgTime: TreeClock,
    arrivalTime: TreeClock, localTime: TreeClock): Promise<DeltaMessage | null> {
    return this.dataset.transact<DeltaMessage | null>({
      id: msgData[1],
      prepare: async txc => {
        // Check we haven't seen this transaction before in the journal
        txc.sw.next('find-tids');
        if (!(await this.tidsGraph.find1<AllTids>({ '@id': 'qs:all', tid: [txc.id] }))) {
          this.log.debug(`Applying tid: ${txc.id}`);

          txc.sw.next('unreify');
          const delta = await this.meldEncoding.asDelta(msgData);
          let patch = new PatchQuads([], delta.insert.map(toDomainQuad));
          let allTidsPatch = await this.newTid(delta.tid);
          // The delta's delete contains reifications of deleted triples
          let tidPatch = await unreify(delta.delete)
            .reduce(async (tripleTidPatch, [triple, theirTids]) => {
              // For each unique deleted triple, subtract the claimed tids from the tids we have
              const ourTripleTids = await this.findTripleTids(tripleId(triple));
              const toRemove = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid.object.value));
              // If no tids are left, delete the triple in our graph
              if (toRemove.length > 0 && toRemove.length == ourTripleTids.length)
                patch.add('oldQuads', toDomainQuad(triple));
              return (await tripleTidPatch).concat({ oldQuads: toRemove });
            }, Promise.resolve(new PatchQuads()));
          // Done determining the applied delta patch. At this point we could
          // have an empty patch, but we still need to complete the journal.

          txc.sw.next('apply-cx'); // "cx" = constraint
          let { cxn, update } = await this.constrainUpdate(patch, arrivalTime, delta.tid);
          // After applying the constraint, patch new quads might have changed
          tidPatch = tidPatch.concat(await this.newTriplesTid(patch.newQuads, delta.tid));

          // Include journaling in final patch
          txc.sw.next('journal');
          const journal = await this.journal.state(), tail = await journal.tail();
          const mainEntryDetails = { delta, localTime: arrivalTime, remoteTime: msgTime };
          let { patch: journaling, tailId } = await (cxn == null ?
            tail.createNext(mainEntryDetails) :
            // Also create an entry for the constraint "transaction"
            tail.createNext(mainEntryDetails, { delta: cxn.delta, localTime }));
          journaling = journaling.concat(await journal.setTail(tailId, localTime));
          // If the constraint has done anything, we need to merge its work
          if (cxn != null) {
            allTidsPatch = allTidsPatch.concat(cxn.allTidsPatch);
            tidPatch = tidPatch.concat(cxn.tidPatch);
            patch = patch.concat(cxn.patch);
            // Re-create the update with the constraint resolution included
            update = patch.isEmpty ? null : await this.asUpdate(localTime, patch)
          }
          return {
            patch: this.transactionPatch(patch, allTidsPatch, tidPatch, journaling),
            // FIXME: If this delta message exceeds max size, what to do?
            value: cxn != null ? new DeltaMessage(localTime, cxn.delta.encoded) : null,
            after: () => update && this.updateSource.next(update)
          };
        } else {
          this.log.debug(`Rejecting tid: ${txc.id} as duplicate`);
          // We don't have to save the new local clock time, nothing's happened
          return { value: null };
        }
      }
    });
  }

  private async constrainUpdate(patch: PatchQuads, arrivalTime: TreeClock, tid: string) {
    const update = patch.isEmpty ? null : await this.asUpdate(arrivalTime, patch);
    const cxn = update == null ? null : await this.applyConstraint({ patch, update, tid });
    return { cxn, update };
  }

  /**
   * Caution: mutates to.patch
   * @param to transaction details to apply the patch to
   * @param localTime local clock time
   */
  private async applyConstraint(
    to: { patch: PatchQuads, update: MeldUpdate, tid: string }) {
    const result = await this.constraint.apply(this.state, to.update);
    if (result != null) {
      const patch = await this.write(result);
      if (!patch.isEmpty) {
        const tid = uuid();
        // Triples that were inserted in the applied transaction may have been
        // deleted by the constraint - these need to be removed from the applied
        // transaction patch but still published in the constraint delta
        const deletedExistingTidQuads = await this.findTriplesTids(patch.oldQuads);
        const deletedTriplesTids = asTriplesTids(deletedExistingTidQuads);
        to.patch.removeAll('newQuads', patch.oldQuads)
          .forEach(delQuad => deletedTriplesTids.with(delQuad, () => []).push(to.tid));
        // Anything deleted by the constraint that did not exist before the
        // applied transaction can now be removed from the constraint patch
        patch.removeAll('oldQuads', quad => deletedExistingTidQuads.get(quad) == null);
        return {
          patch,
          delta: await this.txnDelta(tid, patch.newQuads, deletedTriplesTids),
          ...await this.txnTidPatches(tid, patch.newQuads, deletedExistingTidQuads)
        };
      }
    }
    return null;
  }

  /**
   * Rolls up the given transaction details into a single patch. This method is
   * just a type convenience for ensuring everything needed for a transaction is
   * present.
   * @param time the local time of the transaction
   * @param dataPatch the transaction data patch
   * @param allTidsPatch insertion to qs:all TIDs in TID graph
   * @param tripleTidPatch triple TID patch (inserts and deletes)
   * @param journaling transaction journaling patch
   */
  private transactionPatch(
    dataPatch: PatchQuads,
    allTidsPatch: PatchQuads,
    tripleTidPatch: PatchQuads,
    journaling: PatchQuads): PatchQuads {
    return dataPatch.concat(allTidsPatch).concat(tripleTidPatch).concat(journaling);
  }

  private async asUpdate(time: TreeClock, patch: PatchQuads): Promise<MeldUpdate> {
    return {
      '@ticks': time.ticks,
      '@delete': await this.toSubjects(patch.oldQuads),
      '@insert': await this.toSubjects(patch.newQuads)
    };
  }

  /**
   * @returns flattened subjects compacted with no context
   * @see https://www.w3.org/TR/json-ld11/#flattened-document-form
   */
  private async toSubjects(quads: Quad[]): Promise<Subject[]> {
    // The flatten function is guaranteed to create a graph object
    const graph: any = await flatJsonLd(await rdfToJson(quads), {});
    return graph['@graph'];
  }

  private newTid(tid: UUID | UUID[]): Promise<PatchQuads> {
    return this.tidsGraph.insert(<AllTids>{ '@id': 'qs:all', tid });
  }

  private newTriplesTid(triples: Triple[], tid: UUID): Promise<PatchQuads> {
    return this.tidsGraph.insert(triples.map<HashTid>(
      triple => ({ '@id': tripleId(triple), tid })));
  }

  private newTripleTids(triple: Triple, tids: UUID[]): Promise<PatchQuads> {
    const theTripleId = tripleId(triple);
    return this.tidsGraph.insert(tids.map<HashTid>(
      tid => ({ '@id': theTripleId, tid })));
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

  private findTripleTids(tripleId: string): Promise<Quad[]> {
    return this.tidsGraph.findQuads({ '@id': tripleId } as Partial<HashTid>);
  }

  /**
   * Applies a snapshot to this dataset.
   * Caution: uses multiple transactions, so the world must be held up by the caller.
   * @param snapshot snapshot with batches of quads and tids
   * @param localTime the time of the local process, to be saved
   */
  @SuSetDataset.checkNotClosed.async
  async applySnapshot(snapshot: DatasetSnapshot, localTime: TreeClock) {
    // First reset the dataset with the given parameters.
    // Note that this is not awaited because the tids and quads are hot, so we
    // must subscribe to them on the current tick. The transaction lock will
    // take care of making sure the reset happens first.
    const dataReset = this.dataset.transact({
      id: 'suset-reset',
      prepare: async () =>
        ({ patch: await this.journal.reset(snapshot.lastHash, snapshot.lastTime, localTime) })
    });

    const tidsApplied = snapshot.tids.pipe(mergeMap(
      // Each batch of TIDs goes in happily as an array
      tids => this.dataset.transact({
        id: 'snapshot-tids-batch',
        prepare: async () => ({ patch: await this.newTid(tids) })
      }))).toPromise();

    const quadsApplied = snapshot.quads.pipe(
      // For each batch of reified quads with TIDs, first unreify
      mergeMap(async batch => this.dataset.transact({
        id: 'snapshot-batch',
        prepare: async () => ({
          patch: await from(unreify(batch)).pipe(
            // For each triple in the batch, insert the TIDs into the tids graph
            mergeMap(async ([triple, tids]) => (await this.newTripleTids(triple, tids))
              // And include the triple itself
              .concat({ newQuads: [toDomainQuad(triple)] })),
            // Concat all of the resultant batch patches together
            reduce((batchPatch, entryPatch) => batchPatch.concat(entryPatch)))
            .toPromise()
        })
      }))).toPromise();
    return Promise.all([dataReset, quadsApplied, tidsApplied]);
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
          const journal = await this.journal.state();
          const tail = await journal.tail();
          resolve({
            lastTime: tail.time,
            lastHash: tail.hash,
            quads: this.graph.match().pipe(
              bufferCount(10), // TODO batch size config
              mergeMap(async batch => reifyTriplesTids(
                asTriplesTids(await this.findTriplesTids(batch)))),
              tapComplete(dataEmitted)),
            tids: this.tidsGraph.graph.match(qsName('all'), qsName('#tid')).pipe(
              map(tid => tid.object.value), bufferCount(10)) // TODO batch size config
          });
          await dataEmitted; // If this rejects, data will error
          return {}; // No patch to apply
        }
      }).catch(reject);
    });
  }
}

function tripleId(triple: Triple): string {
  return toPrefixedId('thash', hashTriple(triple).encode());
}
