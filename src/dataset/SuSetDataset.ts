import { JsonDelta, Snapshot, UUID, MeldUpdate, DeltaMessage, Triple } from '../m-ld';
import { Quad } from 'rdf-js';
import { TreeClock } from '../clocks';
import { Hash } from '../hash';
import { Subject } from './jrql-support';
import { Dataset, PatchQuads, Patch } from '.';
import { flatten as flatJsonLd } from 'jsonld';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph } from './JrqlGraph';
import { newDelta, asMeldDelta, reify, unreify, hashTriple, toDomainQuad } from '../m-ld/MeldJson';
import { Observable, from, Subject as Source, asapScheduler, Observer } from 'rxjs';
import { toArray, bufferCount, flatMap, reduce, observeOn, map } from 'rxjs/operators';
import { flatten, Future, tapComplete, getIdLogger, check, rdfToJson } from '../util';
import { generate as uuid } from 'short-uuid';
import { LogLevelDesc, Logger } from 'loglevel';
import { MeldError } from '../m-ld/MeldError';
import { LocalLock } from '../local';
import { SUSET_CONTEXT, qsName, toPrefixedId } from './SuSetGraph';
import { SuSetJournal, SuSetJournalEntry } from './SuSetJournal';

interface HashTid extends Subject {
  '@id': Iri; // hash:<hashed triple id>
  tid: UUID; // Transaction ID
}

interface AllTids extends Subject {
  '@id': 'qs:all'; // Singleton object
  tid: UUID[];
}

type DatasetSnapshot = Omit<Snapshot, 'updates'>;

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant deltas.
 */
export class SuSetDataset extends JrqlGraph {
  private static checkNotClosed =
    check((d: SuSetDataset) => !d.dataset.closed, () => new MeldError('Clone has closed'));

  private readonly tidsGraph: JrqlGraph;
  private readonly journal: SuSetJournal;
  private readonly updateSource: Source<MeldUpdate> = new Source;
  readonly updates: Observable<MeldUpdate>
  private readonly datasetLock: LocalLock;
  private readonly log: Logger;

  constructor(id: string,
    private readonly dataset: Dataset, logLevel: LogLevelDesc = 'info') {
    super(dataset.graph());
    // Named graph for control quads e.g. Journal (note graph name is legacy)
    this.journal = new SuSetJournal(new JrqlGraph(
      dataset.graph(qsName('control')), SUSET_CONTEXT));
    this.tidsGraph = new JrqlGraph(
      dataset.graph(qsName('tids')), SUSET_CONTEXT);
    // Update notifications are strictly ordered but don't hold up transactions
    this.updates = this.updateSource.pipe(observeOn(asapScheduler));
    this.datasetLock = new LocalLock(id, dataset.location);
    this.log = getIdLogger(this.constructor, id, logLevel);
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
    const tail = await journal.tail();
    return tail.hash;
  }

  @SuSetDataset.checkNotClosed.rx
  undeliveredLocalOperations(): Observable<DeltaMessage> {
    return new Observable(subs => {
      this.journal.state()
        .then(journal => journal.lastDelivered())
        .then(last => last.next())
        .then(next => this.emitJournalFrom(next, subs, entry => !entry.remote))
        .catch(err => subs.error(err));
    });
  }

  private emitJournalFrom(entry: SuSetJournalEntry | undefined,
    subs: Observer<DeltaMessage>, filter: (entry: SuSetJournalEntry) => boolean) {
    if (subs.closed == null || !subs.closed) {
      if (entry != null) {
        if (this.dataset.closed) {
          subs.error(new MeldError('Clone has closed'));
        } else {
          if (filter(entry)) {
            const delta = new DeltaMessage(entry.time, entry.delta);
            delta.delivered
              .then(() => this.markDelivered(entry), err => subs.error(err));
            subs.next(delta);
          }
          entry.next().then(next => this.emitJournalFrom(next, subs, filter),
            err => subs.error(err));
        }
      } else {
        subs.complete();
      }
    }
  }

  /**
   * A revup requester will have just sent out any undelivered updates.
   * To ensure we have processed those (relying on the message layer ordering)
   * we always process a revup request in a transaction lock.
   */
  @SuSetDataset.checkNotClosed.async
  async operationsSince(time: TreeClock): Promise<Observable<DeltaMessage> | undefined> {
    return this.dataset.transact<Observable<DeltaMessage> | undefined>({
      id: 'suset-ops-since',
      prepare: async () => {
        const journal = await this.journal.state();
        const ticks = time.getTicks(journal.time);
        const found = ticks != null ? await journal.findEntry(ticks) : '';
        if (found) {
          return {
            value: new Observable(subs =>
              // Don't emit an entry if it's all less than the requested time
              this.emitJournalFrom(found, subs, entry => time.anyLt(entry.time, 'includeIds')))
          };
        } else {
          return { value: undefined };
        }
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<DeltaMessage> {
    return this.dataset.transact<DeltaMessage>({
      id: uuid(), // New transaction ID
      prepare: async txc => {
        const [time, patch] = await prepare();
        txc.sw.next('find-tids');
        const deletedTriplesTids = await this.findTriplesTids(patch.oldQuads);
        txc.sw.next('new-tids');
        const delta = await newDelta({
          tid: txc.id,
          insert: patch.newQuads,
          // Delta has reifications of old quads, which we infer from found triple tids
          delete: this.reify(deletedTriplesTids)
        });
        // Include tid changes in final patch
        const tidPatch = (await this.newTriplesTid(delta.insert, delta.tid))
          .concat({ oldQuads: flatten(deletedTriplesTids.map(tripleTids => tripleTids.tids)) });
        // Include journaling in final patch
        const allTidsPatch = await this.newTid(delta.tid);
        txc.sw.next('journal');
        const { patch: journaling, entry } = await this.journal.nextEntry(delta, time);
        const deltaMsg = new DeltaMessage(time, delta.json);
        deltaMsg.delivered.then(() => this.markDelivered(entry));
        return {
          patch: this.transactionPatch(time, patch, allTidsPatch, tidPatch, journaling),
          value: deltaMsg
        };
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  async apply(msgData: JsonDelta, msgTime: TreeClock, localTime: TreeClock): Promise<void> {
    return this.dataset.transact({
      id: msgData.tid,
      prepare: async txc => {
        // Check we haven't seen this transaction before in the journal
        txc.sw.next('find-tids');
        if (!(await this.tidsGraph.find1<AllTids>({ '@id': 'qs:all', tid: [txc.id] }))) {
          this.log.debug(`Applying tid: ${txc.id}`);
          txc.sw.next('unreify');
          const delta = await asMeldDelta(msgData);
          const patch = new PatchQuads([], delta.insert.map(toDomainQuad));
          // The delta's delete contains reifications of deleted triples
          const tidPatch = await unreify(delta.delete)
            .reduce(async (tripleTidPatch, [triple, theirTids]) => {
              // For each unique deleted triple, subtract the claimed tids from the tids we have
              const ourTripleTids = await this.findTripleTids(tripleId(triple));
              const toRemove = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid.object.value));
              // If no tids are left, delete the triple in our graph
              if (toRemove.length == ourTripleTids.length)
                patch.oldQuads.push(toDomainQuad(triple));
              return (await tripleTidPatch).concat({ oldQuads: toRemove });
            }, this.newTriplesTid(delta.insert, delta.tid));
          // Include journaling in final patch
          const allTidsPatch = await this.newTid(delta.tid);
          txc.sw.next('journal');
          const { patch: journaling } = await this.journal.nextEntry(delta, localTime, msgTime);
          return {
            patch: this.transactionPatch(localTime, patch, allTidsPatch, tidPatch, journaling)
          };
        } else {
          this.log.debug(`Rejecting tid: ${txc.id} as duplicate`);
          // We don't have to save the new local clock time, nothing's happened
          return {};
        }
      }
    });
  }

  /**
   * Rolls up the given transaction details into a single patch and notifies
   * data observers. Mostly this method is just a type convenience for ensuring
   * everything needed for a transaction is present.
   * @param time the local time of the transaction
   * @param dataPatch the transaction data patch
   * @param allTidsPatch insertion to qs:all TIDs in TID graph
   * @param tripleTidPatch triple TID patch (inserts and deletes)
   * @param journaling transaction journaling patch
   */
  private transactionPatch(
    time: TreeClock,
    dataPatch: PatchQuads,
    allTidsPatch: PatchQuads,
    tripleTidPatch: PatchQuads,
    journaling: PatchQuads): Patch {
    // Notify the update (we don't have to wait for this)
    this.notifyUpdate(dataPatch, time);
    return dataPatch.concat(allTidsPatch).concat(tripleTidPatch).concat(journaling);
  }

  private async notifyUpdate(patch: PatchQuads, time: TreeClock) {
    this.updateSource.next({
      '@ticks': time.ticks,
      '@delete': await this.toSubjects(patch.oldQuads),
      '@insert': await this.toSubjects(patch.newQuads)
    });
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
    return this.tidsGraph.insert(triples.map(triple =>
      ({ '@id': tripleId(triple), tid } as HashTid)));
  }

  private newTripleTids(triple: Triple, tids: UUID[]): Promise<PatchQuads> {
    const theTripleId = tripleId(triple);
    return this.tidsGraph.insert(tids.map(tid =>
      ({ '@id': theTripleId, tid } as HashTid)));
  }

  private async findTriplesTids(quads: Triple[]): Promise<{ triple: Triple, tids: Quad[] }[]> {
    return from(quads).pipe(
      flatMap(async quad => ({
        triple: quad,
        tids: await this.findTripleTids(tripleId(quad))
      })),
      toArray()).toPromise();
  }

  private findTripleTids(tripleId: string): Promise<Quad[]> {
    return this.tidsGraph.findQuads({ '@id': tripleId } as Partial<HashTid>);
  }

  private reify(triplesTids: { triple: Triple, tids: Quad[] }[]): Triple[] {
    return flatten(triplesTids.map(tripleTids =>
      reify(tripleTids.triple, tripleTids.tids.map(tidQuad => tidQuad.object.value))));
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

    const tidsApplied = snapshot.tids.pipe(flatMap(
      // Each batch of TIDs goes in happily as an array
      tids => this.dataset.transact({
        id: 'snapshot-tids-batch',
        prepare: async () => ({ patch: await this.newTid(tids) })
      }))).toPromise();

    const quadsApplied = snapshot.quads.pipe(
      // For each batch of reified quads with TIDs, first unreify
      flatMap(async batch => this.dataset.transact({
        id: 'snapshot-batch',
        prepare: async () => ({
          patch: await from(unreify(batch)).pipe(
            // For each triple in the batch, insert the TIDs into the tids graph
            flatMap(async ([triple, tids]) => (await this.newTripleTids(triple, tids))
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
              flatMap(async (batch) => this.reify(await this.findTriplesTids(batch))),
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

  private async markDelivered(entry: SuSetJournalEntry) {
    this.dataset.transact({
      id: 'delta-delivery',
      prepare: async () => ({ patch: await entry.markDelivered() })
    });
  }
}

function tripleId(triple: Triple): string {
  return toPrefixedId('thash', hashTriple(triple).encode());
}
