import { MeldDelta, JsonDelta, Snapshot, UUID, MeldUpdate, DeltaMessage, Triple } from '../m-ld';
import { Quad } from 'rdf-js';
import { namedNode, defaultGraph } from '@rdfjs/data-model';
import { TreeClock } from '../clocks';
import { Hash } from '../hash';
import { Context, Subject } from './jrql-support';
import { Dataset, PatchQuads, Patch } from '.';
import { flatten as flatJsonLd } from 'jsonld';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph } from './JrqlGraph';
import { JsonDeltaBagBlock, newDelta, asMeldDelta, toTimeString, fromTimeString, reify, unreify, hashTriple, toDomainQuad } from '../m-ld/MeldJson';
import { Observable, from, Subject as Source, asapScheduler, Observer } from 'rxjs';
import { toArray, bufferCount, flatMap, reduce, observeOn, isEmpty, map } from 'rxjs/operators';
import { flatten, Future, tapComplete, getIdLogger, check, rdfToJson } from '../util';
import { generate as uuid } from 'short-uuid';
import { LogLevelDesc, Logger } from 'loglevel';
import { MeldError } from '../m-ld/MeldError';
import { LocalLock } from '../local';

const BASE_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/',
  tid: 'qs:#tid' // Property of triple hash and journal entry
}

const TIDS_CONTEXT: Context = {
  ...BASE_CONTEXT,
  hash: 'qs:hash/' // Namespace for triple hashes
};

interface HashTid extends Subject {
  '@id': Iri;
  tid: UUID; // Transaction ID
}

interface AllTids extends Subject {
  '@id': 'qs:all'; // Singleton object
  tid: UUID[];
}

const CONTROL_CONTEXT: Context = {
  ...BASE_CONTEXT,
  tail: { '@id': 'qs:#tail', '@type': '@id' }, // Property of the journal
  lastDelivered: { '@id': 'qs:#lastDelivered', '@type': '@id' }, // Property of the journal
  entry: 'qs:journal/entry/', // Namespace for journal entries
  hash: 'qs:#hash', // Property of a journal entry
  delta: 'qs:#delta', // Property of a journal entry
  remote: 'qs:#remote', // Property of a journal entry
  time: 'qs:#time', // Property of journal AND a journal entry
  ticks: 'qs:#ticks', // Property of a journal entry
  next: { '@id': 'qs:#next', '@type': '@id' } // Property of a journal entry
};

interface Journal extends Subject {
  '@id': 'qs:journal', // Singleton object
  tail: JournalEntry['@id'];
  lastDelivered: JournalEntry['@id'];
  time: string; // JSON-encoded TreeClock (the local clock)
}

/**
 * A journal entry includes a transaction Id
 */
interface JournalEntry extends HashTid {
  hash: string; // Encoded Hash
  delta: string; // JSON-encoded JsonDelta
  remote: boolean;
  time: string; // JSON-encoded TreeClock (the remote clock)
  ticks: number; // Local clock ticks on delivery
  next?: JournalEntry['@id'];
}

function safeTime(je: Journal | JournalEntry) {
  return fromTimeString(je.time) as TreeClock; // Safe after init
}

function qsName(name: string) {
  return namedNode(BASE_CONTEXT.qs + name);
}

type DatasetSnapshot = Omit<Snapshot, 'updates'>;

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant deltas.
 */
export class SuSetDataset extends JrqlGraph {
  private static checkNotClosed =
    check((d: SuSetDataset) => !d.dataset.closed, () => new MeldError('Clone has closed'));

  private readonly controlGraph: JrqlGraph;
  private readonly tidsGraph: JrqlGraph;
  private readonly updateSource: Source<MeldUpdate> = new Source;
  readonly updates: Observable<MeldUpdate>
  private readonly datasetLock: LocalLock;
  private readonly log: Logger;

  constructor(id: string,
    private readonly dataset: Dataset, logLevel: LogLevelDesc = 'info') {
    super(dataset.graph());
    // Named graph for control quads e.g. Journal
    this.controlGraph = new JrqlGraph(
      dataset.graph(qsName('control')), CONTROL_CONTEXT);
    this.tidsGraph = new JrqlGraph(
      dataset.graph(qsName('tids')), TIDS_CONTEXT);
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
    if (await this.controlGraph.describe('qs:journal').pipe(isEmpty()).toPromise())
      return this.dataset.transact(() => this.reset(Hash.random()));
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

  private async reset(startingHash: Hash,
    startingTime?: TreeClock, localTime?: TreeClock): Promise<Patch> {
    const encodedHash = startingHash.encode();
    const entryId = toPrefixedId('entry', encodedHash);
    const insert = await this.controlGraph.insert([{
      '@id': 'qs:journal',
      lastDelivered: entryId,
      time: toTimeString(localTime),
      tail: entryId,
    } as Journal, {
      '@id': entryId,
      hash: encodedHash,
      time: toTimeString(startingTime),
      ticks: localTime?.ticks
    } as Partial<JournalEntry>]);
    // Delete matches everything in all graphs
    return { oldQuads: {}, newQuads: insert.newQuads };
  }

  @SuSetDataset.checkNotClosed.async
  async loadClock(): Promise<TreeClock | null> {
    const journal = await this.loadJournal();
    return fromTimeString(journal.time);
  }

  @SuSetDataset.checkNotClosed.async
  async saveClock(localTime: TreeClock, newClone?: boolean): Promise<void> {
    return this.dataset.transact(async () =>
      this.patchClock(await this.loadJournal(), localTime, newClone));
  }

  /**
   * @return the last hash seen in the journal.
   */
  @SuSetDataset.checkNotClosed.async
  async lastHash(): Promise<Hash> {
    const [, tail] = await this.journalTail();
    return Hash.decode(tail.hash);
  }

  private async journalTail(): Promise<[Journal, JournalEntry]> {
    const journal = await this.loadJournal();
    if (!journal?.tail)
      throw new Error('Journal has no tail');
    return [journal, await this.controlGraph.describe1<JournalEntry>(journal.tail)];
  }

  @SuSetDataset.checkNotClosed.rx
  undeliveredLocalOperations(): Observable<DeltaMessage> {
    return new Observable(subs => {
      this.loadJournal()
        .then(journal => this.controlGraph.describe1<JournalEntry>(journal.lastDelivered))
        .then(last => this.emitJournalFrom(last.next, subs, entry => !entry.remote))
        .catch(err => subs.error(err));
    });
  }

  private emitJournalFrom(entryId: JournalEntry['@id'] | undefined,
    subs: Observer<DeltaMessage>, filter: (entry: JournalEntry) => boolean) {
    if (subs.closed == null || !subs.closed) {
      if (entryId != null) {
        if (this.dataset.closed) {
          subs.error(new MeldError('Clone has closed'));
        } else {
          this.controlGraph.describe1<JournalEntry>(entryId).then(entry => {
            if (filter(entry)) {
              const delta = new DeltaMessage(safeTime(entry), JSON.parse(entry.delta));
              delta.delivered.then(() => this.markDelivered(entry['@id']));
              subs.next(delta);
            }
            this.emitJournalFrom(entry.next, subs, filter);
          }).catch(err => subs.error(err));
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
    return new Promise(async (resolve, reject) => {
      this.dataset.transact(async () => {
        const journal = await this.loadJournal();
        const findTime = time.getTicks(safeTime(journal));
        const found = findTime != null ?
          await this.controlGraph.find1<JournalEntry>({ ticks: findTime }) : '';
        if (found) {
          // Don't emit an entry if it's all less than the requested time (based on remote ID)
          resolve(new Observable(subs =>
            this.emitJournalFrom(found, subs, entry => time.anyLt(safeTime(entry), 'includeIds'))));
        } else {
          resolve(undefined);
        }
      }).catch(reject);
    });
  }

  private async patchClock(journal: Journal, localTime: TreeClock, newClone?: boolean): Promise<PatchQuads> {
    const encodedTime = toTimeString(localTime);
    const update = {
      '@delete': { '@id': 'qs:journal', time: journal.time } as Partial<Journal>,
      '@insert': [{ '@id': 'qs:journal', time: encodedTime } as Partial<Journal>] as Subject[]
    };
    if (newClone) {
      // For a new clone, the journal's dummy tail does not already have a timestamp
      update['@insert'].push({
        '@id': journal.tail,
        time: encodedTime,
        ticks: localTime.ticks
      } as Partial<JournalEntry>);
    }
    return await this.controlGraph.write(update);
  }

  private async loadJournal(): Promise<Journal> {
    return await this.controlGraph.describe1<Journal>('qs:journal');
  }

  @SuSetDataset.checkNotClosed.async
  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<DeltaMessage> {
    return this.dataset.transact<DeltaMessage>(async () => {
      const [time, patch] = await prepare();
      const deletedTriplesTids = await this.findTriplesTids(patch.oldQuads);
      const delta = await newDelta({
        tid: uuid(),
        insert: patch.newQuads,
        // Delta has reifications of old quads, which we infer from found triple tids
        delete: this.reify(deletedTriplesTids)
      });
      // Include tid changes in final patch
      const tidPatch = (await this.newTriplesTid(delta.insert, delta.tid))
        .concat({ oldQuads: flatten(deletedTriplesTids.map(tripleTids => tripleTids.tids)) });
      // Include journaling in final patch
      const allTidsPatch = await this.newTid(delta.tid);
      const [journaling, entry] = await this.journal(delta, time);
      return [this.transactionPatch(time, patch, allTidsPatch, tidPatch, journaling), entry];
    });
  }

  @SuSetDataset.checkNotClosed.async
  async apply(msgData: JsonDelta, msgTime: TreeClock, localTime: TreeClock): Promise<void> {
    return this.dataset.transact(async () => {
      // Check we haven't seen this transaction before in the journal
      if (!(await this.tidsGraph.find1<AllTids>({ '@id': 'qs:all', tid: [msgData.tid] }))) {
        this.log.debug(`Applying tid: ${msgData.tid}`);
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
        const journaling = await this.journal(delta, localTime, msgTime);
        return this.transactionPatch(localTime, patch, allTidsPatch, tidPatch, journaling);
      } else {
        this.log.debug(`Rejecting tid: ${msgData.tid} as duplicate`);
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
    const dataReset = this.dataset.transact(() =>
      this.reset(snapshot.lastHash, snapshot.lastTime, localTime));

    const tidsApplied = snapshot.tids.pipe(flatMap(
      // Each batch of TIDs goes in happily as an array
      tids => this.dataset.transact(() => this.newTid(tids))))
      .toPromise();

    const quadsApplied = snapshot.quads.pipe(
      // For each batch of reified quads with TIDs, first unreify
      flatMap(async batch => this.dataset.transact(async () => from(unreify(batch)).pipe(
        // For each triple in the batch, insert the TIDs into the tids graph
        flatMap(async ([triple, tids]) => (await this.newTripleTids(triple, tids))
          // And include the triple itself
          .concat({ newQuads: [toDomainQuad(triple)] })),
        // Concat all of the resultant batch patches together
        reduce((batchPatch, entryPatch) => batchPatch.concat(entryPatch)))
        .toPromise()))).toPromise();
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
      this.dataset.transact(async () => {
        const dataEmitted = new Future;
        const [, tail] = await this.journalTail();
        resolve({
          lastTime: safeTime(tail),
          lastHash: Hash.decode(tail.hash),
          quads: this.graph.match().pipe(
            bufferCount(10), // TODO batch size config
            flatMap(async (batch) => this.reify(await this.findTriplesTids(batch))),
            tapComplete(dataEmitted)),
          tids: this.tidsGraph.graph.match(qsName('all'), qsName('#tid')).pipe(
            map(tid => tid.object.value), bufferCount(10)) // TODO batch size config
        });
        await dataEmitted; // If this rejects, data will error
      }).catch(reject);
    });
  }

  private async journal(delta: MeldDelta, localTime: TreeClock): Promise<[PatchQuads, DeltaMessage]>
  private async journal(delta: MeldDelta, localTime: TreeClock, remoteTime: TreeClock): Promise<PatchQuads>
  private async journal(delta: MeldDelta, localTime: TreeClock, remoteTime?: TreeClock):
    Promise<PatchQuads | [PatchQuads, DeltaMessage]> {
    const [journal, oldTail] = await this.journalTail();
    const block = new JsonDeltaBagBlock(Hash.decode(oldTail.hash)).next(delta.json);
    const entryId = toPrefixedId('entry', block.id.encode());
    const patch = (await this.controlGraph.write({
      '@delete': { '@id': 'qs:journal', tail: journal.tail } as Partial<Journal>,
      '@insert': [
        { '@id': 'qs:journal', tail: entryId } as Partial<Journal>,
        { '@id': journal.tail, next: entryId } as Partial<JournalEntry>,
        {
          '@id': entryId,
          remote: remoteTime != null,
          hash: block.id.encode(),
          tid: delta.tid,
          time: toTimeString(remoteTime ?? localTime),
          ticks: localTime.ticks,
          delta: JSON.stringify(delta.json)
        } as JournalEntry
      ]
    })).concat(await this.patchClock(journal, localTime));
    if (remoteTime == null) { // This is a local transaction
      // Create a DeltaMessage to be marked delivered later
      const deltaMsg = new DeltaMessage(localTime, delta.json);
      deltaMsg.delivered.then(() => this.markDelivered(entryId));
      return [patch, deltaMsg];
    } else { // This is a remote transaction
      return patch;
    }
  };

  private async markDelivered(entryId: Iri) {
    this.dataset.transact(async () => {
      const journal = await this.loadJournal();
      return this.controlGraph.write({
        '@delete': { '@id': 'qs:journal', lastDelivered: journal.lastDelivered } as Partial<Journal>,
        '@insert': { '@id': 'qs:journal', lastDelivered: entryId } as Partial<Journal>
      });
    });
  }
}

function tripleId(triple: Triple): string {
  return toPrefixedId('hash', hashTriple(triple).encode());
}

function toPrefixedId(prefix: string, ...path: string[]) {
  return `${prefix}:${path.map(encodeURIComponent).join('/')}`;
}