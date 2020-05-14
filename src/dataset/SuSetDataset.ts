import { MeldDelta, MeldJournalEntry, JsonDelta, Snapshot, UUID } from '../m-ld';
import { Quad, Triple } from 'rdf-js';
import { namedNode, defaultGraph } from '@rdfjs/data-model';
import { TreeClock } from '../clocks';
import { Hash } from '../hash';
import { Context, Subject, Group, DeleteInsert } from '../m-ld/jsonrql';
import { Dataset, PatchQuads, Patch } from '.';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph, toGroup } from './JrqlGraph';
import { JsonDeltaBagBlock, newDelta, asMeldDelta, toTimeString, fromTimeString, reify, unreify, hashTriple } from '../m-ld/MeldJson';
import { Observable, from, Subject as Source, asapScheduler, Observer } from 'rxjs';
import { toArray, bufferCount, flatMap, reduce, observeOn } from 'rxjs/operators';
import { flatten, Future, tapComplete, getIdLogger } from '../util';
import { generate as uuid } from 'short-uuid';
import { LogLevelDesc, Logger } from 'loglevel';
import { notClosed } from '../m-ld/MeldError';

const TIDS_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/',
  hash: 'qs:hash/', // Namespace for triple hashes
  tid: 'qs:#tid' // Property of a triple hash
};

interface HashTid extends Subject {
  '@id': Iri,
  tid: UUID // Transaction ID
}

const CONTROL_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/',
  tail: { '@id': 'qs:#tail', '@type': '@id' }, // Property of the journal
  lastDelivered: { '@id': 'qs:#lastDelivered', '@type': '@id' }, // Property of the journal
  entry: 'qs:journal/entry/', // Namespace for journal entries
  tid: 'qs:#tid', // Property of a journal entry
  hash: 'qs:#hash', // Property of a journal entry
  delta: 'qs:#delta', // Property of a journal entry
  remote: 'qs:#remote', // Property of a journal entry
  time: 'qs:#time', // Property of journal AND a journal entry
  ticks: 'qs:#ticks', // Property of a journal entry
  next: { '@id': 'qs:#next', '@type': '@id' } // Property of a journal entry
};

interface Journal extends Subject {
  '@id': 'qs:journal', // Singleton object
  tail: JournalEntry['@id'],
  lastDelivered: JournalEntry['@id'],
  time: string // JSON-encoded TreeClock (the local clock)
}

/**
 * A journal entry includes a transaction Id
 */
interface JournalEntry extends HashTid {
  hash: string, // Encoded Hash
  delta: string, // JSON-encoded JsonDelta
  remote: boolean,
  time: string, // JSON-encoded TreeClock (the remote clock)
  ticks: number, // Local clock ticks on delivery
  next?: JournalEntry['@id']
}

function safeTime(je: Journal | JournalEntry) {
  return fromTimeString(je.time) as TreeClock; // Safe after init
}

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant deltas.
 */
export class SuSetDataset extends JrqlGraph {
  private static notClosed = notClosed((d: SuSetDataset) => d.dataset.closed);
  
  private readonly controlGraph: JrqlGraph;
  private readonly tidsGraph: JrqlGraph;
  private readonly updateSource: Source<DeleteInsert<Group>> = new Source;
  readonly updates: Observable<DeleteInsert<Group>>
  private readonly log: Logger;

  constructor(
    private readonly dataset: Dataset, logLevel: LogLevelDesc = 'info') {
    super(dataset.graph());
    // Named graph for control quads e.g. Journal
    this.controlGraph = new JrqlGraph(
      dataset.graph(namedNode(CONTROL_CONTEXT.qs + 'control')), CONTROL_CONTEXT);
    this.tidsGraph = new JrqlGraph(
      dataset.graph(namedNode(CONTROL_CONTEXT.qs + 'tids')), TIDS_CONTEXT);
    // Update notifications are strictly ordered but don't hold up transactions
    this.updates = this.updateSource.pipe(observeOn(asapScheduler));
    this.log = getIdLogger(this.constructor, this.id, logLevel);
  }

  get id(): string {
    return this.dataset.id;
  }

  @SuSetDataset.notClosed.async
  async initialise() {
    if (!await this.controlGraph.describe1('qs:journal'))
      return this.dataset.transact(() => this.reset(Hash.random()));
  }

  @SuSetDataset.notClosed.async
  async close(err?: any) {
    if (err) {
      this.log.warn('Shutting down due to', err);
      this.updateSource.error(err);
    } else {
      this.log.info('Shutting down normally');
      this.updateSource.complete();
    }
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
      ticks: localTime?.getTicks?.()
    } as Partial<JournalEntry>]);
    // Delete matches everything in all graphs
    return { oldQuads: {}, newQuads: insert.newQuads };
  }

  @SuSetDataset.notClosed.async
  async loadClock(): Promise<TreeClock | null> {
    const journal = await this.loadJournal();
    return fromTimeString(journal.time);
  }

  @SuSetDataset.notClosed.async
  async saveClock(localTime: TreeClock, newClone?: boolean): Promise<void> {
    return this.dataset.transact(async () =>
      this.patchClock(await this.loadJournal(), localTime, newClone));
  }

  /**
   * @return the last hash seen in the journal.
   */
  @SuSetDataset.notClosed.async
  async lastHash(): Promise<Hash> {
    const [, tail] = await this.journalTail();
    return Hash.decode(tail.hash);
  }

  private async journalTail(): Promise<[Journal, JournalEntry]> {
    const journal = await this.loadJournal();
    if (!journal?.tail)
      throw new Error('Journal has no tail');
    return [journal, await this.controlGraph.describe1(journal.tail) as JournalEntry];
  }

  @SuSetDataset.notClosed.rx
  undeliveredLocalOperations(): Observable<MeldJournalEntry> {
    return new Observable(subs => {
      this.loadJournal().then(async (journal) => {
        const last = await this.controlGraph.describe1(journal.lastDelivered) as JournalEntry;
        await this.emitJournalFrom(last.next, subs, entry => !entry.remote);
      }).catch(err => subs.error(err));
    });
  }

  @SuSetDataset.notClosed.async // Used here for private method to end the recursion
  private async emitJournalFrom(entryId: JournalEntry['@id'] | undefined,
    subs: Observer<MeldJournalEntry>, filter: (entry: JournalEntry) => boolean) {
    if (subs.closed == null || !subs.closed) {
      if (entryId != null) {
        const entry = await this.controlGraph.describe1(entryId) as JournalEntry;
        if (filter(entry)) {
          subs.next(new MeldJournalEntry(
            safeTime(entry), JSON.parse(entry.delta), () => this.markDelivered(entry['@id'])));
        }
        await this.emitJournalFrom(entry.next, subs, filter);
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
  @SuSetDataset.notClosed.async
  async operationsSince(time: TreeClock): Promise<Observable<MeldJournalEntry> | undefined> {
    return new Promise(async (resolve, reject) => {
      this.dataset.transact(async () => {
        const journal = await this.loadJournal();
        const findTime = time.getTicks(safeTime(journal));
        const found = findTime != null ? await this.controlGraph.find1(
          <Partial<JournalEntry>>{ ticks: findTime }) : '';
        if (found) {
          resolve(new Observable(subs => {
            // Don't emit an entry if it's all less than the requested time (based on remote ID)
            this.emitJournalFrom(found, subs, entry => time.anyLt(safeTime(entry), 'includeIds'))
              .catch(err => subs.error(err));
          }));
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
        ticks: localTime.getTicks()
      } as Partial<JournalEntry>);
    }
    return await this.controlGraph.write(update);
  }

  private async loadJournal(): Promise<Journal> {
    return await this.controlGraph.describe1('qs:journal') as Journal;
  }

  @SuSetDataset.notClosed.async
  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<MeldJournalEntry> {
    return this.dataset.transact<MeldJournalEntry>(async () => {
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
      const [journaling, entry] = await this.journal(delta, time);
      this.notifyUpdate(patch);
      return [patch.concat(tidPatch).concat(journaling), entry];
    });
  }

  @SuSetDataset.notClosed.async
  async apply(msgData: JsonDelta, msgTime: TreeClock, localTime: () => TreeClock): Promise<void> {
    return this.dataset.transact(async () => {
      // Check we haven't seen this transaction before in the journal
      if (!(await this.controlGraph.find1({ tid: msgData.tid } as Partial<JournalEntry>))) {
        this.log.debug(`Applying tid: ${msgData.tid}`);
        const delta = await asMeldDelta(msgData);
        const patch = new PatchQuads([], delta.insert);
        // The delta's delete contains reifications of deleted triples
        const tripleTidPatch = await unreify(delta.delete)
          .reduce(async (tripleTidPatch, [triple, theirTids]) => {
            // For each unique deleted triple, subtract the claimed tids from the tids we have
            const ourTripleTids = await this.findTripleTids(tripleId(triple));
            const toRemove = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid.object.value));
            // If no tids are left, delete the triple in our graph
            if (toRemove.length == ourTripleTids.length)
              patch.oldQuads.push({ ...triple, graph: defaultGraph() });
            return (await tripleTidPatch).concat({ oldQuads: toRemove });
          }, this.newTriplesTid(delta.insert, delta.tid));
        // Include journaling in final patch
        const [journaling,] = await this.journal(delta, localTime(), msgTime);
        this.notifyUpdate(patch);
        return patch.concat(tripleTidPatch).concat(journaling);
      } else {
        this.log.debug(`Rejecting tid: ${msgData.tid} as duplicate`);
      }
    });
  }

  private async notifyUpdate(patch: PatchQuads) {
    this.updateSource.next({
      '@delete': await toGroup(patch.oldQuads, this.defaultContext),
      '@insert': await toGroup(patch.newQuads, this.defaultContext)
    });
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

  private async findTriplesTids(quads: Quad[]): Promise<{ triple: Triple, tids: Triple[] }[]> {
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

  private reify(triplesTids: { triple: Triple, tids: Triple[] }[]): Triple[] {
    return flatten(triplesTids.map(tripleTids =>
      reify(tripleTids.triple, tripleTids.tids.map(tidQuad => tidQuad.object.value))));
  }

  /**
   * Applies a snapshot to this dataset.
   * Caution: uses multiple transactions, so the world must be held up by the caller.
   * @param data a hot observable of snapshot data in batches of quads
   * @param lastHash the last hash of the snapshot dataset
   * @param lastTime the last time of the snapshot dataset (not the local time of the provider)
   * @param localTime the time of the local process, to be saved
   */
  @SuSetDataset.notClosed.async
  async applySnapshot(data: Observable<Quad[]>,
    lastHash: Hash, lastTime: TreeClock, localTime: TreeClock) {
    // First reset the dataset with the given parameters
    const haveReset = this.dataset.transact(() => this.reset(lastHash, lastTime, localTime));
    // For each batch of reified quads with TIDs, first unreify
    const snapshotApplied = data.pipe(
      flatMap(async batch => this.dataset.transact(async () => from(unreify(batch)).pipe(
        // For each triple in the batch, insert the TIDs into the tids graph
        flatMap(async ([triple, tids]) => (await this.newTripleTids(triple, tids))
          // And include the triple itself
          .concat({ newQuads: [{ ...triple, graph: defaultGraph() }] })),
        // Concat all of the resultant batch patches together
        reduce((batchPatch, entryPatch) => batchPatch.concat(entryPatch)))
        .toPromise()))).toPromise();
    return Promise.all([haveReset, snapshotApplied]);
  }

  /**
   * Takes a snapshot of data, including transaction IDs.
   * This requires a consistent view, so a transaction lock is taken until all data has been emitted.
   * To avoid holding up the world, buffer the data.
   */
  @SuSetDataset.notClosed.async
  async takeSnapshot(): Promise<Omit<Snapshot, 'updates'>> {
    return new Promise((resolve, reject) => {
      this.dataset.transact(async () => {
        const dataEmitted = new Future;
        const [, tail] = await this.journalTail();
        resolve({
          time: safeTime(tail),
          lastHash: Hash.decode(tail.hash),
          data: this.graph.match().pipe(
            bufferCount(10), // TODO batch size config
            flatMap(async (batch) => this.reify(await this.findTriplesTids(batch))),
            tapComplete(dataEmitted))
        });
        await dataEmitted; // If this rejects, data will error
      }).catch(reject);
    });
  }

  private async journal(delta: MeldDelta, localTime: TreeClock, remoteTime?: TreeClock):
    Promise<[PatchQuads, MeldJournalEntry]> {
    const [journal, oldTail] = await this.journalTail();
    const block = new JsonDeltaBagBlock(Hash.decode(oldTail.hash)).next(delta.json);
    const entryId = toPrefixedId('entry', block.id.encode());
    const delivered = () => this.markDelivered(entryId);
    return [
      (await this.controlGraph.write({
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
            ticks: localTime.getTicks(),
            delta: JSON.stringify(delta.json)
          } as JournalEntry
        ]
      })).concat(await this.patchClock(journal, localTime)),
      new MeldJournalEntry(localTime, delta.json, delivered)
    ];
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

function tripleId(quad: Quad): string {
  return toPrefixedId('hash', hashTriple(quad).encode());
}

function toPrefixedId(prefix: string, ...path: string[]) {
  return `${prefix}:${path.map(encodeURIComponent).join('/')}`;
}