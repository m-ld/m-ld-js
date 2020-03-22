import { MeldDelta, MeldJournalEntry, JsonDelta, Snapshot, DeltaMessage, UUID } from '../m-ld';
import { Quad, Triple } from 'rdf-js';
import { namedNode, defaultGraph } from '@rdfjs/data-model';
import { TreeClock } from '../clocks';
import { Hash } from '../hash';
import { Context, Subject, Group, DeleteInsert } from '../m-ld/jsonrql';
import { Dataset, PatchQuads, Patch } from '.';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph, toGroup } from './JrqlGraph';
import { JsonDeltaBagBlock, newDelta, asMeldDelta, toTimeString, fromTimeString, reify, unreify } from '../m-ld/JsonDelta';
import { Observable, Subscriber, from, Subject as Source } from 'rxjs';
import { toArray, bufferCount, flatMap } from 'rxjs/operators';
import { flatten } from '../util';
import { generate as uuid } from 'short-uuid';

const CONTROL_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/',
  tail: { '@id': 'qs:#tail', '@type': '@id' }, // Property of the journal
  lastDelivered: { '@id': 'qs:#lastDelivered', '@type': '@id' }, // Property of the journal
  entry: 'qs:journal/entry/', // Namespace for journal entries
  hash: 'qs:#hash', // Property of a journal entry
  delta: 'qs:#delta', // Property of a journal entry
  remote: 'qs:#remote', // Property of a journal entry
  time: 'qs:#time', // Property of journal AND a journal entry
  next: { '@id': 'qs:#next', '@type': '@id' } // Property of a journal entry
};

interface Journal extends Subject {
  '@id': 'qs:journal', // Singleton object
  tail: JournalEntry['@id'],
  lastDelivered: JournalEntry['@id'],
  time: string // JSON-encoded TreeClock
}

interface JournalEntry extends Subject {
  '@id': Iri,
  hash: string, // Encoded Hash
  delta: string, // JSON-encoded JsonDelta
  remote: boolean,
  time: string, // JSON-encoded TreeClock
  next?: JournalEntry['@id']
}

const TIDS_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/',
  hash: 'qs:hash/', // Namespace for triple hashes
  tid: 'qs:#tid' // Property of a triple hashes
};

interface HashTid extends Subject {
  '@id': Iri,
  tid: UUID // Transaction ID
}

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant deltas.
 */
export class SuSetDataset extends JrqlGraph {
  private readonly controlGraph: JrqlGraph;
  private readonly tidsGraph: JrqlGraph;
  private readonly updateSource: Source<DeleteInsert<Group>> = new Source;

  constructor(
    private readonly dataset: Dataset) {
    super(dataset.graph());
    // Named graph for control quads e.g. Journal
    this.controlGraph = new JrqlGraph(
      dataset.graph(namedNode(CONTROL_CONTEXT.qs + 'control')), CONTROL_CONTEXT);
    this.tidsGraph = new JrqlGraph(
      dataset.graph(namedNode(CONTROL_CONTEXT.qs + 'tids')), TIDS_CONTEXT);
  }

  get id(): string {
    return this.dataset.id;
  }

  get updates(): Observable<DeleteInsert<Group>> {
    return this.updateSource;
  }

  async initialise() {
    if (!await this.controlGraph.describe1('qs:journal'))
      return this.dataset.transact(() => this.reset(Hash.random()));
  }

  close(err?: any) {
    console.log('Shutting down dataset ' + err ? 'due to ' + err : 'normally');
    if (err)
      this.updateSource.error(err);
    else
      this.updateSource.complete();
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
      time: toTimeString(startingTime)
    } as Partial<JournalEntry>]);
    // Delete matches everything in all graphs
    return { oldQuads: {}, newQuads: insert.newQuads };
  }

  async loadClock(): Promise<TreeClock | null> {
    const journal = await this.loadJournal();
    return fromTimeString(journal.time);
  }

  async saveClock(time: TreeClock, newClone?: boolean): Promise<void> {
    return this.dataset.transact(async () =>
      this.patchClock(await this.loadJournal(), time, newClone));
  }

  /**
   * @return the last hash seen in the journal.
   */
  async lastHash(): Promise<Hash> {
    const [, tail] = await this.journalTail();
    return Hash.decode(tail.hash);
  }

  private async journalTail(): Promise<[Journal, JournalEntry]> {
    const journal = await this.loadJournal();
    return [journal, await this.controlGraph.describe1(journal.tail) as JournalEntry];
  }

  unsentLocalOperations(): Observable<MeldJournalEntry> {
    return new Observable(subs => {
      this.loadJournal().then(async (journal) => {
        const last = await this.controlGraph.describe1(journal.lastDelivered) as JournalEntry;
        await this.emitJournalAfter(last, subs);
      });
    });
  }

  private async emitJournalAfter(entry: JournalEntry, subs: Subscriber<MeldJournalEntry>) {
    if (entry.next) {
      entry = await this.controlGraph.describe1(entry.next) as JournalEntry;
      const delivered = () => this.markDelivered(entry['@id']);
      const time = TreeClock.fromJson(entry.time) as TreeClock; // Never null
      subs.next({ time, data: JSON.parse(entry.delta), delivered });
      await this.emitJournalAfter(entry, subs);
    } else {
      subs.complete();
    }
  }

  async operationsSince(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined> {
    const found = await this.controlGraph.find({ hash: lastHash.encode() });
    if (found.size) {
      const entry = await this.controlGraph.describe1(found.values().next().value) as Subject;
      return new Observable(subs => {
        this.emitJournalAfter(entry as JournalEntry, subs);
      });
    }
  }

  private async patchClock(journal: Journal, time: TreeClock, newClone?: boolean): Promise<PatchQuads> {
    const encodedTime = toTimeString(time);
    const update = {
      '@delete': { '@id': 'qs:journal', time: journal.time } as Partial<Journal>,
      '@insert': [{ '@id': 'qs:journal', time: encodedTime } as Partial<Journal>] as Subject[]
    };
    if (newClone) {
      // For a new clone, the journal's dummy tail does not already have a timestamp
      update['@insert'].push({ '@id': journal.tail, time: encodedTime } as Partial<JournalEntry>);
    }
    return await this.controlGraph.write(update);
  }

  private async loadJournal(): Promise<Journal> {
    return await this.controlGraph.describe1('qs:journal') as Journal;
  }

  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<MeldJournalEntry> {
    return this.dataset.transact(async () => {
      const [time, patch] = await prepare();
      const deletedTripleTids = await this.findTriplesTids(patch.oldQuads);
      const delta = await newDelta({
        tid: uuid(),
        insert: patch.newQuads,
        // Delta has reifications of old quads, which we infer from found triple tids
        delete: flatten(patch.oldQuads.map((quad, i) =>
          flatten(deletedTripleTids[i].map(tidQuad => reify(quad, tidQuad.object.value)))))
      });
      // Include tid changes in final patch
      const tidPatch = (await this.newTripleTids(delta.insert, delta.tid))
        .concat({ oldQuads: flatten(deletedTripleTids) });
      // Include journaling in final patch
      const [journaling, entry] = await this.journal(delta, time, false);
      this.postUpdate(patch);
      return [patch.concat(tidPatch).concat(journaling), entry] as [Patch, MeldJournalEntry];
    });
  }

  async apply(msg: JsonDelta, time: TreeClock): Promise<void> {
    return this.dataset.transact(async () => {
      // Check we haven't seen this transaction before in the journal
      if (!(await this.controlGraph.find({ tid: msg.tid })).size) {
        const delta = await asMeldDelta(msg);
        const patch = new PatchQuads([], delta.insert);
        // The delta's delete contains reifications of deleted triples
        const tripleTidPatch = await Object.entries(deletedTidsByTripleId(delta))
          .reduce(async (tripleTidPatch, [tripleId, [triple, theirTids]]) => {
            // For each unique deleted triple, subtract the claimed tids from the tids we have
            const ourTripleTids = await this.findTripleTids(tripleId);
            const oldQuads = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid.object.value));
            // If no tids are left, delete the triple in our graph
            if (oldQuads.length == ourTripleTids.length)
              patch.oldQuads.push({ ...triple, graph: defaultGraph() });
            return (await tripleTidPatch).concat({ oldQuads });
          }, this.newTripleTids(delta.insert, delta.tid));
        // Include journaling in final patch
        const [journaling,] = await this.journal(delta, time, true);
        this.postUpdate(patch);
        return patch.concat(tripleTidPatch).concat(journaling);
      }
    });
  }

  private async postUpdate(patch: PatchQuads) {
    this.updateSource.next({
      '@delete': await toGroup(patch.oldQuads, this.defaultContext),
      '@insert': await toGroup(patch.newQuads, this.defaultContext)
    });
  }

  private newTripleTids(quads: Quad[], tid: UUID): Promise<PatchQuads> {
    return this.tidsGraph.insert(quads.map(quad =>
      ({ '@id': tripleId(quad), tid } as HashTid)));
  }

  private async findTriplesTids(quads: Quad[]): Promise<Quad[][]> {
    return from(quads).pipe(
      flatMap(quad => this.findTripleTids(tripleId(quad))),
      toArray()).toPromise();
  }

  private findTripleTids(tripleId: string): Promise<Quad[]> {
    return this.tidsGraph.findQuads({ '@id': tripleId } as Partial<HashTid>);
  }

  async applySnapshot(data: Observable<Triple[]>,
    lastHash: Hash, lastTime: TreeClock, localTime: TreeClock): Promise<void> {
    return this.dataset.transact(async () => {
      const reset = await this.reset(lastHash, lastTime, localTime);
      return {
        oldQuads: reset.oldQuads,
        newQuads: reset.newQuads.concat(flatten(await data.pipe(toArray()).toPromise()))
      };
    });
  }

  async takeSnapshot(): Promise<Omit<Snapshot, 'updates'>> {
    // Snapshot requires a consistent view, so use a transaction lock
    // until data has been emitted. But, resolve as soon as the observables are prepared.
    return new Promise((resolve, reject) => {
      this.dataset.transact(async () => {
        const [, tail] = await this.journalTail();
        resolve({
          time: fromTimeString(tail.time) as TreeClock,
          lastHash: Hash.decode(tail.hash),
          data: this.graph.match().pipe(bufferCount(10)) // TODO buffer config
        });
      }).catch(reject);
    });
  }

  private async journal(delta: MeldDelta, time: TreeClock, remote: boolean): Promise<[PatchQuads, MeldJournalEntry]> {
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
            '@id': entryId, remote,
            hash: block.id.encode(),
            tid: delta.tid,
            time: toTimeString(time),
            delta: JSON.stringify(delta.json)
          } as JournalEntry
        ]
      })).concat(await this.patchClock(journal, time)),
      { time, data: delta.json, delivered }
    ];
  };

  private async markDelivered(entryId: Iri): Promise<PatchQuads> {
    const journal = await this.loadJournal();
    return await this.controlGraph.write({
      '@delete': { '@id': 'qs:journal', lastDelivered: journal.lastDelivered } as Partial<Journal>,
      '@insert': { '@id': 'qs:journal', lastDelivered: entryId } as Partial<Journal>
    });
  }
}

function deletedTidsByTripleId(delta: MeldDelta): { [tripleId: string]: [Triple, UUID[]] } {
  return unreify(delta.delete)
    .map(([triple, tid]) => [tripleId(triple), triple, tid] as [string, Triple, UUID])
    .reduce((tripleTids, [tripleId, triple, tid]) => {
      const [, tids] = tripleTids[tripleId] || [triple, []];
      tripleTids[tripleId] = [triple, tids.concat(tid)];
      return tripleTids;
    }, {} as {
      [tripleId: string]: [Triple, UUID[]];
    });
}

function tripleId(quad: Quad): string {
  return toPrefixedId('hash', hashTriple(quad).encode());
}

function hashTriple(triple: Triple): Hash {
  switch (triple.object.termType) {
    case 'Literal': return Hash.digest(
      triple.subject.value,
      triple.predicate.value,
      triple.object.termType,
      triple.object.value || '',
      triple.object.datatype.value || '',
      triple.object.language || '');
    default: return Hash.digest(
      triple.subject.value,
      triple.predicate.value,
      triple.object.termType,
      triple.object.value);
  }
}

function toPrefixedId(prefix: string, ...path: string[]) {
  return `${prefix}:${path.map(encodeURIComponent).join('/')}`;
}