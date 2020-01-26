import { MeldDelta, MeldJournalEntry, JsonDelta, Snapshot, DeltaMessage } from './meld';
import { Quad, Triple } from 'rdf-js';
import { namedNode } from '@rdfjs/data-model';
import { TreeClock } from './clocks';
import { Hash } from './hash';
import { Context, Subject } from './jsonrql';
import { Dataset, PatchQuads, Patch } from './Dataset';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph } from './JrqlGraph';
import { reify, JsonDeltaBagBlock, newDelta, asMeldDelta } from './JsonDelta';
import { Observable, Subscriber } from 'rxjs';
import { toArray, bufferCount } from 'rxjs/operators';
import { flatten } from './util';
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

interface Journal {
  '@id': 'qs:journal', // Singleton object
  tail: JournalEntry['@id'],
  lastDelivered: JournalEntry['@id'],
  time: string // JSON-encoded TreeClock
}

interface JournalEntry {
  '@id': Iri,
  hash: string, // Encoded Hash
  delta: string, // JSON-encoded JsonDelta
  remote: boolean,
  time: string, // JSON-encoded TreeClock
  next?: JournalEntry['@id']
}

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every every transaction and creates m-ld compliant deltas.
 */
export class SuSetDataset extends JrqlGraph {
  private readonly controlGraph: JrqlGraph;

  constructor(
    private readonly dataset: Dataset) {
    super(dataset.graph());
    // Named graph for control quads e.g. Journal
    this.controlGraph = new JrqlGraph(
      dataset.graph(namedNode(CONTROL_CONTEXT.qs + 'control')), CONTROL_CONTEXT);
  }

  async initialise() {
    if (!await this.controlGraph.describe('qs:journal'))
      return this.dataset.transact(() => this.reset(Hash.random()));
  }

  private async reset(startingHash: Hash,
    startingTime?: TreeClock, localTime?: TreeClock): Promise<Patch> {
    const encodedHash = startingHash.encode();
    const entryId = 'entry:' + encodedHash;
    const insert = await this.controlGraph.insert({
      '@id': 'qs:journal',
      lastDelivered: entryId,
      time: toTimeString(localTime),
      tail: {
        '@id': entryId,
        hash: encodedHash,
        time: toTimeString(startingTime)
      } as Partial<JournalEntry>
    } as Journal);
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
    return [journal, await this.controlGraph.describe(journal.tail) as JournalEntry];
  }

  unsentLocalOperations(): Observable<MeldJournalEntry> {
    return new Observable(subs => {
      this.loadJournal().then(async (journal) => {
        const last = await this.controlGraph.describe(journal.lastDelivered) as JournalEntry;
        await this.emitJournalAfter(last, subs);
        subs.complete();
      });
    });
  }

  private async emitJournalAfter(entry: JournalEntry, subs: Subscriber<MeldJournalEntry>) {
    if (entry.next) {
      entry = await this.controlGraph.describe(entry.next) as JournalEntry;
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
      const entry = this.controlGraph.describe(found.values().next().value) as Subject;
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
    return await this.controlGraph.describe('qs:journal') as Journal;
  }

  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<MeldJournalEntry> {
    return this.dataset.transact(async () => {
      const [time, patch] = await prepare();
      const delta = await newDelta({
        tid: uuid(),
        insert: patch.newQuads,
        // Find reifications of old quads
        delete: [/*TODO*/]
      });
      // Include reifications in final patch
      const reifications = {
        oldQuads: delta.delete,
        // Reified new quads
        newQuads: reify(delta.insert, delta.tid)
      };
      // Include journaling in final patch
      const [journaling, entry] = await this.journal(delta, time, false);
      return [patch.concat(reifications).concat(journaling), entry] as [Patch, MeldJournalEntry];
    });
  }

  async apply(msg: JsonDelta, time: TreeClock): Promise<void> {
    return this.dataset.transact(async () => {
      // Check we haven't seen this transaction before in the journal
      if (!(await this.controlGraph.find({ tid: msg.tid })).size) {
        const delta = await asMeldDelta(msg), patch = new PatchQuads([/*TODO*/], delta.insert);
        // Include reifications in final patch
        const reifications = {
          oldQuads: [] as Quad[], // TODO
          newQuads: reify(delta.insert, delta.tid)
        }
        // Include journaling in final patch
        const [journaling,] = await this.journal(delta, time, true);
        return patch.concat(reifications).concat(journaling);
      }
    });
  }

  async applySnapshot(data: Observable<Triple[]>, lastHash: Hash, lastTime: TreeClock, localTime: TreeClock) {
    this.dataset.transact(async () => {
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
    const entryId = 'entry:' + block.id.encode();
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

function toTimeString(time?: TreeClock): string | null {
  return time ? JSON.stringify(time.toJson()) : null;
}

function fromTimeString(timeString: string): TreeClock | null {
  return timeString ? TreeClock.fromJson(JSON.parse(timeString)) : null;
}
