import { MeldDelta, MeldJournalEntry } from './meld';
import { Quad } from 'rdf-js';
import { v4 as uuid } from 'uuid';
import { namedNode } from '@rdfjs/data-model';
import { TreeClock } from './clocks';
import { Hash } from './hash';
import { Context, Subject } from './jsonrql';
import { Dataset, PatchQuads, Patch } from './Dataset';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph } from './JrqlGraph';
import { reify, JsonDeltaBagBlock, newDelta } from './JsonDelta';
import { Observable, Subscriber } from 'rxjs';

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
  delta: string, // JSON-encoded MeldDelta
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
      return this.reset(Hash.random());
  }

  private async reset(startingHash: Hash, startingTime?: TreeClock) {
    return this.dataset.transact(async () => {
      const encodedHash = startingHash.encode();
      const entryId = 'entry:' + encodedHash;
      const insert = await this.controlGraph.insert({
        '@id': 'qs:journal',
        lastDelivered: entryId,
        tail: {
          '@id': entryId,
          hash: encodedHash,
          time: startingTime ? JSON.stringify(startingTime.toJson()) : null
        } as Partial<JournalEntry>
      } as Journal);
      // Delete matches everything in all graphs
      return { oldQuads: {}, newQuads: insert.newQuads };
    });
  }

  async loadClock(): Promise<TreeClock | null> {
    const journal = await this.loadJournal();
    return TreeClock.fromJson(JSON.parse(journal.time));
  }

  async saveClock(time: TreeClock, newClone?: boolean): Promise<void> {
    return this.dataset.transact(async () =>
      await this.patchClock(await this.loadJournal(), time, newClone));
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

  async emitJournalAfter(entry: JournalEntry, subs: Subscriber<MeldJournalEntry>) {
    if (entry.next) {
      entry = await this.controlGraph.describe(entry.next) as JournalEntry;
      const delivered = () => this.markDelivered(entry['@id']);
      const time = TreeClock.fromJson(entry.time) as TreeClock; // Never null
      subs.next({ time, data: JSON.parse(entry.delta), delivered });
      await this.emitJournalAfter(entry, subs);
    }
  }

  private async patchClock(journal: Journal, time: TreeClock, newClone?: boolean): Promise<PatchQuads> {
    const encodedTime = JSON.stringify(time.toJson());
    const update = {
      '@context': CONTROL_CONTEXT,
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
        // Establish reifications of old quads
        delete: [/*TODO*/]
      });
      // Include reifications in final patch
      const reifications = {
        oldQuads: delta.delete,
        // Reified new quads
        newQuads: ([] as Quad[]).concat(...delta.insert.map(quad => reify(quad, delta.tid)))
      };
      // Include journaling in final patch
      const [journaling, entry] = await this.journal(time, delta, false);
      return [patch.concat(reifications).concat(journaling), entry] as [Patch, MeldJournalEntry];
    });
  }

  private async journal(time: TreeClock, delta: MeldDelta, remote: boolean): Promise<[PatchQuads, MeldJournalEntry]> {
    const journal = await this.loadJournal();
    const oldTail = await this.controlGraph.describe(journal.tail) as JournalEntry;
    const block = new JsonDeltaBagBlock(Hash.decode(oldTail.hash)).next(delta.json);
    const entryId = 'entry:' + block.id.encode();
    const delivered = () => this.markDelivered(entryId);
    return [
      (await this.controlGraph.write({
        '@context': CONTROL_CONTEXT,
        '@delete': { '@id': 'qs:journal', tail: journal.tail } as Partial<Journal>,
        '@insert': [
          { '@id': 'qs:journal', tail: entryId } as Partial<Journal>,
          { '@id': journal.tail, next: entryId } as Partial<JournalEntry>,
          {
            '@id': entryId, remote,
            hash: block.id.encode(),
            tid: delta.tid,
            time: JSON.stringify(time.toJson()),
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
      '@context': CONTROL_CONTEXT,
      '@delete': { '@id': 'qs:journal', lastDelivered: journal.lastDelivered },
      '@insert': { '@id': 'qs:journal', lastDelivered: entryId }
    });
  }
}