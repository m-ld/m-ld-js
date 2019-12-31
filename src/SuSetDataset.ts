import { MeldDelta, DeltaMessage } from './meld';
import { Quad } from 'rdf-js';
import { v4 as uuid } from 'uuid';
import { literal, namedNode } from '@rdfjs/data-model';
import { TreeClock } from './clocks';
import { Hash } from './hash';
import { Context } from './jsonrql';
import { Dataset, PatchQuads } from './Dataset';
import { Iri } from 'jsonld/jsonld-spec';
import { JrqlGraph } from './JrqlGraph';
import { reify } from './JsonDelta';

const CONTROL_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/',
  tail: 'qs:#tail', // Property of the journal
  lastDelivered: 'qs:#lastDelivered', // Property of the journal
  entry: 'qs:journal/entry/', // Namespace for journal entries
  hash: 'qs:#hash', // Property of a journal entry
  delta: 'qs:#delta', // Property of a journal entry
  remote: 'qs:#remote', // Property of a journal entry
  time: 'qs:#time', // Property of journal AND a journal entry
  next: 'qs:#next' // Property of a journal entry
};
const JOURNAL = 'qs:journal' as Iri; // Singleton object

export interface SuSetTransaction {
  remove(quads: Quad[]): this;
  add(quads: Quad[]): this;
}

export class SuSetDataset extends JrqlGraph {
  private readonly controlGraph: JrqlGraph;

  constructor(
    private readonly dataset: Dataset) {
    super(dataset.graph());
    // Named graph for control quads e.g. Journal
    this.controlGraph = new JrqlGraph(dataset.graph(namedNode(CONTROL_CONTEXT.qs + 'control')));
  }

  async initialise() {
    if (!await this.controlGraph.describe(JOURNAL))
      return this.reset(Hash.random());
  }

  async reset(startingHash: Hash, startingTime?: TreeClock) {
    return this.dataset.transact(async () => {
      const encodedHash = startingHash.encode();
      const insert = await this.controlGraph.insert({
        '@id': JOURNAL,
        tail: {
          '@id': `entry:${encodedHash}`,
          hash: encodedHash,
          time: startingTime ? literal(JSON.stringify(startingTime.toJson())) : null
        }
      }, CONTROL_CONTEXT);
      // Delete matches everything in all graphs
      return { oldQuads: {}, newQuads: insert.newQuads };
    });
  }

  async loadClock(): Promise<TreeClock | null> {
    const journal = await this.controlGraph.describe(JOURNAL, CONTROL_CONTEXT);
    return TreeClock.fromJson(JSON.parse(journal?.time));
  }

  async saveClock(time: TreeClock, newClone?: boolean): Promise<void> {
    return this.dataset.transact(async () => await this.patchClock(time, newClone));
  }

  private async patchClock(time: TreeClock, newClone?: boolean): Promise<PatchQuads> {
    const encodedTime = JSON.stringify(time.toJson());
    // No support for variables, so we have to pre-load the existing journal
    const journal = await this.controlGraph.describe(JOURNAL, CONTROL_CONTEXT);
    const update = {
      '@context': CONTROL_CONTEXT,
      '@delete': { '@id': JOURNAL, time: journal.time },
      '@insert': [{ '@id': JOURNAL, time: encodedTime }]
    };
    if (newClone) {
      // For a new clone, the journal's dummy tail does not already have a timestamp
      update['@insert'].push({ '@id': journal.tail, time: encodedTime });
    }
    return await this.controlGraph.write(update);
  }

  async transact(prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<DeltaMessage> {
    return this.dataset.transact(async () => {
      const [time, patch] = await prepare();
      // Establish reifications of old quads
      const delta: MeldDelta = { tid: uuid(), insert: patch.newQuads, delete: [/*TODO*/] }
      const finalPatch = patch
        // Include reifications in final patch
        .concat(new PatchQuads(
          // Deleted reifications of old quads
          delta.delete,
          // Reified new quads
          delta.insert.reduce((quads: Quad[], quad) => {
            quads.push(...reify(quad, delta.tid));
            return quads;
          }, [])
        ))
        // Include journaling in final patch
        .concat(await this.journal(time));
      return [finalPatch, { time, data: delta }];
    });
  }

  private async journal(time: TreeClock): Promise<PatchQuads> {
    // TODO
    return new PatchQuads([], []).concat(await this.patchClock(time));
  };
}