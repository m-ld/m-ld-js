import { MeldDelta, UUID, DeltaMessage } from './meld';
import { Quad, NamedNode, Quad_Subject, Quad_Predicate, Quad_Object } from 'rdf-js';
import { v4 as uuid } from 'uuid';
import { quad as createQuad, literal } from '@rdfjs/data-model';
import { TreeClock } from './clocks';
import { HashBagBlock } from './blocks';
import { Hash } from './hash';
import { asGroup, GroupLike, Context } from './jsonrql';
import { fromRDF, compact } from 'jsonld';
import { Graph, Dataset, Patch, PatchQuads } from './Dataset';

const ns: (namespace: string) => (name: string) => NamedNode = require('@rdfjs/namespace');
namespace m_ld {
  export namespace qs {
    export const $id = 'http://qs.m-ld.org/';
    const qs = ns($id);
    export const control = qs('control'); // Named graph for control quads e.g. Journal
    export const journal: NamedNode = qs('journal'); // Singleton object
    export const tail: NamedNode = qs('journal/#tail'); // Property of the journal
    export const lastDelivered: NamedNode = qs('journal/#lastDelivered'); // Property of the journal
    export const entry: (hash: string) => NamedNode = ns(qs('journal/entry/').value); // Namespace for journal entries
    export const hash: NamedNode = entry('#hash'); // Property of a journal entry
    export const delta: NamedNode = entry('#delta'); // Property of a journal entry
    export const remote: NamedNode = entry('#remote'); // Property of a journal entry
    export const time: NamedNode = entry('#time'); // Property of a journal entry
    export const next: NamedNode = entry('#next'); // Property of a journal entry
  }
  /*
   * TODO: Correct all implementations to use generic @base for reification
   */
  export namespace jena {
    export const $id = 'http://jena.m-ld.org/JenaDelta/';
    const jena = ns($id);
    export const rid: (uuid: string) => NamedNode = ns(jena('rid/').value); // Namespace for reification IDs
    export const tid: NamedNode = jena('#tid'); // Global property
  }
}
namespace rdf {
  export const $id = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
  const rdf = ns($id);
  export const type: NamedNode = rdf('type');
  export const Statement: NamedNode = rdf('Statement');
  export const subject: NamedNode = rdf('subject');
  export const predicate: NamedNode = rdf('predicate');
  export const object: NamedNode = rdf('object');
}

interface ReifiedQuad extends Quad {
  id: NamedNode;
}

type ReifyingQuad = Quad; // Could be better typed

// See https://jena.apache.org/documentation/notes/reification.html
const reification = (quad: ReifiedQuad, tid: UUID) => [
  createQuad(quad.id, rdf.type, rdf.Statement),
  createQuad(quad.id, rdf.subject, quad.subject),
  createQuad(quad.id, rdf.predicate, quad.predicate),
  createQuad(quad.id, rdf.object, quad.object),
  createQuad(quad.id, m_ld.jena.tid, literal(tid))
];

const reify = (quad: Quad) => ({ id: m_ld.jena.rid(uuid()), ...quad });

export class SuSetDataset implements Graph {
  private readonly controlGraph: Graph;
  private readonly defaultGraph: Graph;

  constructor(
    private readonly dataset: Dataset) {
    this.controlGraph = dataset.graph(m_ld.qs.control);
    this.defaultGraph = dataset.graph();
  }

  async initialise() {
    if (!(await this.dataset.graph(m_ld.qs.control).match(m_ld.qs.journal, m_ld.qs.tail)).length)
      return this.reset(Hash.random(), TreeClock.GENESIS);
  }

  async reset(startingHash: Hash, startingTime: TreeClock) {
    const encodedHash = startingHash.encode();
    const head = m_ld.qs.entry(encodedHash);
    return this.dataset.transact(() => Promise.resolve([{
      oldQuads: {}, // Matches everything in all graphs
      newQuads: [
        // The starting head is a dummy entry that only captures the hash.
        createQuad(head, m_ld.qs.hash, literal(encodedHash), m_ld.qs.control),
        createQuad(head, m_ld.qs.time, literal(JSON.stringify(startingTime.toJson())), m_ld.qs.control),
        createQuad(m_ld.qs.journal, m_ld.qs.lastDelivered, head, m_ld.qs.control),
        createQuad(m_ld.qs.journal, m_ld.qs.tail, head, m_ld.qs.control)
      ]
    }, undefined]));
  }

  async loadClock(): Promise<TreeClock | null> {
    const quads = await this.dataset.graph(m_ld.qs.control).match(m_ld.qs.journal, m_ld.qs.time);
    return quads.length ? TreeClock.fromJson(JSON.parse(quads[0].object.value)) : null;
  }

  async saveClock(time: TreeClock) {

  }

  async transact(prepare: (txn: SuSetTransaction) => Promise<TreeClock>): Promise<DeltaMessage> {
    return this.dataset.transact(async () => {
      const txn = new SuSetDatasetTransaction(this.controlGraph);
      return txn.commit(await prepare(txn));
    });
  }

  async match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Promise<Quad[]> {
    return this.defaultGraph.match(subject, predicate, object);
  }
}

export interface SuSetTransaction {
  remove(quads: Quad[]): this;
  add(quads: Quad[]): this;
}

class SuSetDatasetTransaction implements SuSetTransaction, MeldDelta {
  readonly tid: UUID = uuid();
  readonly insert: ReifiedQuad[] = [];
  readonly delete: ReifyingQuad[] = [];

  constructor(
    private readonly controlGraph: Graph) {
  }

  remove(quads: Quad[]): this {
    // TODO
    return this;
  }

  add(quads: Quad[]): this {
    this.insert.push(...quads.map(reify));
    return this;
  }

  async commit(time: TreeClock): Promise<[Patch, DeltaMessage]> {
    return [
      this.asPatch().concat(await this.journal(time, false)),
      { time, data: this }
    ];
  }

  private asPatch = () => new PatchQuads(this.oldQuads, this.newQuads);

  private get oldQuads(): Quad[] {
    return []; // TODO
  }

  private get newQuads(): Quad[] {
    return this.insert.reduce((quads: Quad[], quad) => {
      quads.push(quad, ...reification(quad, this.tid));
      return quads;
    }, []);
  }

  private async journal(time: TreeClock, isRemote: boolean): Promise<PatchQuads> {
    // Find the old tail
    const oldTailQuad = (await this.controlGraph.match(m_ld.qs.journal, m_ld.qs.tail))[0];
    // const oldTailHashQuad = await this.controlGraph.get(oldTailQuad.object as NamedNode, m_ld.qs.hash);
    // const oldTailDeltaQuad = await this.controlGraph.get(oldTailQuad.object as NamedNode, m_ld.qs.delta);

    // TODO: Also save the time
    return new PatchQuads([], []);
  };
}

class JsonDeltaBagBlock extends HashBagBlock<JsonDelta> {
  private constructor(id: Hash, data: JsonDelta) { super(id, data); }
  protected construct = (id: Hash, data: JsonDelta) => new JsonDeltaBagBlock(id, data);
  protected hash = (data: JsonDelta) => Hash.digest(data.tid, data.insert, data.delete);
}

const DELETE_CONTEXT = {
  '@base': m_ld.jena.$id,
  rdf: rdf.$id,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

interface JsonDelta {
  tid: string,
  insert: string,
  delete: string
}

async function toJson(quads: Quad[], context: Context): Promise<string>;
async function toJson(delta: MeldDelta): Promise<JsonDelta>;
async function toJson(object: Quad[] | MeldDelta, context?: Context): Promise<string | JsonDelta> {
  if (Array.isArray(object)) {
    const jsonld = await fromRDF(object);
    const group = asGroup(await compact(jsonld, context || {}) as GroupLike);
    delete group['@context'];
    return JSON.stringify(group);
  } else {
    return {
      tid: object.tid,
      insert: await toJson(object.insert, {}),
      delete: await toJson(object.delete, DELETE_CONTEXT)
    };
  }
}