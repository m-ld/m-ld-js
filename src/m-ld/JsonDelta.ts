import { MeldDelta, JsonDelta, UUID } from '.';
import { Triple, NamedNode } from 'rdf-js';
import { generate as uuid } from 'short-uuid';
import { literal, namedNode, triple as newTriple } from '@rdfjs/data-model';
import { HashBagBlock } from '../blocks';
import { Hash } from '../hash';
import { asGroup, GroupLike, Context, Group } from './jsonrql';
import { fromRDF, compact, toRDF } from 'jsonld';
import { Iri } from 'jsonld/jsonld-spec';
import { flatten } from '../util';
import { TreeClock } from '../clocks';

//TODO: Correct all implementations to use generic @base for reification
namespace jena {
  export const $id = 'http://jena.m-ld.org/jena-delta/';
  export const tid: NamedNode = namedNode($id + '#tid'); // Reification ID property
}
namespace rdf {
  export const $id = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
  export const type = namedNode($id + 'type');
  export const Statement = namedNode($id + 'Statement');
  export const subject = namedNode($id + 'subject');
  export const predicate = namedNode($id + 'predicate');
  export const object = namedNode($id + 'object');
}

// See https://jena.apache.org/documentation/notes/reification.html
export function reify(triple: Triple, tid: UUID): Triple[] {
  const rid = namedNode(jena.$id + uuid());
  return [
    newTriple(rid, rdf.type, rdf.Statement),
    newTriple(rid, rdf.subject, triple.subject),
    newTriple(rid, rdf.predicate, triple.predicate),
    newTriple(rid, rdf.object, triple.object),
    newTriple(rid, jena.tid, literal(tid))
  ];
}

export function unreify(reifications: Triple[]): [Triple, UUID][] {
  return Object.values(reifications.reduce((rids, reification) => {
    const rid = reification.subject.value;
    let [triple, tid] = rids[rid] || [{}, null];
    switch (reification.predicate.value) {
      case rdf.subject.value:
        if (reification.object.termType == 'NamedNode')
          triple.subject = reification.object;
        break;
      case rdf.predicate.value:
        if (reification.object.termType == 'NamedNode')
          triple.predicate = reification.object;
        break;
      case rdf.object.value:
        triple.object = reification.object;
        break;
      case jena.tid.value:
        tid = reification.object.value;
        break;
    }
    rids[rid] = [triple, tid];
    return rids;
  }, {} as { [rid: string]: [Triple, UUID] }));
}

export class JsonDeltaBagBlock extends HashBagBlock<JsonDelta> {
  constructor(id: Hash, data?: JsonDelta) { super(id, data); }
  protected construct = (id: Hash, data: JsonDelta) => new JsonDeltaBagBlock(id, data);
  protected hash = (data: JsonDelta) => Hash.digest(data.tid, data.insert, data.delete);
}

const DELETE_CONTEXT = {
  '@base': jena.$id,
  rdf: rdf.$id,
  tid: jena.tid.value,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

export async function newDelta(delta: Omit<MeldDelta, 'json'>): Promise<MeldDelta> {
  return {
    ...delta,
    json: {
      tid: delta.tid,
      insert: await toJson(delta.insert, {}),
      delete: await toJson(delta.delete, DELETE_CONTEXT)
    }
  };
}

export async function asMeldDelta(delta: JsonDelta): Promise<MeldDelta> {
  return {
    tid: delta.tid,
    insert: await fromJson(delta.insert, {}),
    delete: await fromJson(delta.delete, DELETE_CONTEXT),
    json: delta
  }
}

async function toJson(triples: Triple[], context: Context): Promise<string> {
  const jsonld = await fromRDF(triples);
  const group = asGroup(await compact(jsonld, context || {}) as GroupLike);
  delete group['@context'];
  return JSON.stringify(group);
}

async function fromJson(json: string, context: Context): Promise<Triple[]> {
  const jsonld = JSON.parse(json) as Group;
  jsonld['@context'] = context;
  return await toRDF(jsonld) as Triple[];
}

export function toTimeString(time?: TreeClock): string | null {
  return time ? JSON.stringify(time.toJson()) : null;
}

export function fromTimeString(timeString: string): TreeClock | null {
  return timeString ? TreeClock.fromJson(JSON.parse(timeString)) : null;
}

