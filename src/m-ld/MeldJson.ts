import { MeldDelta, JsonDelta, UUID } from '.';
import { Triple, NamedNode } from 'rdf-js';
import { literal, namedNode, blankNode, triple as newTriple } from '@rdfjs/data-model';
import { HashBagBlock } from '../blocks';
import { Hash } from '../hash';
import { compact, toRDF } from 'jsonld';
import { rdfToJson } from '../util';
import { TreeClock } from '../clocks';

namespace meld {
  export const $id = 'http://m-ld.org';
  export const tid: NamedNode = namedNode($id + '/#tid'); // TID property
}
namespace rdf {
  export const $id = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
  export const type = namedNode($id + 'type');
  export const Statement = namedNode($id + 'Statement');
  export const subject = namedNode($id + 'subject');
  export const predicate = namedNode($id + 'predicate');
  export const object = namedNode($id + 'object');
}

export function hashTriple(triple: Triple): Hash {
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

// See https://jena.apache.org/documentation/notes/reification.html
export function reify(triple: Triple, tids: UUID[]): Triple[] {
  const rid = blankNode();
  return [
    newTriple(rid, rdf.type, rdf.Statement),
    newTriple(rid, rdf.subject, triple.subject),
    newTriple(rid, rdf.predicate, triple.predicate),
    newTriple(rid, rdf.object, triple.object)
  ].concat(tids.map(tid => newTriple(rid, meld.tid, literal(tid))));
}

export function unreify(reifications: Triple[]): [Triple, UUID[]][] {
  return Object.values(reifications.reduce((rids, reification) => {
    const rid = reification.subject.value;
    let [triple, tids] = rids[rid] || [{}, []];
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
      case meld.tid.value:
        tids.push(reification.object.value);
        break;
    }
    rids[rid] = [triple, tids];
    return rids;
  }, {} as { [rid: string]: [Triple, UUID[]] }));
}

export class JsonDeltaBagBlock extends HashBagBlock<JsonDelta> {
  constructor(id: Hash, data?: JsonDelta) { super(id, data); }
  protected construct = (id: Hash, data: JsonDelta) => new JsonDeltaBagBlock(id, data);
  protected hash = (data: JsonDelta) => Hash.digest(data.tid, data.insert, data.delete);
}

/**
 * @see m-ld/m-ld-core/src/main/java/org/m_ld/MeldResource.java
 */
const DEFAULT_CONTEXT = {
  '@base': meld.$id,
  rdf: rdf.$id,
  xs: 'http://www.w3.org/2001/XMLSchema#',
  tid: meld.tid.value,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

export async function newDelta(delta: Omit<MeldDelta, 'json'>): Promise<MeldDelta> {
  return {
    ...delta,
    json: {
      tid: delta.tid,
      insert: JSON.stringify(await toMeldJson(delta.insert)),
      delete: JSON.stringify(await toMeldJson(delta.delete))
    }
  };
}

export async function asMeldDelta(delta: JsonDelta): Promise<MeldDelta> {
  return {
    tid: delta.tid,
    insert: await fromMeldJson(JSON.parse(delta.insert)),
    delete: await fromMeldJson(JSON.parse(delta.delete)),
    json: delta,
    toString: () => `${this.tid}: ${JSON.stringify(this.json)}`
  }
}

export async function toMeldJson(triples: Triple[]): Promise<any> {
  const jsonld = await rdfToJson(triples);
  const graph: any = await compact(jsonld, DEFAULT_CONTEXT);
  // The jsonld processor may create a top-level @graph with @context
  delete graph['@context'];
  return '@graph' in graph ? graph['@graph'] : graph;
}

export async function fromMeldJson(json: any): Promise<Triple[]> {
  return await toRDF({ '@graph': json, '@context': DEFAULT_CONTEXT }) as Triple[];
}

export function toTimeString(time?: TreeClock): string | null {
  return time ? JSON.stringify(time.toJson()) : null;
}

export function fromTimeString(timeString: string): TreeClock | null {
  return timeString ? TreeClock.fromJson(JSON.parse(timeString)) : null;
}

