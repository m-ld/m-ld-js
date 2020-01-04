import { MeldDelta, JsonDelta, UUID } from './meld';
import { Triple, NamedNode } from 'rdf-js';
import { v4 as uuid } from 'uuid';
import { literal, namedNode, triple as newTriple } from '@rdfjs/data-model';
import { HashBagBlock } from './blocks';
import { Hash } from './hash';
import { asGroup, GroupLike, Context } from './jsonrql';
import { fromRDF, compact } from 'jsonld';
import { Iri } from 'jsonld/jsonld-spec';

//TODO: Correct all implementations to use generic @base for reification
namespace jena {
  export const $id = 'http://jena.m-ld.org/JenaDelta/';
  export const tid: NamedNode = namedNode($id + '#tid'); // Reification ID property
  export const rid: Iri = $id + 'rid/'; // Namespace for reification IDs
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
export function reify(triple: Triple, tid: UUID) {
  const rid = namedNode(jena.rid + uuid());
  return [
    newTriple(rid, rdf.type, rdf.Statement),
    newTriple(rid, rdf.subject, triple.subject),
    newTriple(rid, rdf.predicate, triple.predicate),
    newTriple(rid, rdf.object, triple.object),
    newTriple(rid, jena.tid, literal(tid))
  ];
}

export class JsonDeltaBagBlock extends HashBagBlock<JsonDelta> {
  constructor(id: Hash, data?: JsonDelta) { super(id, data); }
  protected construct = (id: Hash, data: JsonDelta) => new JsonDeltaBagBlock(id, data);
  protected hash = (data: JsonDelta) => Hash.digest(data.tid, data.insert, data.delete);
}

const DELETE_CONTEXT = {
  '@base': jena.$id,
  rdf: rdf.$id,
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

async function toJson(quads: Triple[], context: Context): Promise<string> {
  const jsonld = await fromRDF(quads);
  const group = asGroup(await compact(jsonld, context || {}) as GroupLike);
  delete group['@context'];
  return JSON.stringify(group);
}

