import { MeldDelta, UUID } from './meld';
import { Quad, NamedNode } from 'rdf-js';
import { v4 as uuid } from 'uuid';
import { literal, namedNode, quad as createQuad } from '@rdfjs/data-model';
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
export function reify(quad: Quad, tid: UUID) {
  const rid = namedNode(jena.rid + uuid());
  return [
    createQuad(rid, rdf.type, rdf.Statement),
    createQuad(rid, rdf.subject, quad.subject),
    createQuad(rid, rdf.predicate, quad.predicate),
    createQuad(rid, rdf.object, quad.object),
    createQuad(rid, jena.tid, literal(tid))
  ];
}

export class JsonDeltaBagBlock extends HashBagBlock<JsonDelta> {
  private constructor(id: Hash, data: JsonDelta) { super(id, data); }
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

export interface JsonDelta {
  tid: string,
  insert: string,
  delete: string
}

export namespace JsonDelta {
  export async function toJson(quads: Quad[], context: Context): Promise<string>;
  export async function toJson(delta: MeldDelta): Promise<JsonDelta>;
  export async function toJson(object: Quad[] | MeldDelta, context?: Context): Promise<string | JsonDelta> {
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
}

