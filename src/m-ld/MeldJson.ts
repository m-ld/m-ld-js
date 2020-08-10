import { MeldDelta, JsonDelta, UUID, Triple } from '.';
import { NamedNode, Quad } from 'rdf-js';
import { literal, namedNode, blankNode, triple as newTriple, defaultGraph, quad as newQuad } from '@rdfjs/data-model';
import { HashBagBlock } from '../blocks';
import { Hash } from '../hash';
import { compact, toRDF } from 'jsonld';
import { rdfToJson, flatten, jsonToRdf } from '../util';
import { Context } from '../dataset/jrql-support';
import { ExpandedTermDef } from 'json-rql';
import { Iri } from 'jsonld/jsonld-spec';

export class DomainContext implements Context {
  '@base': Iri;
  '@vocab': Iri;
  [key: string]: string | ExpandedTermDef;

  constructor(domain: string, context: Context | null) {
    Object.assign(this, context);
    if (this['@base'] == null)
      this['@base'] = `http://${domain}/`;
    if (this['@vocab'] == null)
      this['@vocab'] = new URL('/#', this['@base']).href
  }
}

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

export class TripleTids {
  constructor(
    readonly triple: Triple,
    readonly tids: UUID[]) {
  }

  // See https://jena.apache.org/documentation/notes/reification.html
  reify(): Triple[] {
    const rid = blankNode();
    return [
      newTriple(rid, rdf.type, rdf.Statement),
      newTriple(rid, rdf.subject, this.triple.subject),
      newTriple(rid, rdf.predicate, this.triple.predicate),
      newTriple(rid, rdf.object, this.triple.object)
    ].concat(this.tids.map(tid => newTriple(rid, meld.tid, literal(tid))));
  }

  static reify(triplesTids: TripleTids[]): Triple[] {
    return flatten(triplesTids.map(tripleTids => tripleTids.reify()));
  }
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
 * TODO: re-sync with Java
 * @see m-ld/m-ld-core/src/main/java/org/m_ld/MeldResource.java
 */
const DEFAULT_CONTEXT = {
  rdf: rdf.$id,
  xs: 'http://www.w3.org/2001/XMLSchema#',
  tid: meld.tid.value,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

export class MeldJson {
  context: DomainContext;

  constructor(readonly domain: string) {
    this.context = new DomainContext(domain, DEFAULT_CONTEXT);
  }

  newDelta = async (delta: Omit<MeldDelta, 'json'>): Promise<MeldDelta> => ({
    ...delta,
    json: {
      tid: delta.tid,
      insert: JSON.stringify(await this.toMeldJson(delta.insert)),
      delete: JSON.stringify(await this.toMeldJson(delta.delete))
    }
  })

  asMeldDelta = async (delta: JsonDelta): Promise<MeldDelta> => ({
    tid: delta.tid,
    insert: await this.fromMeldJson(JSON.parse(delta.insert)),
    delete: await this.fromMeldJson(JSON.parse(delta.delete)),
    json: delta,
    toString: () => `${delta.tid}: ${JSON.stringify(delta)}`
  })

  toMeldJson = async (triples: Triple[]): Promise<any> => {
    const jsonld = await rdfToJson(triples.map(toDomainQuad));
    const graph: any = await compact(jsonld, this.context);
    // The jsonld processor may create a top-level @graph with @context
    delete graph['@context'];
    return '@graph' in graph ? graph['@graph'] : graph;
  }

  fromMeldJson = async (json: any): Promise<Triple[]> =>
    await jsonToRdf({ '@graph': json, '@context': this.context }) as Triple[]
}

export function toDomainQuad(triple: Triple): Quad {
  return newQuad(triple.subject, triple.predicate, triple.object, defaultGraph());
}
