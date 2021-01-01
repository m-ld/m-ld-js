import { MeldDelta, EncodedDelta, UUID } from '.';
import { NamedNode, Quad } from 'rdf-js';
// FIXME: Make this a data factory field of the MeldEncoding
import { literal, namedNode, blankNode, triple as newTriple, defaultGraph, quad as newQuad } from '@rdfjs/data-model';
import { compact } from 'jsonld';
import { flatten } from './util';
import { Context, ExpandedTermDef } from '../jrql-support';
import { Iri } from 'jsonld/jsonld-spec';
import { rdf, Triple, TripleMap } from './quads';
import { rdfToJson, jsonToRdf } from "./jsonld";

export class DomainContext implements Context {
  '@base': Iri;
  '@vocab': Iri;
  [key: string]: string | ExpandedTermDef;

  constructor(domain: string, context?: Context) {
    if (!/^[a-z0-9_]+([\-.][a-z0-9_]+)*\.[a-z]{2,6}$/.test(domain))
      throw new Error('Domain not specified or not valid');
    Object.assign(this, context);
    if (this['@base'] == null)
      this['@base'] = `http://${domain}/`;
    if (this['@vocab'] == null)
      this['@vocab'] = new URL('/#', this['@base']).href
  }
}

export namespace meld {
  export const $id = 'http://m-ld.org';
  export const tid: NamedNode = namedNode(`${$id}/#tid`); // TID property
  export const rdflseq: NamedNode = namedNode(`${$id}/RdfLseq`);
  export const rdflseqPosIdPre = `${rdflseq.value}/?=`;
}

export function reifyTriplesTids(triplesTids: TripleMap<UUID[]>): Triple[] {
  return flatten([...triplesTids].map(([triple, tids]) => {
    const rid = blankNode();
    return [
      newTriple(rid, rdf.type, rdf.Statement),
      newTriple(rid, rdf.subject, triple.subject),
      newTriple(rid, rdf.predicate, triple.predicate),
      newTriple(rid, rdf.object, triple.object)
    ].concat(tids.map(tid => newTriple(rid, meld.tid, literal(tid))));
  }));
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

/**
 * TODO: re-sync with Java
 * @see m-ld/m-ld-core/src/main/java/org/m_ld/MeldResource.java
 */
const DELTA_CONTEXT = {
  rdf: rdf.$id,
  xs: 'http://www.w3.org/2001/XMLSchema#',
  tid: meld.tid.value,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

export class MeldEncoding {
  context: DomainContext;

  constructor(readonly domain: string) {
    this.context = new DomainContext(domain, DELTA_CONTEXT);
  }

  newDelta = async (delta: Omit<MeldDelta, 'encoded'>): Promise<MeldDelta> => {
    const [del, ins] = await Promise.all([delta.delete, delta.insert]
      .map(triples => this.jsonFromTriples(triples).then(EncodedDelta.encode)));
    return { ...delta, encoded: [1, del, ins] };
  }

  asDelta = async (delta: EncodedDelta): Promise<MeldDelta> => {
    const [ver, del, ins] = delta;
    if (ver !== 1)
      throw new Error(`Encoded delta version ${ver} not supported`);
    const jsons = await Promise.all([del, ins].map(EncodedDelta.decode));
    const [delTriples, insTriples] = await Promise.all(jsons.map(this.triplesFromJson));
    return ({
      insert: insTriples, delete: delTriples, encoded: delta,
      toString: () => `${JSON.stringify(jsons)}`
    });
  }

  jsonFromTriples = async (triples: Triple[]): Promise<any> => {
    const jsonld = await rdfToJson(triples.map(toDomainQuad));
    const graph: any = await compact(jsonld, this.context);
    // The jsonld processor may create a top-level @graph with @context
    delete graph['@context'];
    return '@graph' in graph ? graph['@graph'] : graph;
  }

  triplesFromJson = async (json: any): Promise<Triple[]> =>
    await jsonToRdf({ '@graph': json, '@context': this.context }) as Triple[];
  
}

export function toDomainQuad(triple: Triple): Quad {
  return newQuad(triple.subject, triple.predicate, triple.object, defaultGraph());
}
