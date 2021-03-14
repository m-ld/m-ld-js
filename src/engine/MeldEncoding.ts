import { MeldDelta, EncodedDelta, UUID } from '.';
import { Quad } from 'rdf-js';
import { flatten, lazy } from './util';
import { Context, ExpandedTermDef } from '../jrql-support';
import { Iri } from 'jsonld/jsonld-spec';
import { RdfFactory, Triple, TripleMap } from './quads';
import { activeCtx } from "./jsonld";
import { mld, rdf } from '../ns';
import { SubjectGraph } from './SubjectGraph';
import { ActiveContext } from 'jsonld/lib/context';
import { SubjectQuads } from './SubjectQuads';

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

export function unreify(reifications: Triple[]): [Triple, UUID[]][] {
  return Object.values(reifications.reduce((rids, reification) => {
    const rid = reification.subject.value;
    let [triple, tids] = rids[rid] || [{}, []];
    switch (reification.predicate.value) {
      case rdf.subject:
        if (reification.object.termType == 'NamedNode')
          triple.subject = reification.object;
        break;
      case rdf.predicate:
        if (reification.object.termType == 'NamedNode')
          triple.predicate = reification.object;
        break;
      case rdf.object:
        triple.object = reification.object;
        break;
      case mld.tid:
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
  rdf: rdf.$base,
  xs: 'http://www.w3.org/2001/XMLSchema#',
  tid: mld.tid,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

export class MeldEncoding {
  ctx: ActiveContext;
  ready: Promise<unknown>;

  constructor(
    readonly domain: string,
    readonly makeRdf: RdfFactory) {
    this.ready = activeCtx(new DomainContext(domain, DELTA_CONTEXT))
      .then(ctx => this.ctx = ctx);
  }

  private name = lazy(name => this.makeRdf.namedNode(name));

  reifyTriplesTids(triplesTids: TripleMap<UUID[]>): Triple[] {
    return flatten([...triplesTids].map(([triple, tids]) => {
      const rid = this.makeRdf.blankNode();
      return [
        this.makeRdf.quad(rid, this.name(rdf.type), this.name(rdf.Statement)),
        this.makeRdf.quad(rid, this.name(rdf.subject), triple.subject),
        this.makeRdf.quad(rid, this.name(rdf.predicate), triple.predicate),
        this.makeRdf.quad(rid, this.name(rdf.object), triple.object)
      ].concat(tids.map(tid =>
        this.makeRdf.quad(rid, this.name(mld.tid), this.makeRdf.literal(tid))));
    }));
  }

  newDelta = async (delta: Omit<MeldDelta, 'encoded'>): Promise<MeldDelta> => {
    const [del, ins] = await Promise.all([delta.delete, delta.insert]
      .map(triples => EncodedDelta.encode(this.jsonFromTriples(triples))));
    return { ...delta, encoded: [1, del, ins] };
  }

  asDelta = async (delta: EncodedDelta): Promise<MeldDelta> => {
    const [ver, del, ins] = delta;
    if (ver !== 1)
      throw new Error(`Encoded delta version ${ver} not supported`);
    const jsons = await Promise.all([del, ins].map(EncodedDelta.decode));
    const [delTriples, insTriples] = jsons.map(this.triplesFromJson);
    return ({
      insert: insTriples, delete: delTriples, encoded: delta,
      toString: () => `${JSON.stringify(jsons)}`
    });
  }

  jsonFromTriples = (triples: Triple[]): any => {
    const json = SubjectGraph.fromRDF(triples, this);
    // Recreates JSON-LD compaction behaviour
    return json.length == 0 ? {} : json.length == 1 ? json[0] : json;
  }

  triplesFromJson = (json: any): Triple[] =>
    [...new SubjectQuads('graph', this.ctx, this.makeRdf).quads(json)];

  toDomainQuad = (triple: Triple): Quad => this.makeRdf.quad(
    triple.subject,
    triple.predicate,
    triple.object,
    this.makeRdf.defaultGraph());
}
