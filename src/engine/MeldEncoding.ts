import { MeldDelta, EncodedDelta, UUID, txnId } from '.';
import { flatten, lazy } from './util';
import { Context, ExpandedTermDef } from '../jrql-support';
import { Iri } from 'jsonld/jsonld-spec';
import { RdfFactory, Triple } from './quads';
import { activeCtx } from "./jsonld";
import { M_LD, RDF } from '../ns';
import { SubjectGraph } from './SubjectGraph';
import { ActiveContext } from 'jsonld/lib/context';
import { SubjectQuads } from './SubjectQuads';
import { TreeClock } from './clocks';

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
      case RDF.subject:
        if (reification.object.termType == 'NamedNode')
          triple.subject = reification.object;
        break;
      case RDF.predicate:
        if (reification.object.termType == 'NamedNode')
          triple.predicate = reification.object;
        break;
      case RDF.object:
        triple.object = reification.object;
        break;
      case M_LD.tid:
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
  rdf: RDF.$base,
  xs: 'http://www.w3.org/2001/XMLSchema#',
  tid: M_LD.tid,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

export class MeldEncoding {
  private /*readonly*/ ctx: ActiveContext;
  private readonly ready: Promise<unknown>;

  constructor(
    readonly domain: string,
    readonly rdf: RdfFactory) {
    this.ready = activeCtx(new DomainContext(domain, DELTA_CONTEXT))
      .then(ctx => this.ctx = ctx);
  }

  initialise = () => this.ready;

  private name = lazy(name => this.rdf.namedNode(name));

  reifyTriplesTids(triplesTids: [Triple, string[]][]): Triple[] {
    return flatten(triplesTids.map(([triple, tids]) => {
      const rid = this.rdf.blankNode();
      return [
        // Reification must be known, so Statement type is redundant
        // this.rdf.quad(rid, this.name(RDF.type), this.name(RDF.Statement)),
        this.rdf.quad(rid, this.name(RDF.subject), triple.subject),
        this.rdf.quad(rid, this.name(RDF.predicate), triple.predicate),
        this.rdf.quad(rid, this.name(RDF.object), triple.object)
      ].concat(tids.map(tid =>
        this.rdf.quad(rid, this.name(M_LD.tid), this.rdf.literal(tid))));
    }));
  }

  newDelta = async (delta: Omit<MeldDelta, 'encoded'>, fused = false): Promise<MeldDelta> => {
    const [deletes, inserts] = await Promise.all([delta.deletes, delta.inserts]
      .map((triplesTids, i) => {
          // Encoded inserts are only reified if fused
        if (i === 1 && !fused)
          return triplesTids.map(([triple]) => triple);
        else
          return this.reifyTriplesTids(triplesTids);
      })
      .map(triples => EncodedDelta.encode(this.jsonFromTriples(triples))));
    return { ...delta, encoded: [2, fused, deletes, inserts] };
  }

  asDelta = async (time: TreeClock, encoded: EncodedDelta): Promise<MeldDelta> => {
    const [ver] = encoded;
    if (ver < 1)
      throw new Error(`Encoded delta version ${ver} not supported`);
    let [, fused, del, ins] = encoded;
    // @ts-ignore
    if (ver === 1) {
      fused = false;
      // @ts-ignore
      [, del, ins] = encoded;
    }
    const jsons = await Promise.all([del, ins].map(EncodedDelta.decode));
    const [deletes, inserts] = jsons.map(this.triplesFromJson);
    // Note transaction ID is not needed if the encoding is fused
    const tid = fused ? '' : txnId(time);
    return {
      inserts: fused ? unreify(inserts) : inserts.map(triple => [triple, [tid]]),
      deletes: unreify(deletes),
      encoded,
      toString: () => `${JSON.stringify(jsons)}`
    };
  }

  jsonFromTriples = (triples: Triple[]): any => {
    const json = SubjectGraph.fromRDF(triples, { ctx: this.ctx });
    // Recreates JSON-LD compaction behaviour
    return json.length == 0 ? {} : json.length == 1 ? json[0] : json;
  }

  triplesFromJson = (json: any): Triple[] =>
    [...new SubjectQuads('graph', this.ctx, this.rdf).quads(json)];
}
