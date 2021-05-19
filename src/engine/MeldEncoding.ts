import { EncodedOperation, JsonBuffer, UUID } from '.';
import { flatten, lazy } from './util';
import { Context, ExpandedTermDef } from '../jrql-support';
import { Iri } from 'jsonld/jsonld-spec';
import { RdfFactory, Triple, tripleIndexKey } from './quads';
import { activeCtx } from "./jsonld";
import { M_LD, RDF } from '../ns';
import { SubjectGraph } from './SubjectGraph';
import { ActiveContext } from 'jsonld/lib/context';
import { SubjectQuads } from './SubjectQuads';
import { TreeClock } from './clocks';
import { gzipSync, gunzipSync } from 'zlib';
import { CausalOperation, FusableCausalOperation } from './ops';
const COMPRESS_THRESHOLD_BYTES = 1024;

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

export type TriplesTids = [Triple, string[]][];

export class MeldOperation extends FusableCausalOperation<Triple, TreeClock> {
  static fromOperation = (encoder: MeldEncoder,
    op: CausalOperation<Triple, TreeClock>): MeldOperation => {
    const jsons = [op.deletes, op.inserts]
      .map((triplesTids, i) => {
        // Encoded inserts are only reified if fused
        if (i === 1 && op.from === op.time.ticks)
          return [...triplesTids].map(([triple]) => triple);
        else
          return encoder.reifyTriplesTids([...triplesTids]);
      })
      .map(encoder.jsonFromTriples);
    const [delEnc, insEnc] = jsons.map(json => MeldEncoder.bufferFromJson(json));
    const encoded: EncodedOperation = [2, op.from, op.time.toJson(), delEnc, insEnc];
    return new MeldOperation(op, encoded, jsons);
  }

  static fromEncoded = (encoder: MeldEncoder,
    encoded: EncodedOperation): MeldOperation => {
    const [ver] = encoded;
    if (ver < 2)
      throw new Error(`Encoded operation version ${ver} not supported`);
    let [, from, timeJson, delEnc, insEnc] = encoded;
    const jsons = [delEnc, insEnc].map(MeldEncoder.jsonFromBuffer);
    const [delTriples, insTriples] = jsons.map(encoder.triplesFromJson);
    const time = TreeClock.fromJson(timeJson) as TreeClock;
    const deletes = unreify(delTriples);
    let inserts: MeldOperation['inserts'];
    if (from === time.ticks) {
      const tid = time.hash();
      inserts = insTriples.map(triple => [triple, [tid]]);
    } else {
      // No need to calculate transaction ID if the encoding is fused
      inserts = unreify(insTriples);
    }
    return new MeldOperation({ from, time, deletes, inserts }, encoded, jsons);
  }

  private constructor(
    op: CausalOperation<Triple, TreeClock>,
    /**
     * Serialisation of triples is not required to be normalised. For any m-ld
     * operation, there are many possible serialisations. An operation carries its
     * serialisation with it, for journaling and hashing.
     */
    readonly encoded: EncodedOperation,
    readonly jsons: any) {
    super(op, tripleIndexKey);
  }

  toString() {
    return `${JSON.stringify(this.jsons)}`;
  }
}

/**
 * TODO: re-sync with Java
 * @see m-ld/m-ld-core/src/main/java/org/m_ld/MeldResource.java
 */
const OPERATION_CONTEXT = {
  rdf: RDF.$base,
  xs: 'http://www.w3.org/2001/XMLSchema#',
  tid: M_LD.tid,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

export class MeldEncoder {
  private /*readonly*/ ctx: ActiveContext;
  private readonly ready: Promise<unknown>;

  constructor(
    readonly domain: string,
    readonly rdf: RdfFactory) {
    this.ready = activeCtx(new DomainContext(domain, OPERATION_CONTEXT))
      .then(ctx => this.ctx = ctx);
  }

  async initialise() {
    await this.ready;
  }

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

  jsonFromTriples = (triples: Triple[]): any => {
    const json = SubjectGraph.fromRDF(triples, { ctx: this.ctx });
    // Recreates JSON-LD compaction behaviour
    return json.length == 0 ? {} : json.length == 1 ? json[0] : json;
  }

  triplesFromJson = (json: any): Triple[] =>
    [...new SubjectQuads('graph', this.ctx, this.rdf).quads(json)];
  
  triplesFromBuffer = (enc: JsonBuffer): Triple[] =>
    this.triplesFromJson(MeldEncoder.jsonFromBuffer(enc));
  
  bufferFromTriples = (triples: Triple[]): JsonBuffer =>
    MeldEncoder.bufferFromJson(this.jsonFromTriples(triples));

  static bufferFromJson(json: any): JsonBuffer {
    const stringified = JSON.stringify(json);
    return stringified.length > COMPRESS_THRESHOLD_BYTES ?
      gzipSync(stringified) : stringified;
  }

  static jsonFromBuffer(enc: JsonBuffer): any {
    if (typeof enc != 'string')
      enc = gunzipSync(enc).toString();
    return JSON.parse(enc);
  }
}
