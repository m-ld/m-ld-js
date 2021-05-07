import { EncodedOperation, UUID } from '.';
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
import { gzip as gzipCb, gunzip as gunzipCb, InputType } from 'zlib';
import { CausalOperation } from './ops';
const gzip = (input: InputType) => new Promise<Buffer>((resolve, reject) =>
  gzipCb(input, (err, buf) => err ? reject(err) : resolve(buf)));
const gunzip = (input: InputType) => new Promise<Buffer>((resolve, reject) =>
  gunzipCb(input, (err, buf) => err ? reject(err) : resolve(buf)));
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

export class MeldOperation extends CausalOperation<Triple, TreeClock> {
  static fromOperation = async (enc: MeldEncoding, 
    from: number, time: TreeClock,
    deletes: TriplesTids, inserts: TriplesTids): Promise<MeldOperation> => {
    const jsons = [deletes, inserts]
      .map((triplesTids, i) => {
        // Encoded inserts are only reified if fused
        if (i === 1 && from === time.ticks)
          return triplesTids.map(([triple]) => triple);
        else
          return enc.reifyTriplesTids(triplesTids);
      })
      .map(enc.jsonFromTriples);
    const [delEnc, insEnc] = await Promise.all(
      jsons.map(json => MeldEncoding.bufferFromJson(json)));
    const encoded: EncodedOperation = [2, from, time.toJson(), delEnc, insEnc];
    return new MeldOperation(from, time, deletes, inserts, encoded, jsons);
  }

  static fromEncoded = async (enc: MeldEncoding,
    encoded: EncodedOperation): Promise<MeldOperation> => {
    const [ver] = encoded;
    if (ver < 2)
      throw new Error(`Encoded operation version ${ver} not supported`);
    let [, from, timeJson, delEnc, insEnc] = encoded;
    const jsons = await Promise.all([delEnc, insEnc].map(MeldEncoding.jsonFromBuffer));
    const [delTriples, insTriples] = jsons.map(enc.triplesFromJson);
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
    return new MeldOperation(from, time, deletes, inserts, encoded, jsons);
  }

  private constructor(
    from: number,
    time: TreeClock,
    deletes: TriplesTids,
    inserts: TriplesTids,
    /**
     * Serialisation of triples is not required to be normalised. For any m-ld
     * operation, there are many possible serialisations. An operation carries its
     * serialisation with it, for journaling and hashing.
     */
    readonly encoded: EncodedOperation,
    readonly jsons: any) {
    super(from, time, deletes, inserts);
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

export class MeldEncoding {
  private /*readonly*/ ctx: ActiveContext;
  private readonly ready: Promise<unknown>;

  constructor(
    readonly domain: string,
    readonly rdf: RdfFactory) {
    this.ready = activeCtx(new DomainContext(domain, OPERATION_CONTEXT))
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

  jsonFromTriples = (triples: Triple[]): any => {
    const json = SubjectGraph.fromRDF(triples, { ctx: this.ctx });
    // Recreates JSON-LD compaction behaviour
    return json.length == 0 ? {} : json.length == 1 ? json[0] : json;
  }

  triplesFromJson = (json: any): Triple[] =>
    [...new SubjectQuads('graph', this.ctx, this.rdf).quads(json)];

  static async bufferFromJson(json: any): Promise<Buffer | string> {
    const stringified = JSON.stringify(json);
    return stringified.length > COMPRESS_THRESHOLD_BYTES ?
      gzip(stringified) : stringified;
  }

  static async jsonFromBuffer(enc: string | Buffer): Promise<any> {
    if (typeof enc != 'string')
      enc = (await gunzip(enc)).toString();
    return JSON.parse(enc);
  }
}
