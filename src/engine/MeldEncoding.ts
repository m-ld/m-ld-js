import { BufferEncoding } from '.';
import { flatten, lazy, MsgPack } from './util';
import { Context, ExpandedTermDef, Reference } from '../jrql-support';
import { Iri } from 'jsonld/jsonld-spec';
import { RdfFactory, Triple } from './quads';
import { ActiveContext, activeCtx, compactIri, expandTerm } from './jsonld';
import { M_LD, RDF, XS } from '../ns';
import { SubjectGraph } from './SubjectGraph';
import { SubjectQuads } from './SubjectQuads';
import { MeldError } from './MeldError';
// TODO: Switch to fflate. Node.js zlib uses Pako in the browser
import { gunzipSync, gzipSync } from 'zlib';

const COMPRESS_THRESHOLD_BYTES = 1024;

export type UUID = string;

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
      this['@vocab'] = new URL('/#', this['@base']).href;
  }
}

/**
 * TODO: re-sync with Java
 * @see m-ld/m-ld-core/src/main/java/org/m_ld/MeldResource.java
 */
const OPERATION_CONTEXT = {
  rdf: RDF.$base,
  xs: XS.$base,
  tid: M_LD.tid,
  s: { '@type': '@id', '@id': 'rdf:subject' },
  p: { '@type': '@id', '@id': 'rdf:predicate' },
  o: 'rdf:object'
};

/**
 * A reference triple carries a blank node identifier
 */
export type RefTriple = Triple & Reference;

export type RefTriplesTids = [RefTriple, UUID[]][];

export class MeldEncoder {
  private /*readonly*/ ctx: ActiveContext;
  private readonly ready: Promise<unknown>;

  constructor(
    readonly domain: string,
    readonly rdf: RdfFactory
  ) {
    this.ready = activeCtx(new DomainContext(domain, OPERATION_CONTEXT))
      .then(ctx => this.ctx = ctx);
  }

  async initialise() {
    await this.ready;
  }

  private name = lazy(name => this.rdf.namedNode(name));

  compactIri = (iri: Iri) => compactIri(iri, this.ctx);
  expandTerm = (value: string) => expandTerm(value, this.ctx);

  identifyTriple = (triple: Triple): RefTriple =>
    ({ '@id': `_:${this.rdf.blankNode().value}`, ...triple });

  identifyTriplesTids = (triplesTids: Iterable<[Triple, UUID[]]>): RefTriplesTids =>
    [...triplesTids].map(([triple, tids]) => [this.identifyTriple(triple), tids]);

  reifyTriplesTids(triplesTids: RefTriplesTids): Triple[] {
    return flatten(triplesTids.map(([triple, tids]) => {
      if (!triple['@id'].startsWith('_:'))
        throw new TypeError(`Triple ${triple['@id']} is not a blank node`);
      const rid = this.rdf.blankNode(triple['@id'].slice(2));
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

  static unreifyTriplesTids(reifications: Triple[]): RefTriplesTids {
    return Object.values(reifications.reduce((rids, reification) => {
      const rid = reification.subject.value; // Blank node value
      // Add the blank node IRI prefix to a new triple
      let [triple, tids] = rids[rid] || [{ '@id': `_:${rid}` }, []];
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
    }, {} as { [rid: string]: [RefTriple, UUID[]] }));
  }

  jsonFromTriples = (triples: Triple[]): object => {
    const json = SubjectGraph.fromRDF(triples, { ctx: this.ctx });
    // Recreates JSON-LD compaction behaviour
    return json.length == 0 ? {} : json.length == 1 ? json[0] : json;
  };

  triplesFromJson = (json: object): Triple[] =>
    [...new SubjectQuads('graph', this.ctx, this.rdf).quads(<any>json)];

  triplesFromBuffer = (encoded: Buffer, encoding: BufferEncoding[]): Triple[] =>
    this.triplesFromJson(MeldEncoder.jsonFromBuffer(encoded, encoding));

  bufferFromTriples = (triples: Triple[]): [Buffer, BufferEncoding[]] =>
    MeldEncoder.bufferFromJson(this.jsonFromTriples(triples));

  static bufferFromJson(json: object): [Buffer, BufferEncoding[]] {
    const packed = MsgPack.encode(json);
    return packed.length > COMPRESS_THRESHOLD_BYTES ?
      [gzipSync(packed), [BufferEncoding.MSGPACK, BufferEncoding.GZIP]] :
      [packed, [BufferEncoding.MSGPACK]];
  }

  static jsonFromBuffer<T>(encoded: Buffer, encoding: BufferEncoding[]): T {
    let result: any = encoded;
    for (let i = encoding.length - 1; i >= 0; i--) {
      switch (encoding[i]) {
        case BufferEncoding.JSON:
          result = JSON.parse(result.toString());
          break;
        case BufferEncoding.MSGPACK:
          result = MsgPack.decode(result);
          break;
        case BufferEncoding.GZIP:
          result = gunzipSync(result);
          break;
        default:
          throw new MeldError('Bad update', `Unrecognised encoding ${encoding[i]}`);
      }
    }
    return result;
  }
}
