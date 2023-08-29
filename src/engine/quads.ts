import type { Bindings, DataFactory, NamedNode, Quad, Quad_Object, Term, Variable } from 'rdf-js';
import { DataFactory as RdfDataFactory } from 'rdf-data-factory';
import { IndexMap, IndexSet } from './indices';
import { Binding, QueryableRdfSource } from '../rdfjs-support';
import { Prefixes } from 'quadstore';
import { Datatype } from '../api';
import { Iri } from '@m-ld/jsonld';
import { uuid } from '../util';
import { clone } from './util';

export type Triple = Omit<Quad, 'graph'>;
export type TriplePos = 'subject' | 'predicate' | 'object';

export type {
  DefaultGraph, Quad, Term, DataFactory, NamedNode, Source as QuadSource,
  Quad_Subject, Quad_Predicate, Quad_Object, Bindings, Literal
} from 'rdf-js';

/** Utility interfaces shared with quadstore */
export { Prefixes };

export type TypedData<T = unknown> = {
  readonly type: Datatype<T>,
  readonly data: T
};

export type JsonData = {
  readonly type: undefined,
  readonly data: unknown
};

export type QuadData<T = unknown> = TypedData<T> | JsonData

declare module './quads' {
  export interface Quad {
    before?: Quad_Object;
  }

  export interface Literal {
    typed?: QuadData;
  }
}

export interface LiteralTriple extends Triple {
  object: Literal;
}

export function isLiteralTriple(triple: Triple): triple is LiteralTriple {
  return triple.object.termType === 'Literal';
}

/** Note `datatype.value === typed.type['@id']` */
export interface TypedLiteral extends Literal {
  typed: QuadData;
}

export function isTypedLiteral(term: Term): term is TypedLiteral {
  return term.termType === 'Literal' && term.typed != null;
}

export interface TypedTriple extends LiteralTriple {
  object: TypedLiteral;
}

export function isTypedTriple(triple: Triple): triple is TypedTriple {
  return isTypedLiteral(triple.object);
}

export function unTypeTriple<T extends Triple>(triple: T): Omit<T, 'typed'> {
  if (isTypedTriple(triple)) {
    const { typed: _, ...untypedLiteral } = triple.object;
    return clone(triple, {
      ...triple, object: clone(triple.object, untypedLiteral)
    });
  }
  return triple;
}

export abstract class QueryableRdfSourceProxy implements QueryableRdfSource {
  match: QueryableRdfSource['match'] = (...args) => this.src.match(...args);
  // @ts-ignore - TS can't cope with overloaded query method
  query: QueryableRdfSource['query'] = (...args) => this.src.query(...args);
  countQuads: QueryableRdfSource['countQuads'] = (...args) => this.src.countQuads(...args);

  protected abstract get src(): QueryableRdfSource;
}

export class TripleMap<T, Q extends Triple = Triple> extends IndexMap<Q, T> {
  protected getIndex(key: Triple): string {
    return tripleIndexKey(key);
  }
}

export class QuadSet<Q extends Quad = Quad> extends IndexSet<Q> {
  protected construct(quads?: Iterable<Q>): QuadSet<Q> {
    return new QuadSet<Q>(quads);
  }

  protected getIndex(quad: Q): string {
    return quadIndexKey(quad);
  }
}

export function tripleKey(triple: Triple, prefixes?: Prefixes): string {
  const compact = (term: NamedNode) =>
    prefixes ? prefixes.compactIri(term.value) : term.value;
  if (triple.subject.termType !== 'NamedNode' || triple.predicate.termType !== 'NamedNode')
    throw new TypeError('Triple key requires named node subject & predicate');
  const key = [compact(triple.subject), compact(triple.predicate)];
  switch (triple.object.termType) {
    case 'Variable':
      break; // Supports range matching
    case 'NamedNode':
      key.push(triple.object.termType, compact(triple.object));
      break;
    case 'Literal':
      key.push(triple.object.termType, triple.object.value);
      key.push(compact(triple.object.datatype));
      if (triple.object.language)
        key.push(triple.object.language);
      break;
    default:
      throw new TypeError('Triple key requires named node or literal object');
  }
  // JSON.stringify is ~50% faster than .map(replace(delim)).join(delim)
  return JSON.stringify(key).slice(1, -1);
}

export function tripleFromKey(key: string, rdf: DataFactory, prefixes?: Prefixes): Triple {
  const [subject, predicate, objectTermType, objectValue, datatype, language] =
    JSON.parse(`[${key}]`);
  const expand = (term: string) => prefixes ? prefixes.expandTerm(term) : term;
  let object: Triple['object'];
  switch (objectTermType) {
    case 'Literal':
      object = rdf.literal(objectValue,
        language ?? rdf.namedNode(expand(datatype)));
      break;
    case 'NamedNode':
      object = rdf.namedNode(expand(objectValue));
      break;
    default:
      throw new TypeError('Triple key requires named node or literal object');
  }
  return rdf.quad(
    rdf.namedNode(expand(subject)),
    rdf.namedNode(expand(predicate)),
    object
  );
}

export function tripleIndexKey(triple: Triple) {
  const tik = <Triple & { _tik: string }>triple;
  if (tik._tik == null)
    tik._tik = tripleKey(triple);
  return tik._tik;
}

export function quadIndexKey(quad: Quad) {
  const qik = <Quad & { _qik: string }>quad;
  if (qik._qik == null)
    qik._qik = `${JSON.stringify(quad.graph.value)},${tripleIndexKey(quad)}`;
  return qik._qik;
}

export function canPosition<P extends TriplePos>(pos: P, value?: Term): value is Quad[P] {
  if (!value)
    return false;
  // Subjects and Predicate don't allow literals
  if ((pos == 'subject' || pos == 'predicate') && value.termType == 'Literal')
    return false;
  // Predicates don't allow blank nodes
  return !(pos == 'predicate' && value.termType == 'BlankNode');

}

export function inPosition<P extends TriplePos>(pos: P, value?: Term): Quad[P] {
  if (canPosition(pos, value))
    return value;
  else
    throw new Error(`${value} cannot be used in ${pos} position`);
}

export class RdfFactory extends RdfDataFactory {
  constructor(
    readonly base: Iri | undefined
  ) {
    super();
  }

  /**
   * Generates a new skolemization IRI. The dataset base is allowed to be
   * `undefined` but the function will throw a `TypeError` if it is.
   * @see https://www.w3.org/TR/rdf11-concepts/#h3_section-skolemization
   */
  skolem = () => this.namedNode(
    new URL(`/.well-known/genid/${uuid()}`, this.base).href);

  /** @inheritDoc */
  literal(value: string, languageOrDatatype?: string | NamedNode): Literal;
  /**
   * Creates a typed literal with a specific custom datatype. If the datatype is
   * of class `Datatype`, then the data is the Datatype's native Data type.
   * Otherwise, the data is the relevant (but unknown) Datatype's JSON
   * representation.
   */
  literal(
    value: string,
    datatype: NamedNode | Datatype,
    data: unknown
  ): TypedLiteral;
  /** @internal */
  literal(
    value: string,
    languageOrDatatype?: string | NamedNode | Datatype,
    data?: unknown
  ): Literal {
    let typed: QuadData | undefined;
    if (typeof languageOrDatatype == 'object' && '@id' in languageOrDatatype) {
      typed = { data, type: languageOrDatatype };
      languageOrDatatype = this.namedNode(languageOrDatatype['@id']);
    } else if (data != null) {
      typed = { data, type: undefined };
    }
    const literal = super.literal(value, languageOrDatatype);
    if (typed != null)
      Object.assign(literal, { typed });
    return literal;
  }
}

export function asQueryVar(variable: Variable) {
  return `?${variable.value}`;
}

export function toBinding(bindings: Bindings): Binding {
  const binding: Binding = {};
  for (let [variable, term] of bindings)
    binding[asQueryVar(variable)] = term;
  return binding;
}
