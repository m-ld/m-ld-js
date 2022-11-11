import type { Bindings, DataFactory, NamedNode, Quad, Term } from 'rdf-js';
import { IndexMap, IndexSet } from './indices';
import { Binding, QueryableRdfSource } from '../rdfjs-support';

export type Triple = Omit<Quad, 'graph'>;
export type TriplePos = 'subject' | 'predicate' | 'object';

export type {
  DefaultGraph, Quad, Term, DataFactory, NamedNode, Source as QuadSource,
  Quad_Subject, Quad_Predicate, Quad_Object, Bindings, Literal
} from 'rdf-js';

export abstract class QueryableRdfSourceProxy implements QueryableRdfSource {
  match: QueryableRdfSource['match'] = (...args) => this.src.match(...args);
  // @ts-ignore - TS can't cope with overloaded query method
  query: QueryableRdfSource['query'] = (...args) => this.src.query(...args);
  countQuads: QueryableRdfSource['countQuads'] = (...args) => this.src.countQuads(...args);

  protected abstract get src(): QueryableRdfSource;
}

export class TripleMap<T> extends IndexMap<Triple, T> {
  protected getIndex(key: Triple): string {
    return tripleIndexKey(key);
  }
}

export class QuadSet extends IndexSet<Quad> {
  protected construct(quads?: Iterable<Quad>): QuadSet {
    return new QuadSet(quads);
  }

  protected getIndex(quad: Quad): string {
    return quadIndexKey(quad);
  }
}

export type getTermValue = (term: Term) => string;

export function tripleKey(
  triple: Triple,
  value: getTermValue = term => term.value
): string {
  const key = [
    value(triple.subject),
    value(triple.predicate),
    triple.object.termType,
    value(triple.object)
  ];
  if (triple.object.termType === 'Literal') {
    key.push(value(triple.object.datatype));
    if (triple.object.language)
      key.push(triple.object.language);
  }
  // JSON.stringify is ~50% faster than .map(replace(delim)).join(delim)
  return JSON.stringify(key).slice(1, -1);
}

export function tripleIndexKey(triple: Triple, value?: getTermValue) {
  if (value != null) {
    // No caching if value function is specified
    return tripleKey(triple, value);
  } else {
    const tik = <Triple & { _tik: string }>triple;
    if (tik._tik == null)
      tik._tik = tripleKey(triple);
    return tik._tik;
  }
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

export interface RdfFactory extends Required<DataFactory> {
  /**
   * Generates a new skolemization IRI. The dataset base is allowed to be
   * `undefined` but the function will throw a `TypeError` if it is.
   * @see https://www.w3.org/TR/rdf11-concepts/#h3_section-skolemization
   */
  skolem?(): NamedNode;
}

export function toBinding(bindings: Bindings): Binding {
  const binding: Binding = {};
  for (let [variable, term] of bindings)
    binding[`?${variable.value}`] = term;
  return binding;
}
