import type { DataFactory, NamedNode, Quad, Term } from 'rdf-js';
import { IndexMap, IndexSet } from './indices';
import { memoise } from './util';
import { QueryableRdfSource } from '../rdfjs-support';

export type Triple = Omit<Quad, 'graph'>;
export type TriplePos = 'subject' | 'predicate' | 'object';

export type {
  DefaultGraph, Quad, Term, DataFactory, NamedNode, Source as QuadSource,
  Quad_Subject, Quad_Predicate, Quad_Object
} from 'rdf-js';

export class QueryableRdfSourceProxy implements QueryableRdfSource {
  readonly match: QueryableRdfSource['match'];
  readonly query: QueryableRdfSource['query'];
  readonly countQuads: QueryableRdfSource['countQuads'];

  constructor(readonly src: QueryableRdfSource) {
    this.match = src.match.bind(src);
    this.query = src.query.bind(src);
    this.countQuads = src.countQuads.bind(src);
  }
}

export class QuadMap<T> extends IndexMap<Quad, T> {
  protected getIndex(key: Quad): string {
    return quadIndexKey(key);
  }
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

export function tripleKey(triple: Triple): string[] {
  switch (triple.object.termType) {
    case 'Literal':
      return [
        triple.subject.value,
        triple.predicate.value,
        triple.object.termType,
        triple.object.value || '',
        triple.object.datatype.value || '',
        triple.object.language || ''
      ];
    default:
      return [
        triple.subject.value,
        triple.predicate.value,
        triple.object.termType,
        triple.object.value
      ];
  }
}

export const tripleIndexKey = memoise((triple: Triple) =>
  tripleKey(triple).join('^'));

export const quadIndexKey = memoise((quad: Quad) =>
  [quad.graph.value].concat(tripleKey(quad)).join('^'));

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