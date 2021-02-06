import { Quad, Term, Literal, DataFactory } from 'rdf-js';
import { IndexMap, IndexSet } from "./indices";
import { memoise } from './util';

export type Triple = Omit<Quad, 'graph'>;
export type TriplePos = 'subject' | 'predicate' | 'object';

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
    case 'Literal': return [
      triple.subject.value,
      triple.predicate.value,
      triple.object.termType,
      triple.object.value || '',
      triple.object.datatype.value || '',
      triple.object.language || ''
    ];
    default: return [
      triple.subject.value,
      triple.predicate.value,
      triple.object.termType,
      triple.object.value
    ];
  }
}

const tripleIndexKey = memoise((triple: Triple) =>
  tripleKey(triple).join('^'));

const quadIndexKey = memoise((quad: Quad) => 
  [quad.graph.value].concat(tripleKey(quad)).join('^'));

export function cloneQuad(quad: Quad, rdf: Required<DataFactory>): Quad {
  return rdf.quad(
    cloneTerm(quad.subject, rdf),
    cloneTerm(quad.predicate, rdf),
    cloneTerm(quad.object, rdf),
    cloneTerm(quad.graph, rdf));
}

export function cloneTerm<T extends Term>(term: T, rdf: Required<DataFactory>): T {
  switch (term.termType) {
    case 'Quad':
      return <T>cloneQuad(<Quad>term, rdf);
    case 'BlankNode':
      return <T>rdf.blankNode(term.value);
    case 'DefaultGraph':
      return <T>rdf.defaultGraph();
    case 'Literal':
      const lit = <Literal>term;
      return <T>rdf.literal(term.value, lit.language != null ?
        lit.language : cloneTerm(lit.datatype, rdf));
    case 'NamedNode':
      return <T>rdf.namedNode(term.value);
    case 'Variable':
      return <T>rdf.variable(term.value);
  }
}

export function canPosition<P extends TriplePos>(pos: P, value?: Term): value is Quad[P] {
  if (!value)
    return false;
  // Subjects and Predicate don't allow literals
  if ((pos == 'subject' || pos == 'predicate') && value.termType == 'Literal')
    return false;
  // Predicates don't allow blank nodes
  if (pos == 'predicate' && value.termType == 'BlankNode')
    return false;
  return true;
}

export function inPosition<P extends TriplePos>(pos: P, value?: Term): Quad[P] {
  if (canPosition(pos, value))
    return value;
  else
    throw new Error(`${value} cannot be used in ${pos} position`);
}
