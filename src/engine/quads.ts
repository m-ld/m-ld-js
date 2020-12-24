import { Quad, Term, Literal } from 'rdf-js';
// FIXME: Make this a data factory field of some class
import { namedNode, defaultGraph, variable, blankNode, literal, quad as newQuad } from '@rdfjs/data-model';
import { IndexMap, IndexSet } from "./indices";

export type Triple = Omit<Quad, 'graph'>;
export type TriplePos = 'subject' | 'predicate' | 'object';

export namespace rdf {
  export const $id = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#';
  export const type = namedNode(`${$id}type`);
  export const Statement = namedNode(`${$id}Statement`);
  export const subject = namedNode(`${$id}subject`);
  export const predicate = namedNode(`${$id}predicate`);
  export const object = namedNode(`${$id}object`);
  export const List = namedNode(`${$id}List`);
  export const first = namedNode(`${$id}first`);
  export const rest = namedNode(`${$id}rest`);
  export const nil = namedNode(`${$id}nil`);
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

function tripleIndexKey(triple: Triple): string {
  return tripleKey(triple).join('^');
}

function quadIndexKey(quad: Quad): string {
  return [quad.graph.value].concat(tripleKey(quad)).join('^');
}

export function cloneQuad(quad: Quad): Quad {
  return newQuad(
    cloneTerm(quad.subject),
    cloneTerm(quad.predicate),
    cloneTerm(quad.object),
    cloneTerm(quad.graph));
}

export function cloneTerm<T extends Term>(term: T): T {
  switch (term.termType) {
    case 'Quad':
      return <T>cloneQuad(<Quad>term);
    case 'BlankNode':
      return <T>blankNode(term.value);
    case 'DefaultGraph':
      return <T>defaultGraph();
    case 'Literal':
      const lit = <Literal>term;
      return <T>literal(term.value, lit.language != null ? lit.language : cloneTerm(lit.datatype));
    case 'NamedNode':
      return <T>namedNode(term.value);
    case 'Variable':
      return <T>variable(term.value);
  }
}

export function canPosition<P extends TriplePos>(pos: P, value: Term): value is Quad[P] {
  // Subjects and Predicate don't allow literals
  if ((pos == 'subject' || pos == 'predicate') && value.termType == 'Literal')
    return false;
  // Predicates don't allow blank nodes
  if (pos == 'predicate' && value.termType == 'BlankNode')
    return false;
  return true;
}

export function inPosition<P extends TriplePos>(pos: P, value: Term): Quad[P] {
  if (canPosition(pos, value))
    return value;
  else
    throw new Error(`${value} cannot be used in ${pos} position`);
}
