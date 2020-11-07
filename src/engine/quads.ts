import { Quad, Term, Literal } from 'rdf-js';
import { namedNode, defaultGraph, variable, blankNode, literal, quad as newQuad } from '@rdfjs/data-model';
import { fromRDF, toRDF } from 'jsonld';
import { IndexMap, IndexSet } from "./indices";

export type Triple = Omit<Quad, 'graph'>;

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

export function rdfToJson(quads: Quad[]): Promise<any> {
  // Using native types to avoid unexpected value objects
  return fromRDF(quads, { useNativeTypes: true });
}

export function jsonToRdf(json: any): Promise<Quad[]> {
  // jsonld produces quad members without equals
  return toRDF(json).then((quads: Quad[]) => quads.map(cloneQuad));
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