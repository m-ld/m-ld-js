import type { BaseQuad, Quad, Source, Stream, Term } from 'rdf-js';
import type { Algebra } from 'sparqlalgebrajs';
import type { EventEmitter } from 'events';

/**
 * This module defines the RDFJS and other extended RDF JS community style
 * methods supported by JrqlGraph.
 */

/**
 * Bound variable values from a SPARQL projection.
 * Keys include the variable prefix `?`.
 * @category RDFJS
 */
export interface Binding {
  [key: string]: Term;
}

/**
 * Abstract stream of any type; implicit supertype of an RDFJS
 * [Stream](https://rdf.js.org/stream-spec/#stream-interface)
 * @category RDFJS
 */
// Using type not interface so typedoc does not document EventEmitter
export type BaseStream<T> = EventEmitter & {
  read: () => T | null;
}

/**
 * SPARQL query methods
 * @category RDFJS
 */
export interface QueryableRdf<Q extends BaseQuad = Quad> {
  query(query: Algebra.Construct): Stream<Q>;
  query(query: Algebra.Describe): Stream<Q>;
  query(query: Algebra.Project): BaseStream<Binding>;
  query(query: Algebra.Distinct): BaseStream<Binding>;
}

/**
 * A [Source](https://rdf.js.org/stream-spec/#source-interface) which is able to
 * count quads, as an optimisation for query engines
 *
 * @see https://github.com/comunica/comunica/tree/master/packages/actor-init-sparql-rdfjs#optimization
 * @category RDFJS
 */
export interface CountableRdf {
  countQuads(...args: Parameters<Source['match']>): Promise<number>;
}

/**
 * Rollup interface for an RDF source that can answer SPARQL queries
 * @category RDFJS
 */
export interface QueryableRdfSource<Q extends BaseQuad = Quad>
  extends Source<Q>, QueryableRdf<Q>, CountableRdf {
}