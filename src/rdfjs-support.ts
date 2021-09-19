import { BaseQuad, Quad, Source, Stream, Term } from 'rdf-js';
import { Algebra } from 'sparqlalgebrajs';
import { EventEmitter } from 'events';

/**
 * This module defines the RDF/JS and other extended RDF JS community style methods supported by
 * JrqlGraph.
 */

/**
 * Bound variable values from a SPARQL projection.
 * Keys include the variable prefix `?`.
 */
export interface Binding {
  [key: string]: Term;
}

/**
 * Abstract stream of any type; implicit supertype of an RDF/JS {@link Stream}
 */
export interface BaseStream<T> extends EventEmitter {
  read(): T | null;
}

/**
 * SPARQL query methods
 */
export interface QueryableRdf<Q extends BaseQuad = Quad> {
  query(query: Algebra.Construct): Stream<Q>;
  query(query: Algebra.Describe): Stream<Q>;
  query(query: Algebra.Project): BaseStream<Binding>;
}

/**
 * A {@link Source} which is able to count quads, as an optimisation for query engines
 * @see https://github.com/comunica/comunica/tree/master/packages/actor-init-sparql-rdfjs#optimization
 */
export interface CountableRdf {
  countQuads(...args: Parameters<Source['match']>): Promise<number>;
}

/**
 * Rollup interface for an RDF source hat can answer SPARQL queries
 */
export interface QueryableRdfSource<Q extends BaseQuad = Quad>
  extends Source<Q>, QueryableRdf<Q>, CountableRdf {
}