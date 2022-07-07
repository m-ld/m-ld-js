import type {
  AlgebraQueryable, AlgebraSparqlQueryable, AllMetadataSupport, QueryAlgebraContext,
  QuerySourceContext, QueryStringContext, SparqlResultSupport
} from '@rdfjs/types';

import type { Quadstore } from 'quadstore';

import type { Algebra } from 'sparqlalgebrajs';

type StringContext = Omit<
  QueryStringContext & QuerySourceContext<Quadstore>,
  'source' | 'sources' | 'destination'
>

type AlgebraContext = Omit<
  QueryAlgebraContext & QuerySourceContext<Quadstore>,
  'source' | 'sources' | 'destination'
>

interface Engine extends AlgebraQueryable<Algebra.BaseOperation, AllMetadataSupport, AlgebraContext>
  , AlgebraSparqlQueryable<Algebra.BaseOperation, SparqlResultSupport, AlgebraContext> {}

export declare class Engine {
  constructor(store: Quadstore);
}

declare const __engine: unknown;
