/**
 * This declaration file is stored as .ts so that it is emitted
 */

declare module 'jsonld/lib/compact' {
  import { Iri } from 'jsonld/jsonld-spec';
  import { ActiveContext } from 'jsonld/lib/context';
  import { Options } from 'jsonld';

  function compactIri(opts: {
    activeCtx: ActiveContext,
    iri: Iri
  } & Options.CompactIri): string;
}
