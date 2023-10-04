/**
 * This declaration file is stored as .ts so that it is emitted
 */

declare module '@m-ld/jsonld/lib/compact' {
  import { ActiveContext } from '@m-ld/jsonld/lib/context';
  import { Iri, Options } from '@m-ld/jsonld';

  function compactIri(opts: {
    activeCtx: ActiveContext,
    iri: Iri
  } & Options.CompactIri): string;

  function compactValue(opts: {
    activeCtx: ActiveContext,
    activeProperty: Iri,
    value: any,
    options?: Options.Compact
  }): any;
}
