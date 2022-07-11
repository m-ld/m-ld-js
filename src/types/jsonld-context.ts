/**
 * This declaration file is stored as .ts so that it is emitted
 */

declare module 'jsonld/lib/context' {
  import { Context, Iri } from 'jsonld/jsonld-spec';
  import { ExpandedTermDefinition, Options } from 'jsonld';

  // Using the 'protected' field to prevent type mistakes
  type ActiveContext = Context & { protected: {} };

  function getInitialContext(options: { processingMode?: boolean }): ActiveContext;

  function isKeyword(v: any): boolean;

  function expandIri(
    activeCtx: Context,
    value: string,
    relativeTo: { vocab?: boolean, base?: boolean },
    options: Options.Expand): Iri;
  
  function getContextValue(
    ctx: Context,
    key: string,
    type: keyof ExpandedTermDefinition): string | null;

  function getContextValue(
    ctx: Context,
    key: null,
    type: '@context'): undefined;
}
