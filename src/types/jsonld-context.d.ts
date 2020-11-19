declare module 'jsonld/lib/context' {
  import { Context, Iri } from 'jsonld/jsonld-spec';
  import { Options } from 'jsonld';

  // Using the 'protected' field to prevent type mistakes
  type ActiveContext = Context & { protected: {} };

  function getInitialContext(options: { processingMode?: boolean }): ActiveContext;
  function expandIri(
    activeCtx: Context,
    value: string,
    relativeTo: { vocab?: boolean, base?: boolean },
    options: Options.Common): Iri;
}
