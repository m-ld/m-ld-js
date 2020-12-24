import { Quad } from 'rdf-js';
import { fromRDF, toRDF, Options, processContext } from 'jsonld';
import { cloneQuad } from './quads';
import { Context, Iri } from 'jsonld/jsonld-spec';
import { getInitialContext, expandIri, ActiveContext } from 'jsonld/lib/context';
import { compactIri as _compactIri } from 'jsonld/lib/compact';
export { Options } from 'jsonld';
export { ActiveContext } from 'jsonld/lib/context';

export function rdfToJson(quads: Iterable<Quad>): Promise<any> {
  // Using native types to avoid unexpected value objects
  return fromRDF(quads, { useNativeTypes: true });
}

export async function jsonToRdf(json: any): Promise<Quad[]> {
  const quads = await toRDF(json) as Quad[];
  // jsonld produces quad members without equals
  return quads.map(cloneQuad);
}

export function expandTerm(value: string, ctx: ActiveContext, options?: Options.Expand): Iri {
  return expandIri(ctx, value, { base: true }, options ?? {});
}

export function compactIri(iri: Iri, ctx: ActiveContext, options?: Options.CompactIri): string {
  return _compactIri({ activeCtx: ctx, iri, ...options });
}

export async function activeCtx(ctx: Context, options?: Options.DocLoader): Promise<ActiveContext> {
  return await processContext(getInitialContext({}), ctx, options ?? {});
}
