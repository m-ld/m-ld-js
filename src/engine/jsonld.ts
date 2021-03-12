import { DataFactory, Quad } from 'rdf-js';
import { cloneQuad } from './quads';
import { toRDF, Options, processContext } from 'jsonld';
import { Context, Iri } from 'jsonld/jsonld-spec';
import { getInitialContext, expandIri, ActiveContext } from 'jsonld/lib/context';
import { compactIri as _compactIri } from 'jsonld/lib/compact';

export * from 'jsonld/lib/util';
export * from 'jsonld/lib/context';

export async function jsonToRdf(json: any, rdf: Required<DataFactory>): Promise<Quad[]> {
  const quads = await toRDF(json) as Quad[];
  // jsonld produces quad members without equals
  return quads.map(quad => cloneQuad(quad, rdf));
}

export function expandTerm(value: string, ctx: ActiveContext,
  options?: Options.Expand & { vocab?: boolean }): Iri {
  return expandIri(ctx, value, { base: true, vocab: options?.vocab }, options ?? {});
}

export function compactIri(iri: Iri, ctx?: ActiveContext, options?: Options.CompactIri): string {
  return ctx != null ? _compactIri({ activeCtx: ctx, iri, ...options }) : iri;
}

export async function activeCtx(ctx: Context, options?: Options.DocLoader): Promise<ActiveContext> {
  return processContext(getInitialContext({}), ctx, options ?? {});
}

/**
 * Gets all of the values for a subject's property as an array.
 *
 * @param subject the subject.
 * @param property the property.
 *
 * @return all of the values for a subject's property as an array.
 */
export function getValues(subject: { [key: string]: any }, property: string): Array<any> {
  return [].concat(subject[property] ?? []);
}

export function canonicalDouble(value: number) {
  return value.toExponential(15).replace(/(\d)0*e\+?/, '$1E');
}
