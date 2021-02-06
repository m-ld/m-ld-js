import { DataFactory, Quad } from 'rdf-js';
import { fromRDF, toRDF, Options, processContext } from 'jsonld';
import { cloneQuad } from './quads';
import { Context, Iri, Url } from 'jsonld/jsonld-spec';
import { getInitialContext, expandIri, ActiveContext } from 'jsonld/lib/context';
import { compactIri as _compactIri } from 'jsonld/lib/compact';
export { Options } from 'jsonld';
export { ActiveContext } from 'jsonld/lib/context';
import validDataUrl = require('valid-data-url');

export * from 'jsonld/lib/util';
export * from 'jsonld/lib/context';

export function rdfToJson(quads: Iterable<Quad>): Promise<any> {
  // Using native types to avoid unexpected value objects
  return fromRDF(quads, { useNativeTypes: true });
}

export async function jsonToRdf(json: any, rdf: Required<DataFactory>): Promise<Quad[]> {
  const quads = await toRDF(json) as Quad[];
  // jsonld produces quad members without equals
  return quads.map(quad => cloneQuad(quad, rdf));
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

export function dataUrlData(url: Url, ...contentTypes: string[]): string | undefined {
  const match = url.trim().match(validDataUrl.regex);
  const data = match?.[match.length - 1];
  if (data != null) {
    const contentType = match?.[1]?.split(';')[0]?.toLowerCase() || 'text/plain';
    if (contentTypes.includes(contentType))
      return data;
  }
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
