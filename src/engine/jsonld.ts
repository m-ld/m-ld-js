// Ambient module declarations being re-exported
/// <reference path="../types/jsonld.ts" />
/// <reference path="../types/jsonld-context.ts" />
/// <reference path="../types/jsonld-util.ts" />
/// <reference path="../types/jsonld-url.ts" />
/// <reference path="../types/jsonld-types.ts" />

import { Context, Iri, Options, processContext } from '@m-ld/jsonld';
import { compactIri as _compactIri } from '@m-ld/jsonld/lib/compact';
import { compareValues as _compareValues } from '@m-ld/jsonld/lib/util';
import { ActiveContext, expandIri, getInitialContext } from '@m-ld/jsonld/lib/context';
import { isAbsolute } from '@m-ld/jsonld/lib/url';
import { isObject } from '@m-ld/jsonld/lib/types';
import { isSet } from '../jrql-support';
import { array } from '../util';

export { hasProperty, hasValue } from '@m-ld/jsonld/lib/util';
export { ActiveContext, getContextValue } from '@m-ld/jsonld/lib/context';
export { isAbsolute } from '@m-ld/jsonld/lib/url';
export { isBoolean, isDouble, isNumber, isString } from '@m-ld/jsonld/lib/types';

export function compareValues(v1: any, v2: any): boolean {
  const jsonldEqual = _compareValues(v1, v2);
  if (!jsonldEqual && typeof v1 == 'object' && typeof v2 == 'object') {
    if ('@vocab' in v1 && '@vocab' in v2)
      return v1['@vocab'] === v2['@vocab'];
    else if ('@id' in v1 && '@vocab' in v2)
      return isAbsolute(v1['@id']) && v1['@id'] === v2['@vocab'];
    else if ('@vocab' in v1 && '@id' in v2)
      return isAbsolute(v1['@vocab']) && v1['@vocab'] === v2['@id'];
  }
  return jsonldEqual;
}

export function expandTerm(
  value: string,
  ctx: ActiveContext,
  options?: Options.Expand & { vocab?: boolean }
): Iri {
  return expandIri(ctx, value, {
    base: true, vocab: options?.vocab
  }, options ?? {});
}

export function compactIri(
  iri: Iri,
  ctx?: ActiveContext,
  options?: Options.CompactIri & { vocab?: boolean }
): string {
  return ctx != null ? _compactIri({
    activeCtx: ctx, iri, ...options,
    ...options?.vocab ? { relativeTo: { vocab: options.vocab } } : null
  }) : iri;
}

export async function activeCtx(
  context: Context,
  options?: Options.DocLoader
): Promise<ActiveContext> {
  return nextCtx(initialCtx(), context, options);
}

export function initialCtx(): ActiveContext {
  return getInitialContext({});
}

export async function nextCtx(ctx: ActiveContext, context?: Context,
  options?: Options.DocLoader
): Promise<ActiveContext> {
  return context != null ? processContext(ctx, context, options ?? {}) : ctx;
}

/**
 * Gets all of the values for a subject's property as an array.
 *
 * @param subject the subject.
 * @param property the property.
 * @return all of the values for a subject's property as an array.
 */
export function getValues(subject: { [key: string]: any }, property: string): Array<any> {
  return asValues(subject[property]);
}

/**
 * Normalises the value of a JSON-LD object entry to an array of values.
 *
 * Note that Lists are treated as Subjects.
 *
 * @param value the value.
 * @return the value as an array of values.
 */
export function asValues(value: any) {
  return value == null ? [] : array(isSet(value) ? value['@set'] : value);
}

export function canonicalDouble(value: number) {
  return value.toExponential(15).replace(/(\d)0*e\+?/, '$1E');
}

export function minimiseValue(v: any) {
  if (isObject(v) && ('@id' in v) && Object.keys(v).length > 1)
    return { '@id': (v as { '@id': string })['@id'] };
  else
    return v;
}