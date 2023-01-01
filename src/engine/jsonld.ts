// Ambient module declarations being re-exported
/// <reference path="../types/jsonld.ts" />
/// <reference path="../types/jsonld-context.ts" />
/// <reference path="../types/jsonld-util.ts" />
/// <reference path="../types/jsonld-url.ts" />
/// <reference path="../types/jsonld-types.ts" />

import { Context, ExpandedTermDefinition, Iri, Options, processContext } from '@m-ld/jsonld';
import { compactIri as _compactIri } from '@m-ld/jsonld/lib/compact';
import { compareValues as _compareValues } from '@m-ld/jsonld/lib/util';
import {
  ActiveContext, expandIri, getContextValue, getInitialContext
} from '@m-ld/jsonld/lib/context';
import { isAbsolute } from '@m-ld/jsonld/lib/url';
import { isBoolean, isDouble, isNumber, isObject, isString } from '@m-ld/jsonld/lib/types';
import {
  ExpandedTermDef, isReference, isSet, isValueObject, isVocabReference
} from '../jrql-support';
import { array } from '../util';
import { XS } from '../ns';

export { hasProperty, hasValue } from '@m-ld/jsonld/lib/util';
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

export interface JsonldTermDefiner {
  getTermDetail(key: string, type: keyof ExpandedTermDef): string | null;
}

export interface JsonldCompacter extends JsonldTermDefiner {
  compactIri(
    iri: Iri,
    options?: Options.CompactIri & { vocab?: boolean }
  ): string;
}

export interface JsonldExpander extends JsonldTermDefiner {
  expandTerm(
    value: string,
    options?: Options.Expand & { vocab?: boolean }
  ): Iri;
}

/**
 * Type-compatible with MeldContext
 */
export class JsonldContext implements JsonldCompacter, JsonldExpander {
  static initial() {
    return new JsonldContext(getInitialContext({}));
  }

  static active(context: Context, options?: Options.DocLoader) {
    return this.initial().next(context, options);
  }

  static NONE: JsonldCompacter = {
    compactIri: (iri: Iri) => iri,
    getTermDetail: () => null
  };

  private constructor(
    private readonly ctx: ActiveContext
  ) {}

  async next(context?: Context, options?: Options.DocLoader): Promise<JsonldContext> {
    return context != null ? processContext(this.ctx, context, options ?? {})
      .then(ctx => new JsonldContext(ctx)) : this;
  }

  expandTerm(
    value: string,
    options?: Options.Expand & { vocab?: boolean }
  ): Iri {
    return expandIri(this.ctx, value, {
      base: true, vocab: options?.vocab
    }, options ?? {});
  }

  compactIri(
    iri: Iri,
    options?: Options.CompactIri & { vocab?: boolean }
  ) {
    return _compactIri({
      activeCtx: this.ctx, iri, ...options,
      ...options?.vocab ? { relativeTo: { vocab: options.vocab } } : null
    });
  }

  getTermDetail(key: string, type: keyof ExpandedTermDef): string | null {
    return getContextValue(this.ctx, key, <keyof ExpandedTermDefinition>type);
  }
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
  if (isObject(v)) {
    if (('@id' in v) && Object.keys(v).length > 1)
      return { '@id': (v as { '@id': string })['@id'] };
    else if (('@vocab' in v) && Object.keys(v).length > 1)
      return { '@vocab': (v as { '@vocab': string })['@vocab'] };
  }
  return v;
}

/**
 * Maps the canonical JSON-LD property value to some target type.
 *
 * @param property the property
 * @param value the JSON-LD value (raw values, value objects, references).
 * Subjects are minimised to references.
 * @param fn the map function
 * @param ctx the JSON-LD context, to establish the type
 * @param interceptRaw grabs raw string values before they are canonicalised
 * @todo reconcile with JsProperty & JsType
 */
export function mapValue<T>(
  property: string | null,
  value: any,
  fn: (
    value: string,
    type: '@id' | '@vocab' | Iri | '@none',
    language?: string
  ) => T,
  { ctx, interceptRaw }: {
    ctx?: JsonldExpander,
    interceptRaw?: (value: string) => T | undefined
  } = {}
): T {
  value = minimiseValue(value);
  if (typeof value == 'string') {
    const raw = interceptRaw?.(value);
    if (raw != null)
      return raw;
  } else if (isReference(value)) {
    return interceptRaw?.(value['@id']) ??
      fn(ctx ? ctx.expandTerm(value['@id']) : value['@id'], '@id');
  } else if (isVocabReference(value)) {
    return interceptRaw?.(value['@vocab']) ??
      fn(ctx ? ctx.expandTerm(value['@vocab'], { vocab: true }) : value['@vocab'], '@vocab');
  }
  let type: string | null = null, language: string | null = null;
  if (isValueObject(value)) {
    if (value['@type'])
      type = ctx ? ctx.expandTerm(value['@type']) : value['@type'];
    language = value['@language'] ?? null;
    value = value['@value'];
  }
  if (type == null && property != null && ctx != null)
    type = ctx.getTermDetail(property, '@type');

  if (typeof value == 'string') {
    // Note, we must be in a value object, so do not intercept a raw here.
    if (property === '@type' || type === '@id' || type === '@vocab') {
      const vocab = property === '@type' || type === '@vocab';
      return fn(ctx ? ctx.expandTerm(value, { vocab }) : value, vocab ? '@vocab' : '@id');
    }
    if (property != null && ctx != null)
      language = ctx.getTermDetail(property, '@language');
    if (language != null)
      return fn(value, XS.string, language);
    if (type === XS.double)
      value = canonicalDouble(parseFloat(value));
  } else if (isBoolean(value)) {
    value = value.toString();
    type ??= XS.boolean;
  } else if (isNumber(value)) {
    if (isDouble(value)) {
      value = canonicalDouble(value);
      type ??= XS.double;
    } else {
      value = value.toFixed(0);
      type ??= XS.integer;
    }
  } else {
    throw new TypeError(`Value is not a JSON-LD atom: ${value}`);
  }
  return fn(value, type ?? '@none', language ?? undefined);
}