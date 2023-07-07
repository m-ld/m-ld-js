// Ambient module declarations being re-exported
/// <reference path="../types/jsonld.ts" />
/// <reference path="../types/jsonld-context.ts" />
/// <reference path="../types/jsonld-util.ts" />
/// <reference path="../types/jsonld-url.ts" />
/// <reference path="../types/jsonld-types.ts" />

import { Context, ExpandedTermDefinition, Iri, Options, processContext } from '@m-ld/jsonld';
import { compactIri, compactValue } from '@m-ld/jsonld/lib/compact';
import { compareValues as _compareValues } from '@m-ld/jsonld/lib/util';
import {
  ActiveContext, expandIri, getContextValue, getInitialContext
} from '@m-ld/jsonld/lib/context';
import { isAbsolute } from '@m-ld/jsonld/lib/url';
import { isBoolean, isDouble, isNumber, isString } from '@m-ld/jsonld/lib/types';
import {
  ExpandedTermDef, isReference, isSet, isSubjectObject, isValueObject, isVocabReference
} from '../jrql-support';
import { array } from '../util';
import { RDF, XS } from '../ns';

export { clone, hasProperty, hasValue } from '@m-ld/jsonld/lib/util';
export { isAbsolute } from '@m-ld/jsonld/lib/url';
export { isBoolean, isDouble, isNumber, isString } from '@m-ld/jsonld/lib/types';
export { ActiveContext };

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
  compactValue(property: Iri, value: any): any;
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
  static active(context: Context, options?: Options.DocLoader) {
    return new JsonldContext().next(context, options);
  }

  static NONE: JsonldCompacter = {
    compactIri: iri => iri,
    compactValue: (property, value) => value,
    getTermDetail: () => null
  };

  constructor(
    protected readonly ctx = getInitialContext({})
  ) {}

  protected withCtx(ctx: ActiveContext): this {
    return new JsonldContext(ctx) as this;
  }

  async next(context?: Context, options?: Options.DocLoader): Promise<this> {
    return context != null ? processContext(this.ctx, context, options ?? {})
      .then(ctx => this.withCtx(ctx)) : this;
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
    return compactIri({
      activeCtx: this.ctx, iri, ...options,
      ...options?.vocab ? { relativeTo: { vocab: options.vocab } } : null
    });
  }

  compactValue(property: Iri, value: any) {
    return compactValue({
      activeCtx: this.ctx, activeProperty: property, value
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
  return value.toExponential(15)
    .replace(/(\d)0*e\+?/, '$1E');
}

export function minimiseValue(v: any) {
  if (isSubjectObject(v)) {
    if ('@id' in v)
      return { '@id': (v as { '@id': string })['@id'] };
    else if ('@vocab' in v)
      return { '@vocab': (v as { '@vocab': string })['@vocab'] };
  }
  return v;
}

export function minimiseValues(v: any[]) {
  const minimised = v.map(minimiseValue);
  return minimised.length === 1 ? minimised[0] : minimised;
}

/**
 * Maps the canonical JSON-LD property value to some target type.
 *
 * @param property the property, or `null` if unknown
 * @param value the JSON-LD value (raw values, value objects, references).
 * Subjects are minimised to references.
 * @param ctx the JSON-LD context, to establish the type
 * @todo reconcile with JsProperty & JsType
 */
export function expandValue(
  property: string | null,
  value: any,
  ctx?: JsonldExpander
): {
  raw: any,
  canonical: string, // Canonical expanded or lexical value
  type: '@id' | '@vocab' | Iri | '@none',
  language?: string,
  id?: string // Indicates a value object if not-null, may be blank
} {
  value = minimiseValue(value);
  if (isReference(value)) {
    value = value['@id'];
    const canonical = ctx?.expandTerm(value) ?? value;
    return { raw: value, canonical, type: '@id' };
  } else if (isVocabReference(value)) {
    value = value['@vocab'];
    const canonical = ctx?.expandTerm(value, { vocab: true }) ?? value;
    return { raw: value, canonical, type: '@vocab' };
  }
  let canonical: string | undefined,
    type: string | undefined,
    language: string | undefined,
    id: string | undefined;
  if (isValueObject(value)) {
    id = value['@id'] ?? '';
    if (value['@type'])
      type = ctx?.expandTerm(value['@type']) ?? value['@type'];
    language = value['@language'];
    value = value['@value'];
  }
  if (type == null && property != null && ctx != null)
    type = ctx.getTermDetail(property, '@type') ?? undefined;
  if (type === '@json') {
    return {
      id, raw: value, type: RDF.JSON,
      get canonical() { return JSON.stringify(value); }
    };
  } else if (value instanceof Uint8Array) {
    return {
      id, raw: value, type: XS.base64Binary,
      get canonical() { return Buffer.from(value).toString('base64'); }
    };
  } else if (typeof value == 'string') {
    if (property === '@type' || type === '@id' || type === '@vocab') {
      const vocab = property === '@type' || type === '@vocab';
      canonical = ctx?.expandTerm(value, { vocab }) ?? value;
      return { raw: value, canonical, type: vocab ? '@vocab' : '@id', id };
    }
    if (property != null && ctx != null)
      language = ctx.getTermDetail(property, '@language') ?? undefined;
    if (language != null)
      return { raw: value, canonical: value, type: XS.string, language, id };
    if (type === XS.double)
      canonical = canonicalDouble(parseFloat(value));
    canonical ??= value;
    type ??= XS.string;
  } else if (isBoolean(value)) {
    canonical = value.toString();
    type ??= XS.boolean;
  } else if (isNumber(value)) {
    if (isDouble(value)) {
      canonical = canonicalDouble(value);
      type ??= XS.double;
    } else {
      canonical = value.toFixed(0);
      type ??= XS.integer;
    }
  }
  canonical ??= '';
  type ??= '@none';
  return { raw: value, canonical, type, language, id };
}