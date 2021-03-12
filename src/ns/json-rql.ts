import { Iri } from 'jsonld/jsonld-spec';
import { Variable } from '../jrql-support';

const PN_CHARS_BASE = `[A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD]|[\uD800-\uDB7F][\uDC00-\uDFFF]`;
const PN_CHARS_U = `(?:${PN_CHARS_BASE}|_)`;
const VARNAME = `(?:${PN_CHARS_U}|[0-9])(?:${PN_CHARS_U}|[0-9]|\u00B7|[\u0300-\u036F\u203F-\u2040])*`;

export const $base = 'http://json-rql.org/';

export const item = 'http://json-rql.org/#item'; // Slot item property
export const index = `${$base}#index`; // Entailed slot index property
export const blank = `${$base}#blank`; // Temporary use for processing

export type SubVarName = 'listKey' | 'slotId';

export function subVar(varName: string, subVarName: SubVarName) {
  return `?${varName}__${subVarName}`;
}

const MATCH_SUBVARNAME = new RegExp(`^\\?(${VARNAME})__(listKey|slotId)$`);
export function matchSubVarName(fullVarName: string): [string, SubVarName] | [] {
  const match = MATCH_SUBVARNAME.exec(fullVarName);
  return match != null ? [match[1], match[2] as SubVarName] : [];
}

const MATCH_VAR = new RegExp(`^\\?(${VARNAME})$`);
export function matchVar(token: string): string | undefined {
  return token === '?' ? '' : MATCH_VAR.exec(token)?.[1];
}

// "Hidden" variables are used for JSON-LD processing

export function hiddenVar(name: string): Iri {
  return `${$base}var#${name}`;
};

const MATCH_HIDDEN_VAR = new RegExp(`^${hiddenVar('(' + VARNAME + ')')}$`);
export function matchHiddenVar(value: string): string | undefined {
  return MATCH_HIDDEN_VAR.exec(value)?.[1];
}
