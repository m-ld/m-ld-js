import { Url } from '@m-ld/jsonld';
import {
  Construct, Describe, Reference, Subject, SubjectProperty, SubjectPropertyObject, Value
} from '../jrql-support';
import { JRQL } from '../ns';
import { isArray, isNaturalNumber, setAtPath, trimTail } from './util';
import { SubjectPropertyValues } from '../subjects';
import { JsonldCompacter } from './jsonld';
import { Triple } from './quads';
import validDataUrl = require('valid-data-url');

export enum JrqlMode {
  /**
   * querying
   * - variables to match allowed
   * - inline constraints (filters) allowed
   * - infer variable for missing IRI
   * - lists can be arrays or hashes
   * - list items are raw or slots
   */
  match,
  /**
   * loading new information
   * - variables (to be populated from a match) allowed
   * - inline constraints (binds) allowed
   * - infer skolems for missing Subject IDs
   * - lists can be arrays or hashes
   * - list items are raw or slots
   */
  load,
  /**
   * strict mode, e.g. updates:
   * - no variables
   * - no inline constraints
   * - do not infer anything
   * - missing Subject IDs are blank
   * - lists are hashes
   * - all list contents are slots
   */
  graph,
  /**
   * same as `graph` but
   * - data in typed literals are serialised to/from JSON
   */
  serial
}

/**
 * Allows variables if mode is not `graph`, and disallows list sub-items (as for
 * multiple-item inserts) if mode is `match`. If the list index is a string, it
 * is the variable name.
 */
export function *listItems(
  list: SubjectPropertyObject,
  mode = JrqlMode.graph
): IterableIterator<[ListIndex | string, SubjectPropertyObject]> {
  if (typeof list === 'object') {
    // This handles arrays as well as hashes
    for (let [indexKey, item] of Object.entries(list)) {
      if (mode === JrqlMode.graph || mode === JrqlMode.serial) {
        yield *subItems(list, toIndexNumber(indexKey, 'strict'), item);
      } else {
        // Provided index is either a variable (string) or an index number
        const index = JRQL.matchVar(indexKey) ?? toIndexNumber(indexKey, 'strict');
        if (typeof index == 'string' || mode === JrqlMode.match)
          // Definitely a variable if a string
          yield [index, item];
        else
          // Check for inserting multiple sub-items at one index
          yield *subItems(list, index, item);
      }
    }
  } else {
    // Singleton list item at position zero
    yield [[0], list];
  }
}

function *subItems(
  list: SubjectPropertyObject,
  index: ListIndex,
  item: SubjectPropertyObject
): IterableIterator<[ListIndex, SubjectPropertyObject]> {
  const [topIndex, subIndex] = index;
  if (subIndex == null && isArray(item) && !isArray(list))
    // Object.entries skips empty array positions
    for (let subIndex in item)
      yield [[topIndex, Number(subIndex)], item[subIndex]];
  else
    yield [index, item];
}

/**
 * @param subject
 * @param property
 * @param object
 * @param createList subject['@list'] is an object by default (in case sparse),
 * sub-index is always an array if present
 */
export function addPropertyObject(
  subject: Subject,
  property: SubjectProperty,
  object: Value | Value[],
  createList = () => ({})
): Subject {
  if (typeof property == 'string') {
    const spv = SubjectPropertyValues.for(subject, property);
    if (isArray(object))
      spv.insert(...object);
    else
      spv.insert(object);
  } else {
    setAtPath(subject,
      // Trim out an empty/undefined tail
      trimTail(property).map(String), object,
      path => path.length == 1 ? createList() : []);
  }
  return subject;
}

export type ListIndex = [number, number?];

export function toIndexNumber(
  indexKey: any, strict: 'strict'): ListIndex;
export function toIndexNumber(
  indexKey: any, strict?: 'strict'): ListIndex | undefined;
export function toIndexNumber(
  indexKey: any, strict?: 'strict'): ListIndex | undefined {
  if (indexKey != null && indexKey !== '') {
    if (isNaturalNumber(indexKey)) // ℕ
      return [indexKey];
    switch (typeof indexKey) {
      case 'string':
        return toIndexNumber(
          indexKey.startsWith('data') ?
            dataUrlData(indexKey, 'application/mld-li') : // 'data:,ℕ' or 'data:,ℕ,ℕ'
            indexKey.includes(',') ?
              indexKey.split(',').map(Number) : // 'ℕ,ℕ'
              Number(indexKey), // 'ℕ'
          strict
        );
      case 'object': // [ℕ,ℕ]
        if (isArray(indexKey) &&
          indexKey.length == 2 &&
          indexKey.every(isNaturalNumber))
          return indexKey as [number, number];
    }
  }
  if (strict)
    throw new Error(`List index ${indexKey} is not natural`);
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

export function toIndexDataUrl(index: ListIndex | string): Url {
  if (typeof index == 'string')
    throw new Error('Cannot generate a variable data URL');
  return `data:application/mld-li,${trimTail(index).map(i => i?.toFixed(0)).join(',')}`;
}

export function constructSubject(id: string, properties: SubjectProperty[]) {
  return properties.reduce<Subject>((construct, property) =>
    addPropertyObject(construct, property, '?'), { '@id': id });
}

export function constructProperties(id: string, properties: SubjectProperty[]): Construct {
  return { '@construct': constructSubject(id, properties) };
}

export function describeId(id: string): Describe {
  return { '@describe': id };
}

export function getContextType(
  property: SubjectProperty,
  ctx: JsonldCompacter
): string | null {
  return typeof property == 'string' && ctx != null ?
    ctx.getTermDetail(property, '@type') : null;
}

/**
 * A reference triple carries a blank node identifier
 */
export type RefTriple = Triple & Reference;

export function isRefTriple(triple: Triple): triple is RefTriple {
  return '@id' in triple;
}
