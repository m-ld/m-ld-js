import { Url } from 'jsonld/jsonld-spec';
import { SubjectPropertyObject, Subject, SubjectProperty, Value } from '../jrql-support';
import { jrql } from '../ns';
import { addValue } from './jsonld';
import { setAtPath, trimTail, isNaturalNumber } from './util';
import validDataUrl = require('valid-data-url');
import { includeValues } from '..';
const { isArray } = Array;

/**
 * - `match`: querying
 *   - variables to match
 *   - infer variable for missing IRI
 *   - lists can be arrays or hashes
 *   - list items are raw or slots
 * - `load`: loading new information
 *   - variables from a match
 *   - infer skolems
 *   - infer slots
 *   - lists can be arrays or hashes
 *   - list items are raw or slots
 * - `graph`: strict mode, e.g. updates:
 *   - no variables
 *   - do not infer anything
 *   - lists are hashes
 *   - all list contents are slots
 */
export type JrqlMode = 'match' | 'load' | 'graph';

/**
 * Allows variables if mode is not `graph`, and disallows list sub-items (as for
 * multiple-item inserts) if mode is `match`. If the list index is a string, it
 * is the variable name.
 */
export function* listItems(
  list: SubjectPropertyObject, mode: JrqlMode = 'graph'):
  IterableIterator<[ListIndex | string, SubjectPropertyObject]> {
  if (typeof list === 'object') {
    // This handles arrays as well as hashes
    for (let [indexKey, item] of Object.entries(list)) {
      if (mode === 'graph') {
        yield* subItems(list, toIndexNumber(indexKey, 'strict'), item);
      } else {
        // Provided index is either a variable (string) or an index number
        const index = jrql.matchVar(indexKey) ?? toIndexNumber(indexKey, 'strict');
        if (typeof index == 'string' || mode === 'match')
          // Definitely a variable if a string
          yield [index, item];
        else
          // Check for inserting multiple sub-items at one index
          yield* subItems(list, index, item);
      }
    }
  } else {
    // Singleton list item at position zero
    yield [[0], list];
  }
}

function* subItems(
  list: SubjectPropertyObject, index: ListIndex, item: SubjectPropertyObject):
  IterableIterator<[ListIndex, SubjectPropertyObject]> {
  const [topIndex, subIndex] = index;
  if (subIndex == null && isArray(item) && !isArray(list))
    // Object.entries skips empty array positions
    for (let subIndex in item)
      yield [[topIndex, Number(subIndex)], item[subIndex]];
  else
    yield [index, item];
}

/**
 * @param createList subject['@list'] is an object by default (in case sparse),
 * sub-index is always an array if present
 */
export function addPropertyObject(
  subject: Subject, property: SubjectProperty, object: Value,
  createList: () => object = () => ({})): Subject {
  if (typeof property == 'string') {
    includeValues(subject, property, object);
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
        return toIndexNumber(indexKey.startsWith('data') ?
          dataUrlData(indexKey, 'application/mld-li') : // 'data:,ℕ' or 'data:,ℕ,ℕ'
          indexKey.includes(',') ?
            indexKey.split(',').map(Number) : // 'ℕ,ℕ'
            Number(indexKey), strict); // 'ℕ'
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
