import * as jrql from 'json-rql';
import { Iri } from 'jsonld/jsonld-spec';

/**
 * This module defines the sub-types of json-rql supported by JrqlGraph.
 */

// Re-exporting unchanged types
/**
 * A m-ld transaction is a **json-rql** pattern, which represents a data read or
 * a data update. Supported pattern types are:
 * - {@link Describe}
 * - {@link Select}
 * - {@link Group} or {@link Subject} (the shorthand way to insert data)
 * - {@link Update} (the longhand way to insert or delete data)
 * @see https://json-rql.org/interfaces/pattern.html
 */
export type Pattern = jrql.Pattern;
/**
 * @see https://json-rql.org/#reference
 */
export type Reference = jrql.Reference;
/**
 * A JSON-LD context for some JSON content such as a {@link Subject}. **m-ld**
 * does not require the use of a context, as plain JSON data will be stored
 * in the context of the domain. However in advanced usage, such as for
 * integration with existing systems, it may be useful to provide other context
 * for shared data.
 * @see https://json-rql.org/interfaces/context.html
 */
export type Context = jrql.Context;
/**
 * A query variable, prefixed with "?", used as a placeholder for some value in
 * a query, for example:
 * ```json
 * {
 *   "@select": "?name",
 *   "@where": { "employeeNo": 7, "name": "?name" }
 * }
 * ```
 * @see https://json-rql.org/#variable
 */
export type Variable = jrql.Variable;
export type Value = jrql.Value;
// Utility functions
export { isValueObject, isReference } from 'json-rql';

export type Result = '*' | Variable | Variable[];

export interface Subject extends jrql.Subject {
  // No support for inline filters, @lists or @sets
  [key: string]: Value | Value[] | Context | undefined;
}

export function isSubject(p: Pattern): p is Subject {
  return !isGroup(p) && !isQuery(p);
}

export interface Group extends Pattern {
  '@graph'?: Subject | Subject[];
  '@union'?: Subject[];
}

export function isGroup(p: Pattern): p is Group {
  return '@graph' in p || '@union' in p;
}

export interface Query extends Pattern {
  // No support for @values
  '@where'?: Subject | Subject[] | Group;
}

export function isQuery(p: Pattern): p is Query {
  return isRead(p) || isUpdate(p);
}

export interface Read extends Query {
  // No support for @limit, @orderBy etc.
}

export function isRead(p: Pattern): p is Read {
  return isDescribe(p) || isSelect(p);
}

export interface Describe extends Read {
  '@describe': Iri | Variable;
}

export function isDescribe(p: Pattern): p is Describe {
  return '@describe' in p;
}

export interface Select extends Read {
  '@select': Result;
}

export function isSelect(p: Pattern): p is Select {
  return '@select' in p;
}

export interface Update extends Query {
  '@insert'?: Subject | Subject[];
  '@delete'?: Subject | Subject[];
}

export function isUpdate(p: Pattern): p is Update {
  return '@insert' in p || '@delete' in p;
}
