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
 *
 * > 🚧 *If you have a requirement for an unsupported pattern, please
 * > [contact&nbsp;us](mailto:info@m-ld.io) to discuss your use-case.* You can
 * > browse the full **json-rql** syntax at
 * > [json-rql.org](http://json-rql.org/).
 *
 * @see https://json-rql.org/interfaces/pattern.html
 */
export type Pattern = jrql.Pattern;
/**
 * A reference to a Subject. Used to disambiguate an IRI from a plain string.
 * Unless a custom [Context](#context) is used for the clone, all references
 * will use this format.
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
 * An JSON-LD expanded term definition, as part of a domain {@link Context}.
 * @see https://json-rql.org/interfaces/expandedtermdef.html
 */
export type ExpandedTermDef = jrql.ExpandedTermDef;
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
/**
 * @see https://json-rql.org/#value
 */
export type Value = jrql.Value;
// Utility functions
/** @internal */
export const isValueObject = jrql.isValueObject;
/** @internal */
export const isReference = jrql.isReference;

/**
 * Result declaration of a {@link Select} query.
 * Use of `'*'` specifies that all variables in the query should be returned.
 */
export type Result = '*' | Variable | Variable[];

/**
 * A resource, represented as a JSON object, that is part of the domain data.
 * @see https://json-rql.org/interfaces/subject.html
 */
export interface Subject extends Pattern {
  // No support for inline filters, @lists or @sets
  /**
   * The unique identity of the subject in the domain.
   * > 🚧 *Subjects strictly need not be identified with an `@id`, but the data
   * > of such Subjects cannot be retrieved with a simple {@link Describe}
   * > query.*
   */
  '@id'?: Iri | Variable;
  /**
   * The type of the subject, as an IRI or set of IRIs. (`@type` is actually
   * shorthand for the RDF property
   * [rdf:type](http://www.w3.org/1999/02/22-rdf-syntax-ns#type).)
   */
  '@type'?: Iri | Variable | Iri[] | Variable[];
  /**
   * Specifies a graph edge, that is, a mapping from the `@id` of this subject
   * to one or more values, which may also express constraints.
   */
  [key: string]: Value | Value[] | Context | undefined;
}

/** @internal */
export function isSubject(p: Pattern): p is Subject {
  return !isGroup(p) && !isQuery(p);
}

/**
 * Used to express a group of patterns to match.
 * @see https://json-rql.org/interfaces/group.html
 */
export interface Group extends Pattern {
  /**
   * Specifies a Subject or an array of Subjects to match.
   */
  '@graph'?: Subject | Subject[];
  /**
   * Specifies a set of alternative Subjects (or sets of Subjects) to match.
   */
  '@union'?: (Subject | Subject[])[];
}

/** @internal */
export function isGroup(p: Pattern): p is Group {
  return '@graph' in p || '@union' in p;
}

/**
 * A sub-type of Pattern which matches data using a `@where` clause.
 * @see https://json-rql.org/interfaces/query.html
 */
export interface Query extends Pattern {
  // No support for @values
  /**
   * The data pattern to match, as a set of subjects or a group. Variables are
   * used as placeholders to capture matching properties and values in the domain.
   */
  '@where'?: Subject | Subject[] | Group;
}

/** @internal */
export function isQuery(p: Pattern): p is Query {
  return isRead(p) || isUpdate(p);
}

/**
 * A query type that reads data from the domain.
 */
export interface Read extends Query {
  // No support for @limit, @orderBy etc.
}

/** @internal */
export function isRead(p: Pattern): p is Read {
  return isDescribe(p) || isSelect(p);
}

/**
 * A simple means to get the properties of a specific subject, or a set of
 * subjects matching some `@where` clause.
 * @see https://json-rql.org/interfaces/describe.html
 */
export interface Describe extends Read {
  /**
   * Specifies a single Variable or Iri to return. Each matched value for the
   * identified variable will be output in some suitable expanded format, such
   * as a subject with its top-level properties.
   */
  '@describe': Iri | Variable;
}

/** @internal */
export function isDescribe(p: Pattern): p is Describe {
  return '@describe' in p;
}

/**
 * A query type that returns values for variables in the query. The subjects
 * streamed in the query result will have the form:
 * ```
 * {
 *   "?var1": <value>
 *   "?var2": <value>
 *   ...
 * }
 * ```
 * @see https://json-rql.org/interfaces/select.html
 */
export interface Select extends Read {
  /**
   * A declaration of the selection of variables that will be returned.
   */
  '@select': Result;
}

/** @internal */
export function isSelect(p: Pattern): p is Select {
  return '@select' in p;
}

/**
 * A pattern to update the properties of matching subjects in the domain.
 * @see https://json-rql.org/interfaces/update.html
 */
export interface Update extends Query {
  /**
   * Subjects with properties to be inserted into the domain. If variables are
   * used, then a `@where` clause must be supplied to provide matching values.
   */
  '@insert'?: Subject | Subject[];
  /**
   * Subjects with properties to be deleted from the domain. Variables can be
   * used without a `@where` clause, to match any value.
   */
  '@delete'?: Subject | Subject[];
}

/** @internal */
export function isUpdate(p: Pattern): p is Update {
  return '@insert' in p || '@delete' in p;
}
