import * as jrql from 'json-rql';
import { Iri } from 'jsonld/jsonld-spec';

/**
 * This module defines the sub-types of json-rql supported by JrqlGraph.
 */

// Re-exporting unchanged types
/**
 * A m-ld transaction is a **json-rql** pattern, which represents a data read or
 * a data write. Supported pattern types are:
 * - {@link Describe}
 * - {@link Select}
 * - {@link Group} or {@link Subject} (the shorthand way to insert data)
 * - {@link Update} (the longhand way to insert or delete data)
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
export type Value = jrql.Atom | Subject | Reference;
/**
 * Used to express an ordered or unordered container of data.
 * @see https://json-rql.org/interfaces/container.html
 */
export type Container = List | Set;

/**
 * Used to express an ordered set of data. A List object is reified to a Subject
 * (unlike in JSON-LD) and so it has an @id, which can be set by the user.
 *
 * Note that this reification is only possible when using the `@list` keyword,
 * and not if the active context specifies `"@container": "@list"` for a
 * property, in which case the list itself is anonymous.
 * @see https://json-rql.org/interfaces/list.html
 */
export interface List extends Subject {
  '@list': Value | Value[];
}

/** @internal */
export function isList(value: Subject['any']): value is List {
  return typeof (value) === 'object' && '@list' in value;
}

/**
 * Used to express an unordered set of data and to ensure that values are always
 * represented as arrays.
 * @see https://json-rql.org/interfaces/set.html
 */
export interface Set {
  '@set': Value | Value[];
}

/** @internal */
export function isSet(value: Subject['any']): value is Set {
  return typeof (value) === 'object' && '@set' in value;
}

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
 * 
 * Examples:
 * 
 * *A subject with one property: "fred's name is Fred"*
 * ```json
 * {
 *   "@id": "fred",
 *   "name": "Fred"
 * }
 * ```
 * *A subject with a {@link Reference} property: "fred's wife is wilma"*
 * ```json
 * {
 *   "@id": "fred",
 *   "wife": { "@id": "wilma" }
 * }
 * ```
 * *A subject with another nested subject: "fred's wife is wilma, and her name is Wilma"*
 * ```json
 * {
 *   "@id": "fred",
 *   "wife": {
 *     "@id": "wilma",
 *     "name": "Wilma"
 *   }
 * }
 * ```
 *
 * @see https://json-rql.org/interfaces/subject.html
 */
export interface Subject extends Pattern {
  // No support for inline filters
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
   * to a set of one or more values.
   */
  [key: string]: Value | Value[] | Container | Context | undefined;
}

/** @internal */
export function isSubject(p: Pattern): p is Subject {
  return !isGroup(p) && !isQuery(p);
}

/**
 * Used to express a group of patterns to match, or a group of subjects to write
 * (when used as a transaction pattern).
 *
 * Examples:
 *
 * *Insert multiple subjects*
 * ```json
 * {
 *   "@graph": [
 *     {
 *       "@id": "fred",
 *       "name": "Fred"
 *     },
 *     {
 *       "@id": "wilma",
 *       "name": "Wilma"
 *     }
 *   ]
 * }
 * ```
 * *Delete all properties of subject `fred` **and** all properties of other
 * subjects that reference it*
 * ```json
 * {
 *   "@delete": [
 *     { "@id": "fred", "?prop1": "?value" },
 *     { "@id": "?id2", "?ref": { "@id": "fred" } }
 *   ],
 *   "@where": {
 *     "@union": [
 *       { "@id": "fred", "?prop1": "?value" },
 *       { "@id": "?id2", "?ref": { "@id": "fred" } }
 *     ]
 *   }
 * }
 * ```
 *
 * > Note that when used in a `@where` clause, a plain array can substitute for
 * > a Group, as follows:
 * >
 * > *Select combinations of subjects having a common property value*
 * > ```json
 * > {
 * >   "@select": ["id1", "id2"],
 * >   "@where": {
 * >     "@graph": [
 * >       { "@id": "id1", "name": "?name" },
 * >       { "@id": "id2", "name": "?name" }
 * >     ]
 * >   }
 * > }
 * > ```
 * > *is equivalent to:*
 * > ```json
 * > {
 * >   "@select": ["id1", "id2"],
 * >   "@where": [
 * >     { "@id": "id1", "name": "?name" },
 * >     { "@id": "id2", "name": "?name" }
 * >   ]
 * > }
 * > ```
 *
 * @see https://json-rql.org/interfaces/group.html
 */
export interface Group extends Pattern {
  /**
   * Specifies a Subject or an array of Subjects.
   * 
   * When resolving query solutions, 
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

/** @internal */
export function isWriteGroup(p: Pattern): p is Group {
  return '@graph' in p && !('@union' in p);
}

/**
 * A sub-type of Pattern which matches data using a `@where` clause.
 * @see https://json-rql.org/interfaces/query.html
 */
export interface Query extends Pattern {
  // No support for @values
  /**
   * The data pattern to match, as a set of subjects or a group. Variables are
   * used as placeholders to capture matching properties and values in the
   * domain.
   * 
   * Examples:
   * 
   * *Match a subject by its `@id`*
   * ```json
   * {
   *   ...
   *   "@where": { "@id": "fred" }
   * }
   * ```
   * *Match a subject where any property has a given value*
   * ```json
   * {
   *   ...
   *   "@where": {
   *     "@id": "?id",
   *     "?prop": "Bedrock"
   *   }
   * }
   * ```
   * *Match a subject with a given property, having any value*
   * ```json
   * {
   *   ...
   *   "@where": {
   *     "@id": "?id",
   *     "name": "?name"
   *   }
   * }
   * ```
   *
   * > The Javascript engine supports exact-matches for subject identities, properties and
   * > values. [Inline&nbsp;filters](https://json-rql.org/globals.html#inlinefilter)
   * > will be available in future.
   */
  '@where'?: Subject | Subject[] | Group;
}

/** @internal */
export function isQuery(p: Pattern): p is Query {
  return isRead(p) || isUpdate(p);
}

/**
 * A query pattern that reads data from the domain.
 */
export interface Read extends Query {
  // No support for @limit, @orderBy etc.
}

/** 
 * Determines if the given pattern will read data from the domain.
 */
export function isRead(p: Pattern): p is Read {
  return isDescribe(p) || isSelect(p);
}

/**
 * A query pattern that writes data to the domain. A write can be:
 * - A {@link Subject} (any JSON object not a Read, Group or Update).
 *   Interpreted as data to be inserted.
 * - A {@link Group} containing only a `@graph` key. Interpreted as containing
 *   the data to be inserted.
 * - An explicit {@link Update} with either an `@insert`, `@delete`, or both.
 *
 * Note that this type does not fully capture the details above. Use
 * {@link isWrite} to inspect a candidate pattern.
 */
export type Write = Subject | Group | Update;

/** 
 * Determines if the given pattern can probably be interpreted as a logical
 * write of data to the domain.
 *
 * This function is not exhaustive, and a pattern identified as a write can
 * still turn out to be illogical, for example if it contains an `@insert` with
 * embedded variables and no `@where` clause to bind them.
 *
 * Returns `true` if the logical write is a trivial no-op, such as `{}`,
 * `{ "@insert": {} }` or `{ "@graph": [] }`.
 * 
 * @see {@link Write}
 */
export function isWrite(p: Pattern): p is Write {
  return !isRead(p) && (isSubject(p) || isWriteGroup(p) || isUpdate(p));
}

/**
 * A simple means to get the properties of a specific subject, or a set of
 * subjects matching some `@where` clause.
 *
 * Examples:
 *
 * *Describe a specific subject whose `@id` is `fred`*
 * ```json
 * {
 *   "@describe": "fred"
 * }
 * ```
 * *Describe all subjects in the domain*
 * ```json
 * {
 *   "@describe": "?id",
 *   "@where": { "@id": "?id" }
 * }
 * ```
 * *Describe subjects with a property `age` of `40`*
 * ```json
 * {
 *   "@describe": "?id",
 *   "@where": {
 *     "@id": "?id",
 *     "age": 40
 *   }
 * }
 * ```
 * *Describe all subjects referenced by `fred` via any property*
 * ```json
 * {
 *   "@describe": "?id",
 *   "@where": {
 *     "@id": "fred",
 *     "?prop": { "@id": "?id" }
 *   }
 * }
 * ```
 * See the [`@where`](#_where) property for more examples of how to use a where
 * clause.
 *
 * @see https://json-rql.org/interfaces/describe.html
 */
export interface Describe extends Read {
  /**
   * Specifies a single Variable or Iri to return. Each matched value for the
   * identified variable will be output as a [Subject](/interfaces/subject.html)
   * with its top-level properties.
   */
  '@describe': Iri | Variable;
}

/** @internal */
export function isDescribe(p: Pattern): p is Describe {
  return '@describe' in p;
}

/**
 * A query pattern that returns values for variables in the query.
 *
 * The subjects streamed in the query result will have the form:
 * ```json
 * {
 *   "?var1": <value>
 *   "?var2": <value>
 *   ...
 * }
 * ```
 * Examples:
 * 
 * *Select the ids of subjects having a given name*
 * ```json
 * {
 *   "@select": "?id",
 *   "@where": {
 *     "@id": "?id",
 *     "name": "Wilma"
 *   }
 * }
 * ```
 * *Select the ids and names of all subjects*
 * ```json
 * {
 *   "@select": ["?id", "?value"],
 *   "@where": {
 *     "@id": "?id",
 *     "name": "?value"
 *   }
 * }
 * ```
 * See the [`@where`](#_where) property for more examples of how to use a where
 * clause.
 *
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
 * 
 * Examples:
 * 
 * *Delete a subject property*
 * ```json
 * {
 *   "@delete": {
 *     "@id": "fred",
 *     "name": "Fred"
 *   }
 * }
 * ```
 * *Delete a property, where another property has a value*
 * ```json
 * {
 *   "@delete": {
 *     "@id": "?id",
 *     "age": "?any"
 *   },
 *   "@where": {
 *     "@id": "?id",
 *     "name": "Fred",
 *     "age": "?any"
 *   }
 * }
 * ```
 * *Update a subject property*
 * ```json
 * {
 *   "@delete": {
 *     "@id": "fred",
 *     "name": "Fred"
 *   },
 *   "@insert": {
 *     "@id": "fred",
 *     "name": "Fred Flintstone"
 *   }
 * }
 * ```
 * *Replace all of a subject's properties*
 * ```json
 * {
 *   "@delete": {
 *     "@id": "fred",
 *     "?prop": "?value"
 *   },
 *   "@insert": {
 *     "@id": "fred",
 *     "age": 50,
 *     "name": "Fred Flintstone"
 *   }
 * }
 * ```
 *
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
