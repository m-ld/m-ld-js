import * as jrql from 'json-rql';
import { Iri } from '@m-ld/jsonld';
import { isArray } from './engine/util';

/**
 * This module defines the sub-types of json-rql supported by JrqlGraph.
 */

// Re-exporting unchanged types
/**
 * An IRI (Internationalized Resource Identifier) within an RDF graph is a
 * Unicode string that conforms to the syntax defined in RFC 3987.
 * @see https://www.w3.org/TR/rdf11-concepts/#dfn-iri
 */
export { Iri };
/**
 * A m-ld transaction is a **json-rql** pattern, which represents a data read or
 * a data write. Supported pattern types are:
 * - {@link Describe}
 * - {@link Construct}
 * - {@link Select}
 * - {@link Group} or {@link Subject} (the shorthand way to insert data)
 * - {@link Update} (the longhand way to insert or delete data)
 *
 * @see [json-rql pattern](https://json-rql.org/interfaces/pattern.html)
 * @category json-rql
 */
export type Pattern = jrql.Pattern;
/**
 * A reference to a Subject. Used to disambiguate an IRI from a plain string.
 * Unless a custom [Context](#context) is used for the clone, all references
 * will use this format.
 *
 * This type is also used to distinguish identified subjects (with an `@id`
 * field) from anonymous ones (without an `@id` field).
 *
 * @see [json-rql reference](https://json-rql.org/#reference)
 * @category json-rql
 */
export type Reference = jrql.Reference;

/** @internal */
interface ReferenceConstructor {
  new(value: Reference): Reference;
}

/**
 * Constructor of references from references: used similarly to e.g. `Number`
 * @category json-rql
 */
export const Reference: ReferenceConstructor = class implements Reference {
  readonly '@id': Iri;
  constructor(value: Reference) {
    this['@id'] = value['@id'];
  }
};
/**
 * Like a {@link Reference}, but used for "vocabulary" references. These are relevant to:
 * - Subject properties: the property name is a vocabulary reference
 * - Subject `@type`: the type value is a vocabulary reference
 * - Any value for a property that has been defined as `@vocab` in the Context
 * @see https://www.w3.org/TR/json-ld/#default-vocabulary
 * @category json-rql
 */
export type VocabReference = { '@vocab': Iri };

/** @internal */
interface VocabReferenceConstructor {
  new(value: VocabReference): VocabReference;
}

/**
 * Constructor of vocab references from vocab references: used similarly to e.g. `Number`
 * @category json-rql
 */
export const VocabReference: VocabReferenceConstructor = class implements VocabReference {
  readonly '@vocab': Iri;
  constructor(value: VocabReference) {
    this['@vocab'] = value['@vocab'];
  }
};
/**
 * A JSON-LD context for some JSON content such as a {@link Subject}. **m-ld**
 * does not require the use of a context, as plain JSON data will be stored
 * in the context of the domain. However in advanced usage, such as for
 * integration with existing systems, it may be useful to provide other context
 * for shared data.
 * @see [json-rql context](https://json-rql.org/interfaces/context.html)
 * @category json-rql
 */
export type Context = jrql.Context;
/**
 * An JSON-LD expanded term definition, as part of a domain {@link Context}.
 * @see [json-rql expandedtermdef](https://json-rql.org/interfaces/expandedtermdef.html)
 * @category json-rql
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
 * @see [json-rql variable](https://json-rql.org/#variable)
 * @category json-rql
 */
export type Variable = jrql.Variable;
/**
 * A basic atomic value used as a concrete value or in a filter. Note that the
 * m-ld Javascript engine natively supports `Uint8Array` for binary data.
 * @see [json-rql atom](https://json-rql.org/#atom) @category json-rql
 * @category json-rql
 */
export type Atom =
  number | string | boolean | Uint8Array |
  Variable | ValueObject | Reference | VocabReference;

/**
 * @category json-rql
 * @see [json-rql value object](https://json-rql.org/interfaces/valueobject.html)
 */
export interface ValueObject {
  /**
   * Used to specify the data that is associated with a particular property in
   * the graph. Note that:
   * - in **json-rql** a `@value` will never be a Variable.
   * - **m-ld** allows a `@value` to be any JSON-serialisable type, to allow for
   * custom datatypes. If a value that is not a string, number or boolean is
   * used and there is no corresponding custom datatype, a TypeError may be
   * thrown.
   * @see [JSON-LD typed-values](https://w3c.github.io/json-ld-syntax/#typed-values)
   */
  '@value': any;
  /**
   * Used to set the data type of the typed value.
   * @see [JSON-LD typed-values](https://w3c.github.io/json-ld-syntax/#typed-values)
   * @see [JSON-LD type-coercion](https://w3c.github.io/json-ld-syntax/#type-coercion)
   */
  '@type'?: Iri;
  /**
   * Used to specify the language for a particular string value or the default
   * language of a JSON-LD document.
   */
  '@language'?: string;
  /**
   * The unique identity of the value in the domain. This is only used if the
   * value is _mutable_; in practice this applies only to shared data types.
   * @experimental
   */
  '@id'?: Iri;
}

/**
 * @see [json-rql value](https://json-rql.org/#value)
 * @category json-rql
 */
export type Value = Atom | Subject;
/**
 * The allowable types for a Subject property value, named awkwardly to avoid
 * overloading `Object`. Represents the "object" of a property, in the sense of
 * the object of discourse.
 * @see [json-rql SubjectPropertyObject](https://json-rql.org/#SubjectPropertyObject)
 * @category json-rql
 */
export type SubjectPropertyObject = Value | Container | SubjectPropertyObject[];
/**
 * Used to express an ordered or unordered container of data.
 * @see [json-rql container](https://json-rql.org/interfaces/container.html)
 * @category json-rql
 */
export type Container = List | Set;
/**
 * A stand-in for a Value used as a basis for filtering.
 * @see [json-rql expression](https://json-rql.org/globals.html#expression)
 * @category json-rql
 */
export type Expression = Atom | Constraint;
/** @internal */
export const operators: { [jrql: string]: { sparql: string | undefined } } = {
  ...jrql.operators,
  '@concat': { sparql: 'concat' },
  '@splice': { sparql: undefined }
};
/** @internal */
export type Operator = keyof typeof jrql.operators | '@concat' | '@splice';
/**
 * The expected type of the parameters to the `@splice` operator.
 * @see operators
 * @category json-rql
 */
export type TextSplice = [number, number, string?];
/** @internal */
export function isTextSplice(
  args: Expression | (Expression | undefined)[]
): args is TextSplice {
  if (isArray(args)) {
    const [index, deleteCount, content] = args;
    // noinspection SuspiciousTypeOfGuard
    if (typeof index == 'number' &&
      typeof deleteCount == 'number' &&
      (content == null || typeof content == 'string')) {
      return true;
    }
  }
  return false;
}

/**
 * Used to express an ordered set of data. A List object is reified to a Subject
 * (unlike in JSON-LD) and so it has an @id, which can be set by the user.
 *
 * ## Examples:
 *
 * ---
 * *A priority list of preferences*
 *
 * The second subject in this array shows how the first subject (the List) can
 * be referenced by another subject. When inserting data it might be more
 * readable to simply nest the list under the `interests` property in the outer
 * subject, fred.
 * ```json
 * [{
 *   "@id": "fredInterests",
 *   "@list": ["Lounging", "Bowling", "Pool", "Golf", "Poker"]
 * }, {
 *   "@id": "fred",
 *   "interests": { "@id": "fredInterests" }
 * }]
 * ```
 * ---
 * *A chronology of referenced subjects*
 * ```json
 * {
 *   "@id": "fredAppearsIn",
 *   "@list": [
 *     { "@type": "Episode", "name": "The Flintstone Flyer" },
 *     { "@type": "Episode", "name": "Hot Lips Hannigan" },
 *     { "@type": "Episode", "name": "The Swimming Pool" }
 *   ]
 * }
 * ```
 *
 * > 🚧 This engine does not support use of the `@list` keyword in a JSON-LD
 * > Context term definition.
 *
 * [[include:live-code-setup.script.html]]
 * [[include:how-to/domain-setup.md]]
 * [[include:how-to/lists.md]]
 *
 * @see [m-ld Lists specification](https://spec.m-ld.org/#lists)
 * @see [json-rql list](https://json-rql.org/interfaces/list.html)
 * @category json-rql
 */
export interface List extends Subject {
  /**
   * An array or indexed-object representation of the list contents. Each "item"
   * in the list can be any of the normal subject property objects, such as
   * strings, numbers, booleans or References to other subjects.
   *
   * The indexed-object notation is used to insert or delete items at a specific
   * list index, expressed as a number or numeric string. For more explanation,
   * see the [m-ld Lists specification](https://spec.m-ld.org/#lists).
   */
  '@list': SubjectPropertyObject[] | { [key in string | number]: SubjectPropertyObject };
}

/** @internal */
export function isList(object: SubjectPropertyObject): object is List {
  return typeof object === 'object' && object != null && '@list' in object;
}

/**
 * Used to express an unordered set of data and to ensure that values are always
 * represented as arrays.
 * @see [json-rql set](https://json-rql.org/interfaces/set.html)
 * @category json-rql
 */
export interface Set {
  '@set': SubjectPropertyObject;
}

/** @internal */
export function isSet(object: SubjectPropertyObject): object is Set {
  return typeof object === 'object' && object != null && '@set' in object;
}

// Utility functions
/**
 * A value object can only have the allowed keys (not including @index)
 * @internal
 */
export function isValueObject(value: SubjectPropertyObject): value is ValueObject {
  if (value == null || typeof value != 'object' || !('@value' in value))
    return false;
  for (let key in value)
    if (key !== '@value' && key !== '@type' && key !== '@language' && key !== '@id')
      return false;
  return true;
}

/**
 * Determines if the given object contains the given key, and nothing else which
 * would translate to a graph edge.
 * @internal
 */
function isUnaryObject(value: SubjectPropertyObject, theKey: string) {
  // typeof null === 'object' too
  if (value == null || typeof value != 'object' || !(theKey in value))
    return false;
  for (let key in value) {
    const v = (<any>value)[key];
    if (!(key === theKey || v === null || (isArray(v) && v.length === 0)))
      return false;
  }
  return true;
}

/** @internal */
export function isReference(value: SubjectPropertyObject): value is Reference {
  return isUnaryObject(value, '@id');
}

/** @internal */
export function isVocabReference(value: SubjectPropertyObject): value is VocabReference {
  return isUnaryObject(value, '@vocab');
}

/**
 * Result declaration of a {@link Select} query.
 * Use of `'*'` specifies that all variables in the query should be returned.
 * @category json-rql
 */
export type Result = '*' | Variable | Variable[];

/**
 * A resource, represented as a JSON object, that is part of the domain data.
 *
 * ## Examples:
 *
 * ---
 * *A subject with one property: "fred's name is Fred"*
 * ```json
 * {
 *   "@id": "fred",
 *   "name": "Fred"
 * }
 * ```
 * ---
 * *A subject with a {@link Reference} property: "fred's wife is wilma"*
 * ```json
 * {
 *   "@id": "fred",
 *   "wife": { "@id": "wilma" }
 * }
 * ```
 * ---
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
 * @see [json-rql subject](https://json-rql.org/interfaces/subject.html)
 * @category json-rql
 */
export interface Subject extends Pattern {
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
  [key: string]: SubjectPropertyObject | Context | undefined;
}

/** @internal */
interface SubjectConstructor {
  new(value: Subject): Subject;
}

/**
 * Constructor of subjects from subjects: used similarly to e.g. `Number`
 * @category json-rql
 */
export const Subject: SubjectConstructor = class implements Subject {
  [key: string]: Subject['any'];
  constructor(value: Subject) {
    Object.assign(this, value);
  }
};

/**
 * 'Properties' of a Subject, including from {@link List} and {@link Slot}.
 * Strictly, these are possible paths to a {@link SubjectPropertyObject}
 * aggregated by the Subject. An `@list` contains numeric indexes (which may be
 * numeric strings or variables). The second optional index is used for multiple
 * items being inserted at the first index, using an array.
 * @category json-rql
 */
export type SubjectProperty =
  Iri | Variable | '@item' | '@index' | '@type' | ['@list', number | string, number?];

/**
 * Determines whether the given property object from a well-formed Subject is a
 * graph edge; i.e. not a `@context` or the Subject `@id`.
 *
 * @param property the Subject property in question
 * @param object the object (value) of the property
 * @category json-rql
 */
export function isPropertyObject(
  property: string,
  object: unknown
): object is SubjectPropertyObject {
  return property !== '@context' && property !== '@id' && object != null;
}

/** @internal */
export function isSubject(p: Pattern): p is Subject {
  return !isGroup(p) && !isQuery(p);
}

/** @internal */
export function isSubjectObject(o: SubjectPropertyObject): o is Subject {
  return typeof o == 'object' &&
    !(o instanceof Uint8Array) &&
    !isValueObject(o) &&
    !isReference(o) &&
    !isVocabReference(o) &&
    !isInlineConstraint(o);
}
/**
 * An operator-based constraint of the form `{ <operator> : [<expression>...]
 * }`. The key is the operator, and the value is the array of arguments. If the
 * operator is unary, the expression need not be wrapped in an array.
 * @see [json-rql constraint](https://json-rql.org/interfaces/constraint.html)
 * @category json-rql
 */
export type Constraint = Partial<{
  /**
   * Operators are based on SPARQL expression keywords, lowercase with '@' prefix.
   * @see [json-rql operators](https://json-rql.org/globals.html#operators)
   * @see [SPARQL conditional]
   * (https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rConditionalOrExpression)
   */
  [operator in Operator]: Expression | Expression[];
  // It's not practical to constrain the types further here, see #isConstraint
}>

/** @internal */
export function isConstraint(value: Expression | SubjectPropertyObject): value is Constraint {
  if (value == null || typeof value != 'object')
    return false;
  for (let key in value)
    if (!(key in operators))
      return false;
  return true;
}

/** @internal */
export type ConstrainedVariable = Constraint & { '@value': Variable };
/** @internal */
export type InlineConstraint = Constraint | ConstrainedVariable;

/** @internal */
export function isInlineConstraint(value: SubjectPropertyObject): value is InlineConstraint {
  if (value == null || typeof value != 'object')
    return false;
  let empty = true;
  for (let key in value) {
    if (!(key === '@value' || key in operators))
      return false;
    empty = false;
  }
  return !empty;
}

/**
 * Used to express a group of patterns to match, or a group of subjects to write
 * (when used as a transaction pattern).
 *
 * ## Examples:
 *
 * ---
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
 * ---
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
 * ---
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
 * @see [json-rql group](https://json-rql.org/interfaces/group.html)
 * @category json-rql
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
  '@union'?: (Subject | Group)[];
  /**
   * Specifies a filter or an array of filters, each of the form `{ <operator> :
   * [<expression>...] }`.
   */
  '@filter'?: Constraint | Constraint[];
  /**
   * Specifies a Variable Expression or array of Variable Expressions that
   * define [inline allowable value combinations]
   */
  '@values'?: VariableExpression | VariableExpression[];
  /**
   * Allows a computed value to be assigned to a variable. The variable
   * introduced by the `@bind` clause cannot be used in the same Group, but can
   * be returned from a Read or used in an Update.
   */
  '@bind'?: VariableExpression | VariableExpression[];
}

/** @internal */
export function isGroup(p: Pattern): p is Group {
  return '@graph' in p || '@union' in p || '@filter' in p || '@values' in p;
}

/** @internal */
export function isWriteGroup(p: Pattern): p is Group {
  return '@graph' in p && !('@union' in p || '@filter' in p || '@values' in p);
}

/**
 * A variable expression an object whose keys are variables, and whose values
 * are expressions whose result will be assigned to the variable, e.g.
 * ```json
 * { "?averageSize" : { "@avg" : "?size" } }
 * ```
 * @category json-rql
 */
export interface VariableExpression {
  [key: string]: Expression;
}

/**
 * A sub-type of Pattern which matches data using a `@where` clause.
 * @see [json-rql query](https://json-rql.org/interfaces/query.html)
 * @category json-rql
 */
export interface Query extends Pattern {
  /**
   * An optional [JSON-LD Context](https://w3c.github.io/json-ld-syntax/#the-context)
   * for the query. Use of a query-specific Context is rarely required, as the
   * context is typically the local application, whose needs are specified by
   * the local clone configuration.
   */
  '@context'?: Context;
  /**
   * The data pattern to match, as a set of subjects or a group. Variables are
   * used as placeholders to capture matching properties and values in the
   * domain.
   *
   * ## Examples:
   *
   * ---
   * *Match a subject by its `@id`*
   * ```json
   * {
   *   ...
   *   "@where": { "@id": "fred" }
   * }
   * ```
   * ---
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
   * ---
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
   * > The Javascript engine supports filters for subject identities, properties
   * > and values, including {@link Constraint inline&nbsp;filters}.
   */
  '@where'?: Subject | Subject[] | Group;
}

/** @internal */
export function isQuery(p: Pattern): p is Query {
  return isRead(p) || isUpdate(p);
}

/**
 * A query pattern that reads data from the domain.
 * @category json-rql
 */
export interface Read extends Query {
  // No support for @limit, @orderBy etc.
}

/**
 * Determines if the given pattern will read data from the domain.
 *
 * @category json-rql
 */
export function isRead(p: Pattern): p is Read {
  return isDescribe(p) || isSelect(p) || isConstruct(p);
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
 * @category json-rql
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
 * @category json-rql
 */
export function isWrite(p: Pattern): p is Write {
  return !isRead(p) && (isSubject(p) || isWriteGroup(p) || isUpdate(p));
}

/**
 * A simple means to get the properties of a specific subject, or a set of
 * subjects matching some `@where` clause.
 *
 * ## Examples:
 *
 * ---
 * *Describe a specific subject whose `@id` is `fred`*
 * ```json
 * {
 *   "@describe": "fred"
 * }
 * ```
 * ---
 * *Describe all subjects in the domain*
 * ```json
 * {
 *   "@describe": "?id",
 *   "@where": { "@id": "?id" }
 * }
 * ```
 * ---
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
 * ---
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
 * @see [json-rql describe](https://json-rql.org/interfaces/describe.html)
 * @category json-rql
 */
export interface Describe extends Read {
  /**
   * Specifies a single Variable or Iri to return. Each matched value for the
   * identified variable will be output as a [Subject](/interfaces/subject.html)
   * with its top-level properties.
   */
  '@describe': Iri | Variable | (Iri | Variable)[];
}

/** @internal */
export function isDescribe(p: Pattern): p is Describe {
  return '@describe' in p;
}

/**
 * A query pattern that returns a specified JSON structure with variable
 * substitutions. This is useful to query all information needed for some domain
 * entity, including nested information.
 *
 * The `@construct` member defines a JSON template, with placeholder variables
 * for the data to be filled by the query. The returned subjects will have the
 * given template structure, with a few exceptions:
 * - If there is no value for a subject property, the property will be omitted.
 * - If there are multiple values for a subject property, the property will be
 *   an array.
 * - `@list` contents are always returned as an array, even if the query uses an
 *   object (see examples).
 * - Returned subjects always have an `@id`. If no `@id` is given in the
 *   template (as a fixed IRI or variable), a generated placeholder will be
 *   used (starting with `_:`).
 *
 * A `@construct` can be used by itself as a straightforward pattern match to
 * data already in the domain, or with a `@where` clause to create new data
 * structures.
 *
 * ## Examples:
 *
 * ---
 * *Pattern match an identified subject with nested content*
 * ```json
 * {
 *   "@construct": {
 *     "@id": "fred",
 *     "children": {
 *       "@id": "?child", "name": "?childName"
 *     }
 *   }
 * }
 * ```
 * might return:
 * ```json
 * {
 *   "@id": "fred",
 *   "children": [
 *     { "@id": "pebbles", "name": "Pebbles" },
 *     { "@id": "stony", "name": "Stony" }
 *   ]
 * }
 * ```
 *
 * ---
 * *Pattern match list content*
 * ```json
 * {
 *   "@construct": {
 *     "@id": "fred",
 *     "appearsIn": {
 *       "@list": { "1": "?" }
 *     }
 *   }
 * }
 * ```
 * might return (note sparse array containing only the requested index):
 * ```json
 * {
 *   "@id": "fred",
 *   "appearsIn": {
 *     "@list": [
 *       null,
 *       { "@id": "hotLipsHannigan" }
 *     ]
 *   }
 * }
 * ```
 *
 * ---
 * *Construct new information based on existing information*
 * ```json
 * {
 *   "@construct": {
 *     "@id": "?parent",
 *     "grandchildren": {
 *       "@id": "?grandchild"
 *     }
 *   },
 *   "@where": {
 *     "@id": "?parent",
 *     "children": {
 *       "children": {
 *         "@id": "?grandchild"
 *       }
 *     }
 *   }
 * }
 * ```
 * might return:
 * ```json
 * {
 *   "@id": "fred",
 *   "grandchildren": [
 *     { "@id": "roxy" },
 *     { "@id": "chip" }
 *   ]
 * }
 * ```
 *
 * @see [json-rql construct](https://json-rql.org/interfaces/construct.html)
 * @category json-rql
 */
export interface Construct extends Read {
  /**
   * Specifies a Subject for the requested data, using variables to place-hold
   * variables matched by the `@where` clause.
   */
  '@construct': Subject | Subject[];
}

/** @internal */
export function isConstruct(p: Pattern): p is Construct {
  return '@construct' in p;
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
 * ## Examples:
 *
 * ---
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
 * ---
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
 * @see [json-rql select](https://json-rql.org/interfaces/select.html)
 * @category json-rql
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
 * ## Examples:
 *
 * ---
 * *Delete a subject property*
 * ```json
 * {
 *   "@delete": {
 *     "@id": "fred",
 *     "name": "Fred"
 *   }
 * }
 * ```
 * ---
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
 * ---
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
 * ---
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
 * @see [json-rql update](https://json-rql.org/interfaces/update.html)
 * @category json-rql
 */
export interface Update extends Query {
  /**
   * Subjects with properties to be deleted from the domain. Variables can be
   * used without a `@where` clause, to match any existing value.
   */
  '@delete'?: Subject | Subject[];
  /**
   * Subjects with properties to be inserted into the domain. Variables may be used, values for
   * which will be established as follows:
   * - If a `@where` clause exists, then values matched in the `@where` clause will be used.
   * - If there is no `@where`, but a `@delete` clause exists, then values matched in the
   * `@delete` clause will be used.
   * - If a variable value is not matched by the `@where` or `@delete` clause as above, no
   * insertion happens (i.e. there must exist a _complete_ solution to all variables in the
   * `@insert`).
   *
   * **Note** that in the case that the `@insert` contains no variables, there is a difference
   * between matching with a `@where` and `@delete`. If a `@where` clause is provided, it _must_
   * match some existing data for the inserts to happen. However, if no `@where` clauses is
   * provided, then the insertion will happen even if nothing is matched by the `@delete`.
   *
   * For example, assume this data exists:
   * ```json
   * { "@id": "fred", "name": "Fred" }
   * ```
   *
   * Compare the following update patterns:
   * ```json
   * {
   *   "@delete": { "@id": "fred", "height": "?height" },
   *   "@insert": { "@id": "fred", "height": "6" }
   * }
   * ```
   *
   * The pattern above updates Fred's height to 6, even though no prior height value exists.
   * ```json
   * {
   *   "@delete": { "@id": "fred", "height": "?height" },
   *   "@insert": { "@id": "fred", "height": "6" },
   *   "@where": { "@id": "fred", "height": "?height" }
   * }
   * ```
   *
   * The pattern above does nothing, because no prior height value is matched by the `@where`.
   */
  '@insert'?: Subject | Subject[];
  /**
   * Subjects with properties to be updated in the domain. By default, any
   * subject property included will have its old value deleted and the provided
   * value inserted, for example:
   * ```json
   * {
   *   "@update": { "@id": "fred", "height": "6" }
   * }
   * ```
   * is generally equivalent to:
   * ```json
   * {
   *   "@delete": { "@id": "fred", "height": "?" },
   *   "@insert": { "@id": "fred", "height": "6" }
   * }
   * ```
   * All prior values are deleted, so this query form is best suited for
   * 'registers' – properties that are expected to have a single value. Note
   * that the default behaviour can still lead to multiple values if concurrent
   * updates occur and no constraint exists for the property (a 'conflict').
   *
   * Variables may be used to match data to update. If the variable appears in
   * the property value position the update will have no effect unless an
   * in-line operation is used, e.g.
   * ```json
   * {
   *   "@update": { "@id": "fred", "likes": { "@value": "?old", "@plus": 1 } }
   * }
   * ```
   *
   * {@link SharedDatatype Shared data types} operating on the data may change
   * the default behaviour to make an `@update` logically different to a
   * `@delete` and an `@insert`, particularly when using an in-line operation.
   */
  '@update'?: Subject | Subject[];
  /**
   * If this key is included and the value is truthy, this update is an
   * _agreement_. Use of an agreement will guarantee that all clones converge on
   * the "agreed" data state (although they may continue to change thereafter).
   * Agreements may cause concurrent operations on other clones to be _voided_,
   * that is, reversed and removed from history.
   *
   * The use of an agreement usually requires either that some coordination has
   * occurred in the app (externally to **m-ld**), or that the local user has
   * the authority to unilaterally agree. The precondition will be automatically
   * checked by an {@link AgreementCondition} at all remote clones. A violation
   * may lead to the originating clone being flagged as malware.
   *
   * The key value may be used to include any JSON-serialisable proof that
   * applicable agreement conditions have been met, such as a key to a ledger
   * entry.
   *
   * An update with a falsy flag may be automatically upgraded to an agreement
   * by a constraint.
   *
   * > 🧪 Agreements are an experimental feature. Please contact us to discuss
   * your use-case.
   *
   * @see {@link https://github.com/m-ld/m-ld-security-spec/blob/main/design/suac.md the white
   *   paper}
   * @experimental
   */
  '@agree'?: any;
}

/** @internal */
export function isUpdate(p: Pattern): p is Update {
  return '@insert' in p || '@delete' in p || '@update' in p;
}

/**
 * A 'slot' in a {@link List} is a container for a list item. It is seen
 * infrequently, because most list queries and updates use a shorthand in which
 * the list item appears naked and the slot is implicit. Slots appear:
 * - In list update notifications, in which the slot is always explicit.
 * - Optionally, when moving items in a list.
 *
 * @see [m-ld lists specification](https://spec.m-ld.org/#lists)
 * @category json-rql
 */
export interface Slot extends Subject {
  /**
   * The identity of the slot
   */
  '@id': Iri;
  /**
   * The contained item of the slot
   */
  '@item': Value;
  /**
   * The index of the item in the list
   */
  '@index'?: number;
}

/** @internal */
export function isSlot(s: SubjectPropertyObject): s is Slot {
  return typeof s == 'object' && '@id' in s && '@item' in s;
}
