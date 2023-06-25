import {
  isList, isPropertyObject, isReference, isSet, isValueObject, isVocabReference, Reference, Subject,
  SubjectPropertyObject, Value, VocabReference
} from './jrql-support';
import { isArray } from './engine/util';
import { XS } from './ns';
import { asValues, isAbsolute, minimiseValue } from './engine/jsonld';

/**
 * Javascript atom constructors for types that can be obtained from graph
 * subject properties.
 *
 * @category Utility
 */
export type JsAtomValueConstructor =
  typeof String |
  typeof Number |
  typeof Boolean |
  typeof Date |
  typeof Uint8Array |
  typeof Subject |
  typeof Reference |
  typeof VocabReference

/**
 * Javascript container constructors for types that can be obtained from graph
 * subject properties.
 *
 * @category Utility
 */
export type JsContainerValueConstructor =
  typeof Array |
  typeof Set |
  typeof Optional

/**
 * Javascript constructors for types that can be obtained from graph subject
 * properties.
 *
 * @category Utility
 */
export type JsValueConstructor = JsAtomValueConstructor | JsContainerValueConstructor;

/**
 * Symbolic object for missing Javascript Optional monad
 * @category Utility
 */
export const Optional = {} as { new<T>(value: T): T | undefined; };
/** @internal */
export type Optional = typeof Optional;

/** @internal */
type ArrayConstructed<S> =
  S extends String ? string[] :
    S extends Number ? number[] :
      S extends Boolean ? boolean[] :
        S[];

/** @internal */
type SetConstructed<S> =
  S extends String ? Set<string> :
    S extends Number ? Set<number> :
      S extends Boolean ? Set<boolean> :
        Set<S>;

/** @internal */
type OptionalConstructed<S> =
  S extends String ? string | undefined :
    S extends Number ? number | undefined :
      S extends Boolean ? boolean | undefined :
        S | undefined;

/** @internal */
export type ValueConstructed<T, S = unknown> =
  T extends String ? string :
    T extends Number ? number :
      T extends Boolean ? boolean :
        T extends Array<unknown> ? ArrayConstructed<S> :
          T extends Set<unknown> ? SetConstructed<S> :
            T extends {} ? T : OptionalConstructed<S>;

/**
 * A Javascript value type constructor
 * @category Utility
 */
export type PropertyType<T> = JsValueConstructor & (new (v: any) => T);
/**
 * A Javascript container value type constructor, one of:
 * - `Array`
 * - `Set`
 * - `Optional`
 * @category Utility
 */
export type ContainerType<T> = JsContainerValueConstructor & (new (v: any) => T);
/**
 * A Javascript atom value type constructor, one of:
 * - `String`
 * - `Number`
 * - `Boolean`
 * - `Date`
 * - `Uint8Array`
 * - `Subject`
 * - `Reference`
 * - `VocabReference`
 * @category Utility
 */
export type AtomType<T> = JsAtomValueConstructor & (new (v: any) => T);

/**
 * Runtime representation of a Javascript type that can be {@link cast} from a
 * subject property object (a JSON-LD value).
 *
 * @see {@link JsAtomType}
 * @see {@link JsContainerType}
 * @category Utility
 */
export abstract class JsType<T, S = unknown> {
  static statics = new Map<PropertyType<any>, Map<AtomType<any> | undefined, JsType<any>>>;

  /**
   * Obtains a static, shared type representation for the given parameters.
   * @param type the atom type or container type
   * @param subType the atom type, if `type` is a container (else ignored)
   */
  static for<T, S>(type: PropertyType<T>, subType?: AtomType<S>): JsType<T, S> {
    let subTypes = JsType.statics.get(type);
    if (subTypes == null)
      JsType.statics.set(type, subTypes = new Map());
    let jsType = subTypes.get(subType);
    if (jsType == null) {
      switch (type) {
        case Set:
        case Array:
        case Optional:
          subTypes.set(subType, jsType = new JsContainerType(type, subType));
          break;
        default:
          subTypes.set(subType, jsType = new JsAtomType(<AtomType<any>>type));
      }
    }
    return <JsType<T, S>>jsType;
  }

  /** @returns Raw Javascript type information */
  abstract get type(): [PropertyType<T>, AtomType<S>?];

  /**
   * Casts a property value to this JavaScript type.
   *
   * @param value the value to cast (as from a subject property)
   * @throws TypeError if the given value does not have the correct type
   */
  abstract cast(value: SubjectPropertyObject): ValueConstructed<T, S>;
}

/**
 * Runtime representation of a supported Javascript atom type, such as string,
 * number, boolean, Date, etc.
 * @category Utility
 */
export class JsAtomType<T> extends JsType<T> {
  /**
   * @param aType the expected type for the returned value
   * @param merge a function to merge multiple values into a single atom
   */
  constructor(
    readonly aType: AtomType<T>,
    readonly merge = maxValue
  ) {
    super();
  }

  toString() {
    return this.aType.name;
  }

  get type(): [AtomType<T>] {
    return [this.aType];
  }

  /**
   * Casts a property value to the given JavaScript type. This is a typesafe cast
   * which will not perform type coercion e.g. strings to numbers.
   *
   * @param value the value to cast (as from a subject property)
   * @throws TypeError if the given value does not have the correct type
   */
  cast(value: SubjectPropertyObject): ValueConstructed<T> {
    // Expecting precisely one value
    if (isSet(value) || isList(value) || isArray(value)) {
      const values = castToArray(value, this.aType);
      if (values.length == 0)
        throw new TypeError('missing mandatory value');
      if (values.length == 1)
        return values[0];
      else
        return this.merge(this.aType, ...values);
    } else {
      return castValue(value, <any>this.aType);
    }
  }
}

/**
 * Runtime representation of a supported Javascript container type, such as an
 * Array or Set.
 * @category Utility
 */
export class JsContainerType<T, S> extends JsType<T, S> {
  /**
   * @param cType the expected type for the returned value
   * @param aType if `type` is `Array` or `Set`, the expected item type. If not
   * provided, values in a multi-valued property will not be cast
   * @param merge a function to merge multiple values into a single atom
   */
  constructor(
    readonly cType: ContainerType<T>,
    readonly aType?: AtomType<S>,
    readonly merge = maxValue
  ) {
    super();
  }

  toString() {
    return this.cType.name;
  }

  get type(): [PropertyType<T>, AtomType<S>?] {
    return [this.cType, this.aType];
  }

  get emptyValue() {
    switch (this.cType) {
      case Set:
        return new Set;
      case Array:
        return [];
      default: // Optional:
        return undefined;
    }
  }

  /**
   * Casts a property value to the given JavaScript type. This is a typesafe cast
   * which will not perform type coercion e.g. strings to numbers.
   *
   * @param value the value to cast (as from a subject property)
   * @throws TypeError if the given value does not have the correct type
   */
  cast(value: SubjectPropertyObject): ValueConstructed<T, S> {
    const values = castToArray(value, this.aType);
    switch (this.cType) {
      case Set:
        return <any>new Set(values);
      case Array:
        return <any>values;
      default: // Optional:
        if (values.length <= 1)
          return values[0];
        else
          return this.merge(this.aType, ...values);
    }
  }
}

/**
 * Runtime representation of the mapping between a Javascript class property and
 * a runtime type.
 * @see {@link value}
 * @category Utility
 */
export class JsProperty<T, S = unknown> {
  /**
   * Construct a property mapping for a known type and subtype.
   * @see JsType#for
   * @category Utility
   */
  static for<T, S>(
    property: string,
    type: PropertyType<T>,
    subType?: AtomType<S>
  ) {
    return new JsProperty(property, JsType.for(type, subType));
  }

  /**
   * @param name the JSON-LD property to inspect
   * @param type the property type
   */
  constructor(
    readonly name: string,
    readonly type: JsType<T, S>
  ) {}

  /**
   * Extracts a property value from the given subject with the given Javascript
   * type. This is a typesafe cast which will not perform type coercion e.g.
   * strings to numbers.
   *
   * Per **m-ld** [data&nbsp;semantics](https://spec.m-ld.org/#data-semantics), a
   * single value in a field is equivalent to a singleton set (see example), and
   * will also cast successfully to a singleton array.
   *
   * ## Examples:
   *
   * ```js
   * new JsProperty('name', JsType.for(String)).value({ name: 'Fred' }); // => 'Fred'
   * new JsProperty('name', JsType.for(Number)).value({ name: 'Fred' }); // => throws TypeError
   * new JsProperty('name', JsType.for(Set, String)).value({ name: 'Fred' }); // => Set(['Fred'])
   * new JsProperty('age', JsType.for(Set)).value({ name: 'Fred' }, 'age'); // => Set([])
   * new JsProperty('shopping', JsType.for(Array, String)).value({
   *   shopping: { '@list': ['Bread', 'Milk'] }
   * }); // => ['Bread', 'Milk']
   * new JsProperty('birthday', JsType.for(Date)).value({
   *   birthday: {
   *     '@value': '2022-01-08T16:49:43.572Z',
   *     '@type': 'http://www.w3.org/2001/XMLSchema#dateTime'
   *   }
   * }); // => Javascript Date
   * ```
   *
   * @param subject the subject to inspect
   * @throws TypeError if the given property does not have the correct type
   */
  value(subject: Subject): ValueConstructed<T, S> {
    const value = subject[this.name];
    if (value == null) {
      if (this.type instanceof JsContainerType) {
        return <any>this.type.emptyValue;
      } else {
        throw new TypeError(`${value} is not a ${this.type}`);
      }
    } else if (isPropertyObject(this.name, value)) {
      try {
        return this.type.cast(value);
      } catch (e) {
        throw new TypeError(`${this.name} ${e}`);
      }
    } else {
      throw new TypeError(`${this.name} is not a property`);
    }
  }
}

/**
 * Top-level utility version of {@link JsProperty#value}
 * @category Utility
 */
export function propertyValue<T, S>(
  subject: Subject,
  property: string,
  type: PropertyType<T>,
  subType?: AtomType<S>
): ValueConstructed<T, S> {
  return new JsProperty(property, JsType.for(type, subType)).value(subject);
}

/**
 * Top-level utility version of {@link JsType.cast}
 * @category Utility
 */
export function castPropertyValue<T, S>(
  value: SubjectPropertyObject,
  type: PropertyType<T>,
  subType?: AtomType<S>,
  property?: string
): ValueConstructed<T, S> {
  try {
    return JsType.for(type, subType).cast(value);
  } catch (e) {
    throw new TypeError(`${property ?? 'Property'} ${e}`);
  }
}

/**@internal*/
function castToArray<S>(
  value: SubjectPropertyObject,
  subType: AtomType<S> | undefined
) {
  let values: any[] = valueAsArray(value);
  if (subType != null)
    values = values.map(v => castValue(v, subType));
  return values;
}

/**@internal*/
function valueAsArray(value: SubjectPropertyObject) {
  if (isList(value)) {
    if (isArray(value['@list']))
      return value['@list'];
    else
      return Object.assign([], value['@list']);
  } else {
    return asValues(value);
  }
}

/**@internal*/
function castValue<T>(value: Value, type: JsAtomValueConstructor): T {
  if (isValueObject(value)) {
    switch (type) {
      case Number:
        if (value['@type'] === XS.integer || value['@type'] === XS.double)
          return <T>type(value['@value']);
        break;
      case String:
        if (value['@language'] != null || value['@type'] === XS.string)
          return <T>type(value['@value']);
        break;
      case Boolean:
        if (value['@type'] === XS.boolean)
          return <T>type(value['@value']);
        break;
      case Date:
        if (value['@type'] === XS.dateTime)
          return <T>new Date(String(value['@value']));
        break;
      case Uint8Array:
        if (value['@type'] === XS.base64Binary)
          return <T>Buffer.from(String(value['@value']), 'base64');
        break;
      // Do not support Object, Reference or VocabReference here
    }
  } else {
    switch (type) {
      case Number:
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'number')
          return <T>value;
        break;
      case String:
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'string')
          return <T>value;
        break;
      case Boolean:
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'boolean')
          return <T>value;
        break;
      case Date:
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'string') {
          const date = new Date(value);
          if (date.toString() !== 'Invalid Date')
            return <T>date;
        }
        break;
      case Subject:
        if (typeof value == 'object')
          return <T>value;
        break;
      case Reference:
        if (isReference(value))
          return <T>value;
        if (isVocabReference(value) && isAbsolute(value['@vocab']))
          return <T>{ '@id': value['@vocab'] };
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'string')
          return <T>value;
        break;
      case VocabReference:
        if (isVocabReference(value))
          return <T>value;
        if (isReference(value) && isAbsolute(value['@id']))
          return <T>{ '@vocab': value['@id'] };
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'string')
          return <T>value;
        break;
      case Uint8Array:
        if (value instanceof Uint8Array)
          return <T>value;
        break;
    }
  }
  throw new TypeError(`${value} is not a ${type.name}`);
}

/**
 * An atom value merge strategy that takes the maximum value. Subjects,
 * References and VocabReferences are compared by their identity (`@id` or
 * `@vocab`).
 *
 * @param type the atom type
 * @param values the values to merge by finding the maximum
 * @category Utility
 */
export function maxValue<T>(
  type: AtomType<T> | undefined,
  ...values: ValueConstructed<T>[]
): ValueConstructed<T> {
  switch (type) {
    case undefined:
    case String:
    case Number:
    case Boolean:
    case Date:
    case Uint8Array:
      return values.reduce((result, value) => value > result ? value : result);
    case Subject:
    case Reference:
    case VocabReference:
      return values.reduce(([result, resultStr], value) => {
        const valueStr = JSON.stringify(minimiseValue(value));
        return valueStr > resultStr ? [value, valueStr] : [result, resultStr];
      }, [<any>{}, ''])[0];
  }
  throw new TypeError(`${type} is not a known atom type`);
}

/**
 * An atom value merge strategy that refused to merge and throws. This should be
 * used in situations where a exception is suitable for the application logic.
 *
 * Note that in many situations it may be better to declare the property as an
 * `Array` or `Set`, and to present the conflict to the user for resolution.
 *
 * @param type the atom type
 * @param values the values to merge by throwing an exception
 * @throws {TypeError} always
 * @category Utility
 */
export function noMerge<T>(type: AtomType<T>, ...values: ValueConstructed<T>[]): never {
  throw new TypeError(`multiple values: ${JSON.stringify(values)}}`);
}

/**
 * Reverse of {@link castPropertyValue}: normalises a JavaScript value to a
 * JSON-LD value suitable for use in a {@link Subject}.
 * @category Utility
 */
export function normaliseValue(
  value: ValueConstructed<unknown>
): SubjectPropertyObject | undefined {
  if (isArray(value))
    return value.map(v => normaliseAtomValue(v));
  else if (value instanceof Set)
    return normaliseValue([...value]);
  else if (value == null)
    return [];
  else
    return normaliseAtomValue(value);
}

/**@internal*/
function normaliseAtomValue(
  value: ValueConstructed<unknown>
): SubjectPropertyObject {
  switch (typeof value) {
    case 'string':
    case 'number':
    case 'boolean':
      return value;
    case 'object':
      if (value instanceof Date)
        return {
          '@type': XS.dateTime,
          '@value': value.toISOString()
        };
      else if (value != null)
        return <Subject>value; // Could also be a reference or a value object
  }
  throw new TypeError(`${value} cannot be converted to JSON-LD`);
}