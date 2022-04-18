import {
  isList, isPropertyObject, isReference, isSet, isValueObject, isVocabReference, Reference, Subject,
  SubjectPropertyObject, Value, VocabReference
} from './jrql-support';
import { array } from './util';
import { isArray } from './engine/util';
import { XS } from './ns';
import { isAbsolute } from './engine/jsonld';

export type JsAtomValueConstructor =
  typeof String |
  typeof Number |
  typeof Boolean |
  typeof Date |
  typeof Uint8Array |
  typeof Subject |
  typeof Reference |
  typeof VocabReference

export type JsContainerValueConstructor =
  typeof Array |
  typeof Set |
  typeof Optional

export type JsValueConstructor = JsAtomValueConstructor | JsContainerValueConstructor;

/**
 * Symbolic object for missing Javascript Optional monad
 */
export const Optional = {} as { new<T>(value: T): T | undefined; };
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
export type ValueConstructed<T, S> =
  T extends String ? string :
    T extends Number ? number :
      T extends Boolean ? boolean :
        T extends Array<unknown> ? ArrayConstructed<S> :
          T extends Set<unknown> ? SetConstructed<S> :
            T extends {} ? T : OptionalConstructed<S>;

/** @internal */
export type PropertyType<T> = JsValueConstructor & (new (v: any) => T);
/** @internal */
export type ContainerType<T> = JsContainerValueConstructor & (new (v: any) => T);
/** @internal */
export type AtomType<T> = JsAtomValueConstructor & (new (v: any) => T);

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
 * propertyValue({ name: 'Fred' }, 'name', String); // => 'Fred'
 * propertyValue({ name: 'Fred' }, 'name', Number); // => throws TypeError
 * propertyValue({ name: 'Fred' }, 'name', Set, String); // => Set(['Fred'])
 * propertyValue({ name: 'Fred' }, 'age', Set); // => Set([])
 * propertyValue({
 *   shopping: { '@list': ['Bread', 'Milk'] }
 * }, 'shopping', Array, String); // => ['Bread', 'Milk']
 * propertyValue({
 *   birthday: {
 *     '@value': '2022-01-08T16:49:43.572Z',
 *     '@type': 'http://www.w3.org/2001/XMLSchema#dateTime'
 *   }
 * }, 'birthday', Date); // => Javascript Date
 * ```
 *
 * @param subject the subject to inspect
 * @param property the property to inspect
 * @param type the expected type for the returned value
 * @param subType if `type` is `Array` or `Set`, the expected item type. If not
 * provided, values in a multi-valued property will not be cast
 * @throws TypeError if the given property does not have the correct type
 * @category Utility
 */
export function propertyValue<T, S>(
  subject: Subject,
  property: string,
  type: PropertyType<T>,
  subType?: AtomType<S>
): ValueConstructed<T, S> {
  const value = subject[property];
  if (value == null) {
    switch (type) {
      case Set:
        return <any>new Set;
      case Array:
        return <any>[];
      case Optional:
        return <any>undefined;
      default:
        throw new TypeError(`${value} is not a ${type.name}`);
    }
  } else if (isPropertyObject(property, value)) {
    return castPropertyValue(value, type, subType, property);
  } else {
    throw new TypeError(`${property} is not a property`);
  }
}

/**
 * Casts a property value to the given JavaScript type. This is a typesafe cast
 * which will not perform type coercion e.g. strings to numbers.
 *
 * @param value the value to cast (as from a subject property)
 * @param type the expected type for the returned value
 * @param subType if `type` is `Array` or `Set`, the expected item type. If not
 * provided, values in a multi-valued property will not be cast
 * @param property the property name, for error reporting only
 * @throws TypeError if the given property does not have the correct type
 * @category Utility
 */
export function castPropertyValue<T, S>(
  value: SubjectPropertyObject,
  type: PropertyType<T>,
  subType?: AtomType<S>,
  property?: string
): ValueConstructed<T, S> {
  switch (type) {
    case Set:
    case Array:
    case Optional:
      // Expecting 0..n values
      let values: any[] = valueAsArray(value);
      if (subType != null)
        values = values.map(v => castValue(v, subType));
      switch (type) {
        case Set:
          return <any>new Set(values);
        case Array:
          return <any>values;
        default: // Optional
          if (values.length <= 1)
            return values[0];
          else break;
      }
    default:
      // Expecting a single value
      if (isSet(value) || isList(value) || isArray(value)) {
        const values = valueAsArray(value);
        if (values.length == 1)
          return castPropertyValue(values[0], type, subType);
      } else {
        return castValue(value, <any>type);
      }
  }
  throw new TypeError(`${property ?? 'Property'} has multiple values: ${JSON.stringify(value)}}`);
}

/**@internal*/
function valueAsArray(value: SubjectPropertyObject) {
  if (isSet(value)) {
    return array(value['@set']);
  } else if (isList(value)) {
    if (isArray(value['@list']))
      return value['@list'];
    else
      return Object.assign([], value['@list']);
  } else {
    return array(value);
  }
}

/**@internal*/
function castValue<T>(value: Value, type: JsAtomValueConstructor): T {
  if (isValueObject(value)) {
    switch (type) {
      case Number:
        if (value['@type'] === XS.integer || value['@type'] === XS.double)
          return <any>type(value['@value']);
        break;
      case String:
        if (value['@language'] != null || value['@type'] === XS.string)
          return <any>type(value['@value']);
        break;
      case Boolean:
        if (value['@type'] === XS.boolean)
          return <any>type(value['@value']);
        break;
      case Date:
        if (value['@type'] === XS.dateTime)
          return <any>new Date(String(value['@value']));
        break;
      case Uint8Array:
        if (value['@type'] === XS.base64Binary)
          return <any>Buffer.from(String(value['@value']), 'base64');
        break;
      // Do not support Object, Reference or VocabReference here
    }
  } else {
    switch (type) {
      case Number:
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'number')
          return <any>value;
        break;
      case String:
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'string')
          return <any>value;
        break;
      case Boolean:
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'boolean')
          return <any>value;
        break;
      case Date:
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'string') {
          const date = new Date(value);
          if (date.toString() !== 'Invalid Date')
            return <any>date;
        }
        break;
      case Subject:
        if (typeof value == 'object')
          return <any>value;
        break;
      case Reference:
        if (isReference(value))
          return <any>value;
        if (isVocabReference(value) && isAbsolute(value['@vocab']))
          return <any>{ '@id': value['@vocab'] };
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'string')
          return <any>value;
        break;
      case VocabReference:
        if (isVocabReference(value))
          return <any>value;
        if (isReference(value) && isAbsolute(value['@id']))
          return <any>{ '@vocab': value['@id'] };
        // noinspection SuspiciousTypeOfGuard
        if (typeof value == 'string')
          return <any>value;
        break;
      // Do not support Buffer here
    }
  }
  throw new TypeError(`${value} is not a ${type.name}`);
}

/**
 * Reverse of {@link castPropertyValue}: normalises a JavaScript value to a
 * JSON-LD value suitable for use in a {@link Subject}.
 */
export function normaliseValue(
  value: ValueConstructed<unknown, unknown>
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
  value: ValueConstructed<unknown, unknown>
): SubjectPropertyObject {
  switch (typeof value) {
    case 'string':
    case 'number':
    case 'boolean':
      return value;
    case 'object':
      if (value instanceof Date)
        return { '@type': XS.dateTime, '@value': value.toISOString() };
      else if (value instanceof Uint8Array)
        return { '@type': XS.base64Binary, '@value': Buffer.from(value).toString('base64') };
      else if (value != null)
        return <Subject>value; // Could also be a reference or a value object
  }
  throw new TypeError(`${value} cannot be converted to JSON-LD`);
}