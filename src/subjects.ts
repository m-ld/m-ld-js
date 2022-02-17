import {
  isList, isPropertyObject, isReference, isSet, isValueObject, isVocabReference, Reference, Subject,
  SubjectPropertyObject, Value, VocabReference
} from './jrql-support';
import { isArray } from './engine/util';
import { compareValues, getValues, hasProperty, hasValue } from './engine/jsonld';
import { array } from './util';
import { XS } from './ns';

/** @internal */
export class SubjectPropertyValues {
  private readonly wasArray: boolean;
  private readonly configurable: boolean;

  constructor(
    readonly subject: Subject,
    readonly property: string,
    readonly deepUpdater?: (values: Iterable<any>) => void
  ) {
    this.wasArray = isArray(this.subject[this.property]);
    this.configurable = Object.getOwnPropertyDescriptor(subject, property)?.configurable ?? false;
  }

  get values() {
    return getValues(this.subject, this.property);
  }

  set values(values: any[]) {
    // Apply deep updates to the final values
    this.deepUpdater?.(values);
    // Per contract of updateSubject, this always L-value assigns (no pushing)
    this.subject[this.property] = values.length === 0 ? [] : // See next
      // Properties which were not an array before get collapsed
      values.length === 1 && !this.wasArray ? values[0] : values;
    if (values.length === 0 && this.configurable)
      delete this.subject[this.property];
  }

  delete(...values: any[]) {
    this.values = SubjectPropertyValues.minus(this.values, values);
  }

  insert(...values: any[]) {
    const object = this.subject[this.property];
    if (isPropertyObject(this.property, object) && isSet(object))
      object['@set'] = SubjectPropertyValues.union(array(object['@set']), values);
    else
      this.values = SubjectPropertyValues.union(this.values, values);
  }

  exists(value?: any) {
    if (value != null) {
      const object = this.subject[this.property];
      if (!isPropertyObject(this.property, object))
        return false;
      else if (isSet(object))
        return hasValue(object, '@set', value);
      else
        return hasValue(this.subject, this.property, value);
    } else {
      return hasProperty(this.subject, this.property);
    }
  }

  private static union(values: any[], unionValues: any[]): any[] {
    return values.concat(SubjectPropertyValues.minus(unionValues, values));
  }

  private static minus(values: any[], minusValues: any[]): any[] {
    return values.filter(value => !minusValues.some(
      minusValue => compareValues(value, minusValue)));
  }
}

/**
 * Includes the given value in the Subject property, respecting **m-ld** data
 * semantics by expanding the property to an array, if necessary.
 *
 * @param subject the subject to add the value to.
 * @param property the property that relates the value to the subject.
 * @param values the value to add.
 * @category Utility
 */
export function includeValues(subject: Subject, property: string, ...values: Value[]) {
  new SubjectPropertyValues(subject, property).insert(...values);
}
/**
 * Determines whether the given set of subject property has the given value.
 * This method accounts for the identity semantics of {@link Reference}s and
 * {@link Subject}s.
 *
 * @param subject the subject to inspect
 * @param property the property to inspect
 * @param value the value to find in the set. If `undefined`, then wildcard
 * checks for any value at all.
 * @category Utility
 */
export function includesValue(subject: Subject, property: string, value?: Value): boolean {
  return new SubjectPropertyValues(subject, property).exists(value);
}

export type AtomValueConstructor =
  typeof String |
  typeof Number |
  typeof Boolean |
  typeof Date |
  typeof Uint8Array |
  typeof Subject |
  typeof Reference |
  typeof VocabReference

export type ContainerValueConstructor =
  typeof Array |
  typeof Set

export type PropertyValueConstructor = AtomValueConstructor | ContainerValueConstructor;

/** @internal */
type ValueConstructed<T, S> =
  T extends String ? string :
    T extends Number ? number :
      T extends Boolean ? boolean :
        T extends unknown[] ? S[] :
          T extends Set<unknown> ? Set<S> : T;

/**
 * Extracts a property value from the given subject with the given type. This is
 * a typesafe cast which will not perform type coercion e.g. strings to numbers.
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
  type: PropertyValueConstructor & (new (v: any) => T),
  subType?: AtomValueConstructor & (new (v: any) => S)
): ValueConstructed<T, S> {
  const value = subject[property];
  if (value == null) {
    switch (type) {
      case Set:
        return <any>new Set;
      case Array:
        return <any>[];
      default:
        throw new TypeError(`${value} is not a ${type.name}`);
    }
  } else if (isPropertyObject(property, value)) {
    return castPropertyValue(value, type, subType);
  } else {
    throw new TypeError(`${property} is not a property`);
  }
}
/**
 * Casts a property value to the given type. This is a typesafe cast which will
 * not perform type coercion e.g. strings to numbers.
 *
 * @param value the value to cast (as from a subject property)
 * @param type the expected type for the returned value
 * @param subType if `type` is `Array` or `Set`, the expected item type. If not
 * provided, values in a multi-valued property will not be cast
 * @throws TypeError if the given property does not have the correct type
 * @category Utility
 */
export function castPropertyValue<T, S>(
  value: SubjectPropertyObject,
  type: PropertyValueConstructor & (new (v: any) => T),
  subType?: AtomValueConstructor & (new (v: any) => S)
): ValueConstructed<T, S> {
  switch (type) {
    case Set:
    case Array:
      // Expecting multiple values
      let values: any[] = valueAsArray(value);
      if (subType != null)
        values = values.map(v => castValue(v, subType));
      if (type === Set)
        return <any>new Set(values);
      else
        return <any>values;
    default:
      // Expecting a single value
      if (isSet(value) || isList(value) || isArray(value)) {
        const values = valueAsArray(value);
        if (values.length == 1)
          return castPropertyValue(values[0], type, subType);
        else
          throw new TypeError('Property has multiple values');
      }
      return castValue(value, <any>type);
  }
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
function castValue<T>(value: Value, type: AtomValueConstructor): T {
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
      // Do not support Object or Reference here
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
        break;
      case VocabReference:
        if (isVocabReference(value))
          return <any>value;
        break;
      // Do not support Buffer here
    }
  }
  throw new TypeError(`${value} is not a ${type.name}`);
}