import { isPropertyObject, isSet, Reference, Subject, Value } from './jrql-support';
import { isArray } from './engine/util';
import { compareValues, getValues, hasProperty, hasValue } from './engine/jsonld';

export { compareValues, getValues };

/**
 * @internal
 * @todo A `@list` property is a property according to `isPropertyObject` but
 * should have very different behaviour
 */
export class SubjectPropertyValues {
  private readonly prior: 'atom' | 'array' | 'set';
  private readonly configurable: boolean;

  constructor(
    readonly subject: Subject,
    readonly property: string,
    readonly deepUpdater?: (values: Iterable<any>) => void
  ) {
    const object = this.subject[this.property];
    if (isArray(object))
      this.prior = 'array';
    else if (isPropertyObject(this.property, object) && isSet(object))
      this.prior = 'set';
    else
      this.prior = 'atom';
    this.configurable = Object.getOwnPropertyDescriptor(subject, property)?.configurable ?? false;
  }

  get values() {
    return getValues(this.subject, this.property);
  }

  insert(...values: any[]) {
    this.update([], values);
  }

  delete(...values: any[]) {
    this.update(values, []);
  }

  update(deletes: any[], inserts: any[]) {
    const oldValues = this.values;
    let values = SubjectPropertyValues.minus(oldValues, deletes);
    values = SubjectPropertyValues.union(values, inserts);
    // Apply deep updates to the final values
    this.deepUpdater?.(values);
    // Do not call setter if nothing has changed
    if (oldValues !== values) {
      if (this.prior == 'set') {
        // A JSON-LD Set cannot have any other key than @set
        this.subject[this.property] = { '@set': values };
      } else {
        // Per contract of updateSubject, this always L-value assigns (no pushing)
        this.subject[this.property] = values.length === 0 ? [] : // See next
          // Properties which were not an array before get collapsed
          values.length === 1 && this.prior == 'atom' ? values[0] : values;
        if (values.length === 0 && this.configurable)
          delete this.subject[this.property];
      }
    }
  }

  exists(value?: any): boolean {
    if (isArray(value)) {
      return value.every(v => this.exists(v));
    } else if (value != null) {
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

  /** @returns `values` if nothing has changed */
  private static union(values: any[], unionValues: any[]): any[] {
    const newValues = SubjectPropertyValues.minus(unionValues, values);
    return newValues.length > 0 ? values.concat(newValues) : values;
  }

  /** @returns `values` if nothing has changed */
  private static minus(values: any[], minusValues: any[]): any[] {
    if (values.length === 0 || minusValues.length === 0)
      return values;
    const filtered = values.filter(value => !minusValues.some(
      minusValue => compareValues(value, minusValue)));
    return filtered.length === values.length ? values : filtered;
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
export function includeValues(
  subject: Subject,
  property: string,
  ...values: Value[]
) {
  new SubjectPropertyValues(subject, property).insert(...values);
}

/**
 * Determines whether the given set of subject property has the given value.
 * This method accounts for the identity semantics of {@link Reference}s and
 * {@link Subject}s.
 *
 * @param subject the subject to inspect
 * @param property the property to inspect
 * @param value the value or values to find in the set. If `undefined`, then
 * wildcard checks for any value at all. If an empty array, always returns `true`
 * @category Utility
 */
export function includesValue(
  subject: Subject,
  property: string,
  value?: Value | Value[]
): boolean {
  return new SubjectPropertyValues(subject, property).exists(value);
}
