import { isPropertyObject, isSet, Reference, Subject, Value } from './jrql-support';
import { isArray } from './engine/util';
import { compareValues, getValues, hasProperty, hasValue } from './engine/jsonld';
import { array } from './util';

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

  insert(...values: any[]) {
    const object = this.subject[this.property];
    if (isPropertyObject(this.property, object) && isSet(object))
      object['@set'] = SubjectPropertyValues.union(array(object['@set']), values);
    else
      this.setValues(SubjectPropertyValues.union(this.getValues(), values));
  }

  delete(...values: any[]) {
    this.setValues(SubjectPropertyValues.minus(this.getValues(), values));
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

  private getValues() {
    return getValues(this.subject, this.property);
  }

  private setValues(values: any[]) {
    // Apply deep updates to the final values
    this.deepUpdater?.(values);
    // Per contract of updateSubject, this always L-value assigns (no pushing)
    this.subject[this.property] = values.length === 0 ? [] : // See next
      // Properties which were not an array before get collapsed
      values.length === 1 && !this.wasArray ? values[0] : values;
    if (values.length === 0 && this.configurable)
      delete this.subject[this.property];
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
