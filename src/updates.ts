import { isPropertyObject, isSet, Subject, Value } from './jrql-support';
import { DeleteInsert, Resource } from './api';
import { addValue, getValues, hasProperty, hasValue, removeValue, ValueOptions } from './engine/jsonld';

/**
 * A **m-ld** update notification, indexed by Subject.
 */
export type SubjectUpdates = { [id: string]: DeleteInsert<Subject>; };

/**
 * Provides an alternate view of the update deletes and inserts, by Subject.
 *
 * An update is presented with arrays of inserted and deleted subjects:
 * ```json
 * {
 *   "@delete": [{ "@id": "foo", "severity": 3 }],
 *   "@insert": [
 *     { "@id": "foo", "severity": 5 },
 *     { "@id": "bar", "severity": 1 }
 *   ]
 * }
 * ```
 *
 * In many cases it is preferable to apply inserted and deleted properties to
 * app data views on a subject-by-subject basis. This property views the above
 * as:
 * ```json
 * {
 *   "foo": {
 *     "@delete": { "@id": "foo", "severity": 3 },
 *     "@insert": { "@id": "foo", "severity": 5 }
 *   },
 *   "bar": {
 *     "@delete": {},
 *     "@insert": { "@id": "bar", "severity": 1 }
 *   }
 * }
 * ```
 */
export function asSubjectUpdates(update: DeleteInsert<Subject[]>): SubjectUpdates {
  return bySubject(update, '@insert', bySubject(update, '@delete'));
}

/** @internal */
function bySubject(update: DeleteInsert<Subject[]>,
  key: keyof DeleteInsert<Subject[]>,
  bySubject: SubjectUpdates = {}): SubjectUpdates {
  return update[key].reduce((bySubject, subject) => {
    const id = subject['@id'] ?? ''; // Should never be empty
    bySubject[id] = Object.assign(bySubject[id] ?? {}, { [key]: subject });
    return bySubject;
  }, bySubject);
}

/** @internal */
const valueOptions = (subject: Subject, key: string): ValueOptions => ({
  // m-ld semantics are strict on Set properties
  allowDuplicate: false,
  // Try to preserve the array-ness of keys, as far as possible
  propertyIsArray: Array.isArray(subject[key])
});

/**
 * Applies a subject update to the given subject, expressed as a
 * {@link Resource}. This method will correctly apply the deleted and inserted
 * properties from the update, accounting for **m-ld**
 * [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics).
 * @param subject the resource to apply the update to
 * @param update the update, obtained from an {@link asSubjectUpdates} transformation
 * @typeParam T the app-specific subject type of interest
 */
export function updateSubject<T>(subject: Resource<T>, update: DeleteInsert<Subject>): Resource<T> {
  // Allow for undefined/null ids
  const inserts = subject['@id'] === update['@insert']?.['@id'] ? update['@insert'] : {};
  const deletes = subject['@id'] === update['@delete']?.['@id'] ? update['@delete'] : {};
  new Set(Object.keys(subject).concat(Object.keys(inserts))).forEach(key => {
    switch (key) {
      case '@id': break;
      default:
        const opts = valueOptions(subject, key);
        for (let del of getValues(deletes, key))
          removeValue(subject, key, del, opts);
        for (let ins of getValues(inserts, key))
          addValue(subject, key, ins, opts);
    }
  });
  return subject;
}

/**
 * Includes the given value in the Subject property, respecting **m-ld** data
 * semantics by expanding the property to an array, if necessary.
 * @param subject the subject to add the value to.
 * @param property the property that relates the value to the subject.
 * @param value the value to add.
 */
export function includeValue(subject: Subject, property: string, value: Value) {
  const object = subject[property];
  if (isPropertyObject(property, object) && isSet(object))
    addValue(object, '@set', value, valueOptions(subject, property));
  else
    addValue(subject, property, value, valueOptions(subject, property));
}

/**
 * Determines whether the given set of values contains the given value. This
 * method accounts for the identity semantics of {@link Reference}s and
 * {@link Subject}s.
 * @param set the set of values to inspect
 * @param value the value to find in the set. If `undefined`, then wildcard
 * checks for any value at all.
 */
export function includesValue(subject: Subject, property: string, value?: Value): boolean {
  if (value != null) {
    const object = subject[property];
    if (!isPropertyObject(property, object))
      return false;
    else if (isSet(object))
      return hasValue(object, '@set', value);
    else
      return hasValue(subject, property, value);
  } else {
    return hasProperty(subject, property)
  }
}