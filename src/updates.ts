import { Subject, Value } from './jrql-support';
import { DeleteInsert, Resource } from './api';
import { addValue, getValues, hasProperty, hasValue, removeValue, ValueOptions } from 'jsonld/lib/util';

/**
 * Indexes a **m-ld** update notification by Subject.
 *
 * By default, updates are presented with arrays of inserted and deleted
 * subjects:
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
 * app data views on a subject-by-subject basis. This method transforms the
 * above into:
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
 *
 * @param update a **m-ld** update notification obtained via the
 * {@link follow} method
 */
export function asSubjectUpdates(update: DeleteInsert<Subject[]>): SubjectUpdates {
  return bySubject(update, '@insert', bySubject(update, '@delete'));
}

/**
 * A **m-ld** update notification, indexed by Subject.
 * @see {@link asSubjectUpdates}
 */
export type SubjectUpdates = { [id: string]: DeleteInsert<Subject>; };

/** @internal */
function bySubject(update: DeleteInsert<Subject[]>,
  key: '@insert' | '@delete', bySubject: SubjectUpdates = {}): SubjectUpdates {
  return update[key].reduce((byId, subject) => ({ ...byId, [subject['@id'] ?? '*']: { ...byId[subject['@id'] ?? '*'], [key]: subject } }), bySubject);
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
  const inserts = update['@insert'] && subject['@id'] == update['@insert']['@id'] ? update['@insert'] : {};
  const deletes = update['@delete'] && subject['@id'] == update['@delete']['@id'] ? update['@delete'] : {};
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
 * Determines whether the given set of values contains the given value. This
 * method accounts for the identity semantics of {@link Reference}s and
 * {@link Subject}s.
 * @param set the set of values to inspect
 * @param value the value to find in the set. If `undefined`, then wildcard
 * checks for any value at all.
 */
export function includesValue(subject: Subject, property: string, value?: Value): boolean {
  if (value != null)
    return hasValue(subject, property, value);
  else
    return hasProperty(subject, property)
}