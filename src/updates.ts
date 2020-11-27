import { Subject, Value, isValueObject, Reference } from './jrql-support';
import { array } from './util';
import { DeleteInsert, Resource } from './api';

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
      default: subject[key as keyof Resource<T>] =
        updateProperty(subject[key], inserts[key], deletes[key]);
    }
  });
  return subject;
}

/** @internal */
function updateProperty(value: any, insertVal: any, deleteVal: any): any {
  let rtn = array(value).filter(v => !includesValue(array(deleteVal), v));
  rtn = rtn.concat(array(insertVal).filter(v => !includesValue(rtn, v)));
  return rtn.length == 1 && !Array.isArray(value) ? rtn[0] : rtn;
}

/**
 * Determines whether the given set of values contains the given value. This
 * method accounts for the identity semantics of {@link Reference}s and
 * {@link Subject}s.
 * @param set the set of values to inspect
 * @param value the value to find in the set
 */
export function includesValue(set: Value[], value: Value): boolean {
  // FIXME support value objects
  if (isSubjectOrRef(value)) {
    return !!value['@id'] && set.filter(isSubjectOrRef).map(v => v['@id']).includes(value['@id']);
  } else {
    return set.includes(value);
  }
}

/** @internal */
function isSubjectOrRef(value: Value): value is Subject | Reference {
  return typeof value == 'object' && !isValueObject(value);
}
