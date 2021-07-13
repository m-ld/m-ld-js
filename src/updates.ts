import {
  isList, isPropertyObject, isSet, List, Reference, Slot, Subject, Value
} from './jrql-support';
import { DeleteInsert, GraphSubject, GraphSubjects, isDeleteInsert } from './api';
import { compareValues, getValues, hasProperty, hasValue } from './engine/jsonld';
import { deepValues, isArray, isNaturalNumber, setAtPath } from './engine/util';
import { array } from './util';
import { isReference } from 'json-rql';

/**
 * An update to a single graph Subject.
 */
export type SubjectUpdate = DeleteInsert<GraphSubject | undefined>;
/**
 * A **m-ld** update notification, indexed by graph Subject ID.
 */
export type SubjectUpdates = { [id: string]: SubjectUpdate };
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
 *
 * Javascript references to other Subjects in a Subject's properties will always
 * be collapsed to json-rql Reference objects (e.g. `{ '@id': '<iri>' }`).
 */
export function asSubjectUpdates(update: DeleteInsert<GraphSubject[]>): SubjectUpdates {
  return bySubject(update, '@insert', bySubject(update, '@delete'));
}

/** @internal */
function bySubject(update: DeleteInsert<GraphSubject[]>,
  key: keyof DeleteInsert<GraphSubject[]>,
  bySubject: SubjectUpdates = {}): SubjectUpdates {
  for (let subject of update[key])
    Object.assign(
      // @id should never be empty
      bySubject[subject['@id'] ?? '*'] ??= { '@delete': undefined, '@insert': undefined },
      { [key]: unReifyRefs({ ...subject }) });
  return bySubject;
}

/** @internal */
function unReifyRefs(subject: Subject) {
  for (let [path, value] of deepValues(subject,
    (value, path) => path.length > 0 && typeof value == 'object' && '@id' in value))
    setAtPath(subject, path, { '@id': value['@id'] });
  return subject;
}

/**
 * Applies an update to the given subject in-place. This method will correctly
 * apply the deleted and inserted properties from the update, accounting for
 * **m-ld** [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics).
 *
 * Referenced Subjects will also be updated if they have been affected by the
 * given update, deeply. If a reference property has changed to a different
 * object (whether or not that object is present in the update), it will be
 * updated to a json-rql Reference (e.g. `{ '@id': '<iri>' }`).
 *
 * Changes are applied to non-`@list` properties using only L-value assignment,
 * so the given Subject can be safely implemented with property setters, such as
 * using `set` in a class, or by using `defineProperty`, or using a Proxy; for
 * example to trigger side-effect behaviour. Removed properties are set to an
 * empty array (`[]`) to signal emptiness, and then deleted.
 *
 * Changes to `@list` items are enacted in reverse index order, by calling
 * `splice`. If the `@list` value is a hash, it will have a `length` property
 * added. To intercept these calls, re-implement `splice` on the `@list`
 * property value.
 *
 * CAUTION: If this function is called independently on subjects which reference
 * each other via Javascript references, or share referenced subjects, then the
 * referenced subjects may be updated more than once, with unexpected results.
 * To avoid this, use a {@link SubjectUpdater} to process the whole update.
 *
 * @param subject the resource to apply the update to
 * @param update the update, as a {@link MeldUpdate} or obtained from
 * {@link asSubjectUpdates}
 * @typeParam T the app-specific subject type of interest
 * @see [m-ld data semantics](http://spec.m-ld.org/#data-semantics)
 */
export function updateSubject<T extends Subject & Reference>(
  subject: T, update: SubjectUpdates | DeleteInsert<GraphSubjects>): T {
  return new SubjectUpdater(update).update(subject);
}

/**
 * Applies an update to more than one subject. Subjects (by Javascript
 * reference) will not be updated more than once, even if multiply-referenced.
 * @see {@link updateSubject}
 */
export class SubjectUpdater {
  private readonly delOrInsForSubject:
    (subject: GraphSubject, key: keyof DeleteInsert<any>) => GraphSubject | undefined;
  private readonly done = new Set<object>();

  constructor(update: SubjectUpdates | DeleteInsert<GraphSubjects>) {
    if (isDeleteInsert(update))
      this.delOrInsForSubject = (subject, key) =>
        update[key].graph.get(subject['@id']);
    else
      this.delOrInsForSubject = (subject, key) =>
        subject['@id'] in update ? update[subject['@id']][key] : undefined;
  }

  /**
   * Applies an update to the given subject in-place.
   * 
   * @returns the given subject, for convenience
   * @see {@link updateSubject}
   */
  update<T extends Subject & Reference>(subject: T): T {
    if (!this.done.has(subject)) {
      this.done.add(subject);
      const deletes = this.delOrInsForSubject(subject, '@delete');
      const inserts = this.delOrInsForSubject(subject, '@insert');
      for (let property of new Set(Object.keys(subject).concat(Object.keys(inserts ?? {})))) {
        switch (property) {
          case '@id': break;
          case '@list':
            this.updateList(subject, deletes, inserts);
            break;
          default:
            const subjectProperty = new SubjectPropertyUpdater(subject, property, this);
            subjectProperty.delete(...getValues(deletes ?? {}, property));
            subjectProperty.insert(...getValues(inserts ?? {}, property));
        }
      }
    }
    return subject;
  }

  /** @internal */
  updateValues(values: Iterable<any>) {
    for (let value of values)
      if (typeof value == 'object' && '@id' in value && !isReference(value))
        this.update(value);
  }

  private updateList(subject: GraphSubject, deletes?: GraphSubject, inserts?: GraphSubject) {
    if (isList(subject)) {
      if (isListUpdate(deletes) || isListUpdate(inserts))
        this.updateListIndexes(subject['@list'],
          isListUpdate(deletes) ? deletes['@list'] : {},
          isListUpdate(inserts) ? inserts['@list'] : {});
      this.updateValues(array(subject['@list']));
    }
  }

  private updateListIndexes(list: List['@list'], deletes: List['@list'], inserts: List['@list']) {
    const splice = typeof list.splice == 'function' ? list.splice : (() => {
      // Array splice operation must have a length field to behave
      const maxIndex = Math.max(...Object.keys(list).map(Number).filter(isNaturalNumber));
      list.length = isFinite(maxIndex) ? maxIndex + 1 : 0;
      return [].splice;
    })();
    const splices: { deleteCount: number, items?: any[] }[] = []; // Sparse
    for (let i in deletes)
      splices[i] = { deleteCount: 1 };
    for (let i in inserts)
      (splices[i] ??= { deleteCount: 0 }).items =
        // List updates are always expressed with identified slots, but the list
        // is assumed to contain direct items, not slots
        array(inserts[i]).map((slot: Slot) => this.update(slot)['@item']);
    let deleteCount = 0, items: any[] = [];
    for (let i of Object.keys(splices).reverse().map(Number)) {
      deleteCount += splices[i].deleteCount;
      items.unshift(...splices[i].items ?? []);
      if (!(i - 1 in splices)) {
        splice.call(list, i, deleteCount, ...items);
        deleteCount = 0;
        items = [];
      }
    }
  }
}

/** @internal */
function isListUpdate(updatePart?: Subject): updatePart is List {
  return updatePart != null && isList(updatePart);
}

/** @internal */
class SubjectPropertyUpdater {
  private readonly wasArray: boolean;
  private readonly configurable: boolean;

  constructor(
    readonly subject: Subject,
    readonly property: string,
    readonly subjectUpdater?: SubjectUpdater) {
    this.wasArray = isArray(this.subject[this.property]);
    this.configurable = Object.getOwnPropertyDescriptor(subject, property)?.configurable ?? false;
  }

  get values() {
    return getValues(this.subject, this.property);
  }

  set values(values: any[]) {
    // Apply deep updates to the final values
    this.subjectUpdater?.updateValues(values);
    // Per contract of updateSubject, this always L-value assigns (no pushing)
    this.subject[this.property] = values.length === 0 ? [] : // See next
      // Properties which were not an array before get collapsed
      values.length === 1 && !this.wasArray ? values[0] : values;
    if (values.length === 0 && this.configurable)
      delete this.subject[this.property];
  }

  delete(...values: any[]) {
    this.values = SubjectPropertyUpdater.minus(this.values, values);
  }

  insert(...values: any[]) {
    const object = this.subject[this.property];
    if (isPropertyObject(this.property, object) && isSet(object))
      object['@set'] = SubjectPropertyUpdater.union(array(object['@set']), values);
    else
      this.values = SubjectPropertyUpdater.union(this.values, values);
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
      return hasProperty(this.subject, this.property)
    }
  }

  private static union(values: any[], unionValues: any[]): any[] {
    return values.concat(SubjectPropertyUpdater.minus(unionValues, values));
  }

  private static minus(values: any[], minusValues: any[]): any[] {
    return values.filter(value => !minusValues.some(
      minusValue => compareValues(value, minusValue)));
  }
}

/**
 * Includes the given value in the Subject property, respecting **m-ld** data
 * semantics by expanding the property to an array, if necessary.
 * @param subject the subject to add the value to.
 * @param property the property that relates the value to the subject.
 * @param values the value to add.
 */
export function includeValues(subject: Subject, property: string, ...values: Value[]) {
  new SubjectPropertyUpdater(subject, property).insert(...values);
}

/**
 * Determines whether the given set of values contains the given value. This
 * method accounts for the identity semantics of {@link Reference}s and
 * {@link Subject}s.
 * @param subject the subject to inspect
 * @param property the property to inspect
 * @param value the value to find in the set. If `undefined`, then wildcard
 * checks for any value at all.
 */
export function includesValue(subject: Subject, property: string, value?: Value): boolean {
  return new SubjectPropertyUpdater(subject, property).exists(value);
}
