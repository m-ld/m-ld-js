import { isList, List, Reference, Slot, Subject } from './jrql-support';
import { GraphSubject, GraphUpdate, UpdateForm } from './api';
import { clone, getValues } from './engine/jsonld';
import { isNaturalNumber } from './engine/util';
import { array } from './util';
import { isReference } from 'json-rql';
import { SubjectPropertyValues } from './subjects';

/**
 * Simplified form of {@link GraphUpdate}, with plain Subject arrays
 * @category Utility
 */
export type SubjectsUpdate = UpdateForm<GraphSubject[]>;
/**
 * An update to a single graph Subject.
 * @category Utility
 */
export type SubjectUpdate = UpdateForm<GraphSubject>;
/**
 * A **m-ld** update notification, indexed by graph Subject ID.
 * @category Utility
 */
export type SubjectUpdates = { [id: string]: SubjectUpdate };
/** @internal */
const graphUpdateKeys = ['@delete', '@insert', '@update'];
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
 *
 * @param update the update to convert
 * @param copy if flagged, each subject in the update is cloned
 * @category Utility
 */
export function asSubjectUpdates(update: SubjectsUpdate, copy?: true): SubjectUpdates {
  const su: SubjectUpdates = {};
  for (let key of graphUpdateKeys as (keyof GraphUpdate)[]) {
    for (let subject of update[key] ?? []) {
      Object.assign(
        su[subject['@id']] ??= {},
        { [key]: copy ? clone(subject) : subject }
      );
    }
  }
  return su;
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
 * @param ignoreSharedData if `false`, any shared data expressions in the update
 * will cause a `RangeError` – useful in development to catch problems early
 * {@link asSubjectUpdates}
 * @typeParam T the app-specific subject type of interest
 * @see [m-ld data semantics](http://spec.m-ld.org/#data-semantics)
 * @category Utility
 */
export function updateSubject<T extends Subject & Reference>(
  subject: T,
  update: SubjectUpdates | GraphUpdate,
  ignoreSharedData = true
): T {
  return new SubjectUpdater(update, ignoreSharedData).update(subject);
}

/** @internal */
function isGraphUpdate(update: SubjectUpdates | GraphUpdate): update is GraphUpdate {
  return Object.keys(update).some(key => graphUpdateKeys.includes(key));
}

/**
 * Applies an update to more than one subject. Subjects (by Javascript
 * reference) will not be updated more than once, even if multiply-referenced.
 *
 * @see {@link updateSubject}
 * @category Utility
 */
export class SubjectUpdater {
  private readonly verbForSubject:
    (subject: GraphSubject, key: keyof GraphUpdate) => GraphSubject | undefined;
  private readonly done = new Set<object>();

  constructor(
    update: SubjectUpdates | GraphUpdate,
    readonly ignoreSharedData = true
  ) {
    if (isGraphUpdate(update)) {
      this.verbForSubject = (subject, key) =>
        update[key].graph.get(subject['@id']);
    } else {
      this.verbForSubject = (subject, key) =>
        subject['@id'] in update ? update[subject['@id']][key] : undefined;
    }
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
      if (!this.ignoreSharedData && this.verbForSubject(subject, '@update') != null)
        throw new RangeError('Subject updater cannot apply shared data type updates');
      const deletes = this.verbForSubject(subject, '@delete');
      const inserts = this.verbForSubject(subject, '@insert');
      for (let property of new Set(Object.keys(subject).concat(Object.keys(inserts ?? {})))) {
        switch (property) {
          case '@id':
            break;
          case '@list':
            this.updateList(subject, deletes, inserts);
            break;
          default:
            const subjectProperty = new SubjectPropertyValues(subject, property, this.updateValues);
            subjectProperty.update(
              getValues(deletes ?? {}, property),
              getValues(inserts ?? {}, property));
        }
      }
    }
    return subject;
  }

  /** @internal */
  updateValues = (values: Iterable<any>) => {
    for (let value of values)
      if (typeof value == 'object' && '@id' in value && !isReference(value))
        this.update(value);
  }

  private updateList(subject: GraphSubject, deletes?: GraphSubject, inserts?: GraphSubject) {
    if (isList(subject)) {
      if (isListUpdate(deletes) || isListUpdate(inserts)) {
        this.updateListIndexes(subject['@list'],
          isListUpdate(deletes) ? deletes['@list'] : {},
          isListUpdate(inserts) ? inserts['@list'] : {});
      }
      this.updateValues(array(subject['@list']));
    }
  }

  private updateListIndexes(list: List['@list'], deletes: List['@list'], inserts: List['@list']) {
    const splice = typeof list.splice == 'function' ? list.splice : (() => {
      // Array splice operation must have a length field to behave
      if (!('length' in list)) {
        const maxIndex = Math.max(...Object.keys(list).map(Number).filter(isNaturalNumber));
        list.length = isFinite(maxIndex) ? maxIndex + 1 : 0;
      }
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