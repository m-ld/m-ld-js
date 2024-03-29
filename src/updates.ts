import { isPropertyObject, Reference, Subject } from './jrql-support';
import { GraphSubject, GraphSubjects, GraphUpdate, isGraphSubjects, UpdateForm } from './api';
import { clone } from './engine/jsonld';
import { isReference } from 'json-rql';
import { SubjectLike, SubjectPropertyValues } from './subjects';
import { isArray, iterable, mapObject } from './engine/util';
import { SubjectGraph } from './engine/SubjectGraph';

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
export type SubjectUpdates = {
  [id: string]: SubjectUpdate
};
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

/** @internal */
function *idsInUpdate(update: SubjectsUpdate) {
  // Output unique ids
  const done = new Set<string>();
  for (let key of graphUpdateKeys as (keyof GraphUpdate)[]) {
    for (let subject of update[key] ?? []) {
      const id = subject['@id'];
      if (!done.has(id)) {
        done.add(id);
        yield id;
      }
    }
  }
}

/**
 * Things that can be interpreted as an update to graph subjects
 * @category Utility
 */
export type SubjectUpdateLike =
  SubjectUpdates | SubjectsUpdate | GraphSubject | GraphSubject[] | undefined;

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
 * Changes to text represented as a {@link SharedDatatype shared datatype}
 * supporting the `@splice` operator will be correctly applied to plain strings,
 * and can also be applied to any Object having a `splice` method taking three
 * parameters, `index`, `deleteCount` and `content`. This allows an app to
 * efficiently apply character-level changes, e.g. to document editing
 * components. The proxy object should also implement a `toJSON()` method which
 * returns the current string state.
 *
 * CAUTION: If this function is called independently on subjects which reference
 * each other via Javascript references, or share referenced subjects, then the
 * referenced subjects may be updated more than once, with unexpected results.
 * To avoid this, use a {@link SubjectUpdater} to process the whole update.
 *
 * @param subject the resource to apply the update to
 * @param update the update, as a {@link MeldUpdate} or obtained from {@link asSubjectUpdates}
 * @param ignoreUnsupported if `false`, any unsupported data expressions in the
 * update will cause a `RangeError` – useful in development to catch problems
 * early
 * @typeParam T the app-specific subject type of interest
 * @see [m-ld data semantics](http://spec.m-ld.org/#data-semantics)
 * @category Utility
 */
export function updateSubject<T extends SubjectLike & Reference>(
  subject: T,
  update: SubjectUpdateLike,
  ignoreUnsupported = true
): T {
  return new SubjectUpdater(update, ignoreUnsupported).update(subject);
}

/** @internal */
function isSubjectsUpdate(update: SubjectUpdateLike): update is SubjectsUpdate {
  return update != null &&
    Object.keys(update).some(key => graphUpdateKeys.includes(key));
}

/** @internal */
function getGraph(update: GraphSubject[]): GraphSubjects['graph'] {
  return isGraphSubjects(update) ? update.graph : new SubjectGraph(update).graph;
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
    (subject: SubjectLike & Reference, key: keyof GraphUpdate) => Subject | undefined;
  private readonly done = new Set<object>();

  readonly affectedIds: Iterable<string>;

  constructor(
    update: SubjectUpdateLike,
    readonly ignoreUnsupported = true
  ) {
    if (update == null) {
      this.verbForSubject = () => undefined;
      this.affectedIds = [];
    } else if (isArray(update)) {
      const graph = getGraph(update);
      this.verbForSubject = (subject, key) =>
        key === '@insert' ? graph.get(subject['@id']) : undefined;
      this.affectedIds = iterable(() => graph.keys());
    } else if ('@id' in update) {
      this.verbForSubject = (subject, key) =>
        key === '@insert' && update['@id'] === subject['@id'] ? update : undefined;
      this.affectedIds = [update['@id'] as string];
    } else if (isSubjectsUpdate(update)) {
      const graphical = mapObject(update, (k, v) =>
        graphUpdateKeys.includes(k) ? { [k]: getGraph(v) } : undefined);
      this.verbForSubject = (subject, key) => graphical[key].get(subject['@id']);
      this.affectedIds = iterable(() => idsInUpdate(update));
    } else {
      this.verbForSubject = (subject, key) =>
        subject['@id'] in update ? update[subject['@id']][key] : undefined;
      this.affectedIds = Object.keys(update);
    }
  }

  /**
   * Applies an update to the given subject in-place.
   *
   * @returns the given subject, for convenience
   * @see {@link updateSubject}
   */
  update<T extends SubjectLike & Reference>(subject: T): T {
    if (!this.done.has(subject)) {
      this.done.add(subject);
      const deletes = this.verbForSubject(subject, '@delete');
      const inserts = this.verbForSubject(subject, '@insert');
      const updates = this.verbForSubject(subject, '@update');
      for (let property of new Set(Object.keys(subject).concat(Object.keys(inserts ?? {})))) {
        if (isPropertyObject(property, 'any')) {
          SubjectPropertyValues
            .for(subject, property, this.updateValues, this.ignoreUnsupported)
            .update(deletes, inserts, updates);
        }
      }
    }
    return subject;
  }

  /** @internal */
  updateValues = (values: Iterable<any>, unref = false) => {
    for (let value of values)
      if (typeof value == 'object' && '@id' in value && (unref || !isReference(value)))
        this.update(value);
  };
}
