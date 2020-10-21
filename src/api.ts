import * as spec from '@m-ld/m-ld-spec';
import {
  Subject, Update, Value, isValueObject, Reference, Variable, Read, Write
} from './jrql-support';
import { Observable, Subscription } from 'rxjs';
import { toArray } from 'rxjs/operators';
import { array, shortId } from './util';

/**
 * A convenience type for a struct with a `@insert` and `@delete` property, like
 * a {@link MeldUpdate}.
 */
export interface DeleteInsert<T> {
  '@delete': T;
  '@insert': T;
}

/**
 * A utility to generate a variable with a random Id. Convenient to use when
 * generating query patterns in code.
 */
export function any(): Variable {
  return `?${shortId(4)}`;
}

// Unchanged from m-ld-spec
/** @see m-ld [specification](http://spec.m-ld.org/interfaces/livestatus.html) */
export type LiveStatus = spec.LiveStatus;
/** @see m-ld [specification](http://spec.m-ld.org/interfaces/meldstatus.html) */
export type MeldStatus = spec.MeldStatus;

export type ReadResult<T> = Observable<T> & PromiseLike<T[]>;

export function readResult<T>(result: Observable<T>): ReadResult<T> {
  const then: PromiseLike<T[]>['then'] =
    (onfulfilled, onrejected) => result.pipe(toArray()).toPromise()
      .then(onfulfilled, onrejected);
  return Object.assign(result, { then });
}

/**
 * Read methods for a {@link MeldState}
 */
export interface MeldReadState {
  /**
   * Actively reads data from the domain.
   *
   * The query executes in response to the first subscription to the returned
   * stream, and subsequent subscribers will share the same results stream.
   *
   * All results are guaranteed to derive from the current state; however since
   * the observable results are delivered asynchronously, the current state is
   * not guaranteed to be live in the subscriber. In order to keep this state
   * alive during iteration (for example, to perform a consequential operation),
   * do not resolve the transaction until you're done.
   *
   * @param request the declarative read description
   * @returns an observable stream of subjects.
   * @see {@link MeldClone.transact}
   */
  read<R extends Read = Read, S = Subject>(request: R): ReadResult<Resource<S>>;
  /**
   * Shorthand method for retrieving a single Subject by its `@id`, if it exists.
   * 
   * @param id the Subject `@id`
   * @returns a promise resolving to the requested Subject, or `undefined` if not found
   */
  get<S = Subject>(id: string): Promise<Resource<S> | undefined>;
}

/**
 * A data state corresponding to a local clock tick. A state can be some initial
 * state (an new clone), or follow a write operation, which may have been
 * transacted locally in this clone, or remotely on another clone.
 *
 * - Methods are typed to ensure that app code is aware of **m-ld**
 *   [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics). See the
 *   [Resource](/#resource) type for more details.
 * - Static utility methods are provided to help update app views of data based
 *   on updates notified via the {@link read} method.
 *
 * The `get` and `delete` methods are intentionally suggestive of a REST API,
 * which could be implemented by the class when used in a service environment.
 *
 * > 🚧 `put`, `post` and `patch` methods will be available in a future release.
 *
 * If a data state is not 'live' for read and write operations, the methods will
 * throw.
 *
 * A data state signalled from a write operation or a follow will always be
 * live. If a consumer needs to keep a state live while waiting for an
 * asynchronous operation to complete (including any reads), it should pass a
 * procedure to the transact method. Otherwise, liveness ends aggressively as
 * soon as the state object goes out of the current synchronous function scope.
 *
 * @see m-ld [specification](http://spec.m-ld.org/interfaces/meldupdate.html)
 */
export interface MeldState extends MeldReadState {
  /**
   * Actively writes data to the domain.
   *
   * As soon as this method is called, this current state is no longer alive
   * ({@link write} will throw). To keep operating on state, use the returned
   * new state.
   *
   * The returned new state will 
   *
   * @param request the declarative write description
   * @returns the next state of the domain, changed by this write operation only
   */
  write<W extends Write>(request: W): Promise<MeldState>;
  /**
   * Shorthand method for deleting a single Subject by its `@id`. This will also
   * remove references to the given Subject from other Subjects.
   * @param id the Subject `@id`
   */
  delete(id: string): Promise<MeldState>;
}

/**
 * @see m-ld [specification](http://spec.m-ld.org/interfaces/meldupdate.html)
 */
export interface MeldUpdate extends spec.MeldUpdate {
  '@delete': Subject[];
  '@insert': Subject[];
}

/**
 * A m-ld state machine extends the {@link MeldState} API for convenience, but
 * note that the state of a machine is inherently mutable. For example, two
 * consecutive asynchronous reads will not necessarily operate on the same
 * state. To work with immutable state, use the `read` and `write` method
 * overloads taking a procedure.
 */
export interface MeldStateMachine extends MeldState {
  read(procedure: (state: MeldReadState) => PromiseLike<unknown> | void,
    handler?: (update: MeldUpdate, state: MeldReadState) => PromiseLike<unknown> | void): Subscription;
  read<R extends Read = Read, S = Subject>(request: R): ReadResult<Resource<S>>;

  write(procedure: (state: MeldState) => PromiseLike<unknown> | void): Promise<MeldState>;
  write<W extends Write>(request: W): Promise<MeldState>;
}

/**
 * A **m-ld** clone represents domain data to an app.
 *
 * The Javascript clone engine uses a database engine, for which in-memory,
 * on-disk and in-browser persistence options are available (see
 * [Getting&nbsp;Started](/#getting-started)).
 *
 * @see https://spec.m-ld.org/interfaces/meldclone.html
 */
export interface MeldClone extends MeldStateMachine {
  /**
   * The current and future status of a clone. This stream is hot and
   * continuous, terminating when the clone closes (and can therefore be used to
   * detect closure).
   */
  readonly status: Observable<MeldStatus> & LiveStatus;
  /**
   * Closes this clone engine gracefully. Using this method ensures that data
   * has been fully flushed to storage and all transactions have been notified
   * to the domain (if this clone is online).
   * @param err used to notify a reason for the closure, for example an
   * application failure, for problem diagnosis.
   */
  close(err?: any): Promise<unknown>;
}

/**
 * A constraint asserts an invariant for data in a clone. When making
 * transactions against the clone, the constraint is 'checked', and violating
 * transactions fail.
 *
 * Constraints are also 'applied' for incoming updates from other clones. This
 * is because a constraint may be violated as a result of data changes in either
 * clone. In this case, the constraint must resolve the violation by application
 * of some rule.
 *
 * > 🚧 *Data constraints are currently an experimental feature. Please
 * > [contact&nbsp;us](mailto:info@m-ld.io) to discuss constraints required for
 * > your use-case.*
 *
 * In this clone engine, constraints are checked and applied for updates prior
 * to their application to the data (the updates are 'provisional'). If the
 * constraint requires to know the final state, it must infer it from the given
 * reader and the update.
 *
 * @see http://m-ld/org/doc/#concurrency
 */
export interface MeldConstraint {
  /**
   * Check the given update does not violate this constraint.
   * @param update the provisional update, prior to application to the data
   * @param state a read-only view of data from the clone prior to the update
   * @returns a rejection if the constraint is violated (or fails)
   */
  check(state: MeldReadState, update: MeldUpdate): Promise<unknown>;
  /**
   * Applies the constraint to an update being applied to the data. If the
   * update would cause a violation, this method must provide an Update which
   * resolves the violation.
   * @param update the provisional update, prior to application to the data
   * @param state a read-only view of data from the clone prior to the update
   * @returns `null` if no violation is found. Otherwise, an Update the resolves
   * the violation so that the constraint invariant is upheld.
   */
  apply(state: MeldReadState, update: MeldUpdate): Promise<Update | null>;
}

/**
 * Captures **m-ld** [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics)
 * as applied to an app-specific subject type `T`. Applies the following changes
 * to `T`:
 * - Always includes an `@id` property, as the subject identity
 * - Non-array properties are redefined to allow arrays (note this is
 *   irrespective of any `single-valued` constraint, which is applied at
 *   runtime)
 * - Required properties are redefined to allow `undefined`
 *
 * Since any property can have zero to many values, it may be convenient to
 * combine use of this type with the {@link array} utility when processing
 * updates.
 *
 * Note that a Resource always contains concrete data, unlike Subject, which can
 * include variables and filters as required for its role in query
 * specification.
 * 
 * @typeParam T the app-specific subject type of interest
 */
export type Resource<T> = Subject & Reference & {
  [P in keyof T]: T[P] extends Array<unknown> ? T[P] | undefined : T[P] | T[P][] | undefined;
};

export namespace MeldClone {
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
  export type SubjectUpdates = { [id: string]: DeleteInsert<Subject> };

  /** @internal */
  function bySubject(update: DeleteInsert<Subject[]>,
    key: '@insert' | '@delete', bySubject: SubjectUpdates = {}): SubjectUpdates {
    return update[key].reduce((byId, subject) =>
      ({ ...byId, [subject['@id'] ?? '*']: { ...byId[subject['@id'] ?? '*'], [key]: subject } }), bySubject);
  }

  /**
   * Applies a subject update to the given subject, expressed as a
   * {@link Resource}. This method will correctly apply the deleted and inserted
   * properties from the update, accounting for **m-ld**
   * [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics).
   * @param subject the resource to apply the update to
   * @param update the update, obtained from an {@link asSubjectUpdates} transformation
   */
  export function update<T>(subject: Resource<T>, update: DeleteInsert<Subject>): Resource<T> {
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
    // TODO support value objects
    if (isSubjectOrRef(value)) {
      return !!value['@id'] && set.filter(isSubjectOrRef).map(v => v['@id']).includes(value['@id']);
    } else {
      return set.includes(value);
    }
  }

  function isSubjectOrRef(value: Value): value is Subject | Reference {
    return typeof value == 'object' && !isValueObject(value);
  }
}