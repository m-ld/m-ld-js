import * as spec from '@m-ld/m-ld-spec';
import {
  Subject, Update, Reference, Variable, Read, Write
} from './jrql-support';
import { Observable, Subscription } from 'rxjs';
import { toArray } from 'rxjs/operators';
import { shortId } from './util';

/**
 * A convenience type for a struct with a `@insert` and `@delete` property, like
 * a {@link MeldUpdate}.
 */
export interface DeleteInsert<T> {
  '@delete': T;
  '@insert': T;
}

/**
 * A utility to generate a variable with a unique Id. Convenient to use when
 * generating query patterns in code.
 */
export function any(): Variable {
  return `?${shortId((nextAny++).toString(16))}`;
}
/** @internal */
let nextAny = 0;

// Unchanged from m-ld-spec
/** @see m-ld [specification](http://spec.m-ld.org/interfaces/livestatus.html) */
export type LiveStatus = spec.LiveStatus;
/** @see m-ld [specification](http://spec.m-ld.org/interfaces/meldstatus.html) */
export type MeldStatus = spec.MeldStatus;

/**
 * Convenience return type for reading data from a clone. Use as a
 * [Promise](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise)
 * with `.then` or `await` to obtain the results as an array of
 * {@link Subject}s. Use as an
 * [Observable](https://rxjs.dev/api/index/class/Observable) with `.subscribe`
 * (or other RxJS methods) to be notified of individual Subjects as they arrive.
 *
 * Note that all reads operate on a data snapshot, so results will not be
 * affected by concurrent writes, even outside the scope of a read procedure.
 *
 * @see {@link MeldStateMachine.read}
 */
export type ReadResult<T> = Observable<T> & PromiseLike<T[]>;

/** @internal */
export function readResult<T>(result: Observable<T>): ReadResult<T> {
  const then: PromiseLike<T[]>['then'] =
    (onfulfilled, onrejected) => result.pipe(toArray()).toPromise()
      .then(onfulfilled, onrejected);
  return Object.assign(result, { then });
}

/**
 * Read methods for a {@link MeldState}.
 *
 * Methods are typed to ensure that app code is aware of **m-ld**
 * [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics). See the
 * [Resource](/#resource) type for more details.
 */
export interface MeldReadState {
  /**
   * Actively reads data from the domain.
   *
   * The query executes in response to either subscription to the returned
   * result, or calling `.then`.
   *
   * All results are guaranteed to derive from the current state; however since
   * the observable results are delivered asynchronously, the current state is
   * not guaranteed to be live in the subscriber. In order to keep this state
   * alive during iteration (for example, to perform a consequential operation),
   * perform the read in the scope of a read procedure.
   *
   * @param request the declarative read description
   * @returns read subjects
   * @see {@link MeldStateMachine.read}
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
 * The `get` and `delete` methods are intentionally suggestive of a REST API,
 * which could be implemented by the class when used in a service environment.
 *
 * > 🚧 `put`, `post` and `patch` methods will be available in a future release.
 *
 * If a data state is not available ('live') for read and write operations, the
 * methods will throw. Liveness depends on how this state was obtained. A
 * mutable state such as a {@link MeldClone} is always live until it is closed.
 * An immutable state such as obtained in a read or write procedure is live
 * until either a write is performed in the procedure, or the procedure's
 * asynchronous return promise resolves or rejects.
 *
 * @see {@link MeldStateMachine.read}
 * @see {@link MeldStateMachine.write}
 * @see m-ld [specification](http://spec.m-ld.org/interfaces/meldupdate.html)
 */
export interface MeldState extends MeldReadState {
  /**
   * Actively writes data to the domain.
   *
   * As soon as this method is called, this current state is no longer 'live'
   * ({@link write} will throw). To keep operating on state, use the returned
   * new state.
   *
   * @param request the declarative write description
   * @typeParam W one of the {@link Write} types
   * @returns the next state of the domain, changed by this write operation only
   */
  write<W = Write>(request: W): Promise<MeldState>;
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
 * A function type specifying a 'procedure' during which a clone state is
 * available as immutable. Strictly, the immutable state is guaranteed to remain
 * 'live' until the procedure's return Promise resolves or rejects.
 *
 * @typeParam S can be {@link MeldReadState} (default) or {@link MeldState}. If
 * the latter, the state can be transitioned to another immutable state using
 * {@link MeldState.write}.
 */
export type StateProc<S extends MeldReadState = MeldReadState> =
  (state: S) => PromiseLike<unknown> | void;

/**
 * A function type specifying a 'procedure' during which a clone state is
 * available as immutable following an update. Strictly, the immutable state is
 * guaranteed to remain 'live' until the procedure's return Promise resolves or
 * rejects.
 */
export type UpdateProc =
  (update: MeldUpdate, state: MeldReadState) => PromiseLike<unknown> | void;

/**
 * A m-ld state machine extends the {@link MeldState} API for convenience, but
 * note that the state of a machine is inherently mutable. For example, two
 * consecutive asynchronous reads will not necessarily operate on the same
 * state. To work with immutable state, use the `read` and `write` method
 * overloads taking a procedure.
 */
export interface MeldStateMachine extends MeldState {
  /**
   * Handle updates from the domain, from the moment this method is called. All
   * data changes are signalled through the handler, strictly ordered according
   * to the clone's logical clock. The updates can therefore be correctly used
   * to maintain some other view of data, for example in a user interface or
   * separate database. This will include the notification of 'rev-up' updates
   * after a connect to the domain. To change this behaviour, ignore updates
   * while the clone status is marked as `outdated`.
   *
   * This method is equivalent to calling {@link read} with a no-op procedure.
   *
   * @param handler a procedure to run for every update
   * @returns a subscription, allowing the caller to unsubscribe the handler
   */
  follow(handler: UpdateProc): Subscription;

  /**
   * Performs some read procedure on the current state, with notifications of
   * subsequent updates.
   *
   * The state passed to the procedure is immutable and is guaranteed to remain
   * 'live' until the procedure's return Promise resolves or rejects.
   *
   * @param procedure a procedure to run for the current state
   * @param handler a procedure to run for every update that follows the state
   * in the procedure
   */
  read(procedure: StateProc, handler?: UpdateProc): Subscription;

  /**
   * Actively reads data from the domain.
   *
   * The query executes in response to either subscription to the returned
   * result, or calling `.then`.
   *
   * All results are guaranteed to derive from the current state; however since
   * the observable results are delivered asynchronously, the current state is
   * not guaranteed to be live in the subscriber. In order to keep this state
   * alive during iteration (for example, to perform a consequential operation),
   * perform the request in the scope of a read procedure instead.
   *
   * @param request the declarative read description
   * @returns read subjects
   */
  read<R extends Read = Read, S = Subject>(request: R): ReadResult<Resource<S>>;

  /**
   * Performs some write procedure on the current state.
   *
   * The state passed to the procedure is immutable and is guaranteed to remain
   * 'live' until its `write` method is called, or the procedure's return
   * Promise resolves or rejects.
   *
   * @param procedure a procedure to run against the current state. This
   * procedure is able to modify the given state incrementally using its `write`
   * method
   */
  write(procedure: StateProc<MeldState>): Promise<MeldState>;

  /**
   * Actively writes data to the domain.
   *
   * @param request the declarative write description
   * @typeParam W one of the {@link Write} types
   * @returns the next state of the domain, changed by this write operation only
   */
  write<W = Write>(request: W): Promise<MeldState>;
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
   * @param state a read-only view of data from the clone prior to the update
   * @param update the provisional update, prior to application to the data
   * @returns a rejection if the constraint is violated (or fails)
   */
  check(state: MeldReadState, update: MeldUpdate): Promise<unknown>;
  /**
   * Applies the constraint to an update being applied to the data. If the
   * update would cause a violation, this method must mutate the given update
   * using its `append` method, to resolve the violation.
   * @param state a read-only view of data from the clone prior to the update
   * @param update the provisional update, prior to application to the data
   * @returns a rejection only if the constraint application fails
   */
  apply(state: MeldReadState, update: MutableMeldUpdate): Promise<unknown>;
}

/**
 * An update to which further updates can be appended.
 */
export interface MutableMeldUpdate extends MeldUpdate {
  append(update: Update): Promise<unknown>;
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