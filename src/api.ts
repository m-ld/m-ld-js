import * as spec from '@m-ld/m-ld-spec';
import { Read, Reference, Subject, SubjectProperty, Update, Variable, Write } from './jrql-support';
import { firstValueFrom, Observable, Subscription } from 'rxjs';
import { toArray } from 'rxjs/operators';
import { shortId } from './util';
import { Iri } from 'jsonld/jsonld-spec';
import { SubjectGraph } from './engine/SubjectGraph';

/**
 * A convenience type for a struct with a `@insert` and `@delete` property, like
 * a {@link MeldUpdate}.
 */
export interface DeleteInsert<T> {
  readonly '@delete': T;
  readonly '@insert': T;
}

/** @internal */
export function isDeleteInsert(o: any): o is DeleteInsert<unknown> {
  return '@insert' in o && '@delete' in o;
}

/**
 * A utility to generate a variable with a unique Id. Convenient to use when
 * generating query patterns in code.
 */
export const any = (): Variable => `?${anyName()}`
/**
 * A utility to generate a unique blank node.
 */
export const blank = () => '_:' + anyName();
/** @internal */
let nextAny = 0x1111;
/** @internal */
export const anyName = (): string => shortId((nextAny++).toString(16))

// Unchanged from m-ld-spec
/** @see m-ld [specification](http://spec.m-ld.org/interfaces/livestatus.html) */
export type LiveStatus = spec.LiveStatus;
/** @see m-ld [specification](http://spec.m-ld.org/interfaces/meldstatus.html) */
export type MeldStatus = spec.MeldStatus;

/**
 * Convenience return type for reading data from a clone. Use as a
 * [Promise](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise)
 * with `.then` or `await` to obtain the results as an array of identified
 * {@link Subject}s. Use as an
 * [Observable](https://rxjs.dev/api/index/class/Observable) with `.subscribe`
 * (or other RxJS methods) to be notified of individual Subjects as they arrive.
 *
 * Note that all reads operate on a data snapshot, so results will not be
 * affected by concurrent writes, even outside the scope of a read procedure.
 *
 * @see {@link MeldStateMachine.read}
 */
export type ReadResult = Observable<GraphSubject> & PromiseLike<GraphSubjects>;

/** @internal */
export function readResult(result: Observable<GraphSubject>): ReadResult {
  const then: PromiseLike<GraphSubjects>['then'] =
    (onfulfilled, onrejected) => firstValueFrom(result.pipe(toArray<GraphSubject>()))
      .then(onfulfilled == null ? null : graph => onfulfilled(new SubjectGraph(graph)), onrejected);
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
  read<R extends Read = Read>(request: R): ReadResult;
  /**
   * Shorthand method for retrieving a single Subject by its `@id`, if it exists.
   * 
   * @param id the Subject `@id`
   * @returns a promise resolving to the requested Subject, or `undefined` if not found
   */
  get(id: string): Promise<GraphSubject | undefined>;
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
 * A fully-identified Subject from the backend.
 */
export type GraphSubject = Readonly<Subject & Reference>;

/**
 * Convenience for collections of identified Subjects, such as found in a
 * {@link MeldUpdate}. Extends `Array` and serialisable to JSON-LD as such.
 */
export interface GraphSubjects extends Array<GraphSubject> {
  /**
   * Subjects in the collection indexed by `@id`. In addition, if a Subject in
   * the graph references another Subject in the same graph, the reference is
   * realised as a Javascript reference, so it is possible to traverse the graph
   * from some known root object. This means that the graph cannot necessarily
   * by serialised to JSON as it may not be acyclic.
   */
  graph: ReadonlyMap<Iri, GraphSubject>;
}

/**
 * @see m-ld [specification](http://spec.m-ld.org/interfaces/meldupdate.html)
 */
export interface MeldUpdate extends DeleteInsert<GraphSubjects> {
  /**
   * Partial subjects, containing properties that have been deleted from the
   * domain. Note that deletion of a property (even of all properties) does not
   * necessarily indicate that the subject's identity is not longer represented
   * in the domain.
   */
  readonly '@delete': GraphSubjects;
  /**
   * Partial subjects, containing properties that have been inserted into the
   * domain.
   */
  readonly '@insert': GraphSubjects;
  /**
   * Current local clock ticks at the time of the update.
   * @see MeldStatus.ticks
   */
  readonly '@ticks': number;
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
   * @typeParam R one of the {@link Read} types
   * @returns read subjects
   */
  read<R extends Read = Read>(request: R): ReadResult;

  /**
   * Performs some write procedure on the current state.
   *
   * The state passed to the procedure is immutable and is guaranteed to remain
   * 'live' until its `write` method is called, or the procedure's return
   * Promise resolves or rejects.
   *
   * @param procedure a procedure to run against the current state. This
   * procedure is able to modify the given state, incrementally using its
   * `write` method
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
 * @see [m-ld Specification](https://spec.m-ld.org/interfaces/meldclone.html)
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
 * > [contact&nbsp;us](https://m-ld.org/hello/) to discuss constraints required for
 * > your use-case.*
 *
 * In this clone engine, constraints are checked and applied for updates prior
 * to their application to the data (the updates are 'interim'). If the
 * constraint requires to know the final state, it must infer it from the given
 * reader and the update.
 *
 * @see [m-ld concurrency](http://m-ld/org/doc/#concurrency)
 */
export interface MeldConstraint {
  /**
   * Check the given update does not violate this constraint.
   * @param state a read-only view of data from the clone prior to the update
   * @param update the interim update, prior to application to the data
   * @returns a rejection if the constraint is violated (or fails)
   */
  check(state: MeldReadState, update: InterimUpdate): Promise<unknown>;
  /**
   * Applies the constraint to an update being applied to the data. If the
   * update would cause a violation, this method must mutate the given update
   * using its `assert` method, to resolve the violation.
   * @param state a read-only view of data from the clone prior to the update
   * @param update the interim update, prior to application to the data
   * @returns a rejection only if the constraint application fails
   */
  apply(state: MeldReadState, update: InterimUpdate): Promise<unknown>;
}

/**
 * An update to which further updates can be asserted or entailed.
 */
export interface InterimUpdate {
  /**
   * An assertion is an update that maintains the data integrity of the domain
   * by changing data that the app created, or that was the result of a prior
   * assertion.
   *
   * An assertion is incorporated into the final update sent to other clones and
   * echoed to the local app. Examples of assertions:
   * - Delete previous value from a single-value register
   * - Rewrite a list index predicate to a CRDT-specific form
   *
   * @param update the update to assert into the domain
   * @see {@link ready}
   */
  assert(update: Update): void;
  /**
   * An entailment is an update that maintains the data integrity of a domain by
   * changing only entailed data. Entailed data is data that can be deduced
   * automatically, and can only be created by entailment, and not by an app.
   *
   * An entailment is not included in updates sent to other clones or to the
   * local app. Examples of entailments:
   * - The size of a collection
   * - Membership of a duck-type class
   *
   * @param update the update to entail into the domain
   * @see {@link ready}
   */
  entail(update: Update): void;
  /**
   * Removes assertions (not entailments) from this update, prior to application
   * to the dataset. This is used to rewrite assertions made by the application,
   * or the work of prior constraints.
   *
   * Removal of an assertion is possible:
   * 1. during constraint checking,
   * 2. during constraint application, if the assertion was made by another
   *    constraint. An attempt to remove an assertion made in the original
   *    update will be ignored, which may lead to unexpected results.
   *
   * @param key Whether to remove `@delete` or `@insert` components
   * @param pattern the Subject assertions to remove
   * @see {@link ready}
   */
  remove(key: keyof DeleteInsert<any>, pattern: Subject | Subject[]): void;
  /**
   * Substitutes the given alias for the given property subject, property, or
   * subject and property, in updates provided to the application. This allows a
   * constraint to hide a data implementation detail.
   *
   * @param subjectId the subject to which the alias applies
   * @param property if `@id`, the subject IRI is aliased. Otherwise, the
   * property is aliased.
   * @param alias the alias for the given subject and/or property. It is an
   * error if the property is `@id` and a `SubjectProperty` alias is provided.
   */
  alias(subjectId: Iri | null, property: '@id' | Iri, alias: Iri | SubjectProperty): void;
  /**
   * A promise that resolves to the current update. If any modifications made by
   * the methods above have affected the `@insert` and `@delete` of the update,
   * they will have been applied.
   */
  readonly update: Promise<MeldUpdate>;
}