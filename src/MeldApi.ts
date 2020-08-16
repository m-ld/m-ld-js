import { generate } from 'short-uuid';
import * as spec from '@m-ld/m-ld-spec';
import {
  Context, Subject, Describe, Pattern, Update, Value, isValueObject, Reference, Variable, Read
} from './jrql-support';
import { Observable } from 'rxjs';
import { map, flatMap, toArray as rxToArray, take } from 'rxjs/operators';
import { flatten } from 'jsonld';

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
/**
 * An update event signalling a write operation, which may have been transacted
 * locally in this clone, or remotely on another clone.
 * @see m-ld [specification](http://spec.m-ld.org/interfaces/meldupdate.html)
 */
export interface MeldUpdate extends spec.MeldUpdate {
  '@delete': Subject[];
  '@insert': Subject[];
}

/**
 * A means to access the local clock tick of a transaction response.
 */
export interface HasExecTick {
  /**
   * The promise will be resolved with the local clone clock tick of the
   * transaction. For a read, this will be the tick from which the results were
   * obtained. For a write, this is the tick of the transaction completion. In
   * both cases, the promise resolution is a microtask in the event loop
   * iteration corresponding to the given clone clock tick. Therefore, `follow`
   * can be immediately called and the result subscribed, to be notified of
   * strictly subsequent updates.
   *
   * Note that this means the actual tick value is typically redundant, for this
   * engine. It's provided for consistency and in case of future use.
   */
  readonly tick: Promise<number>;
}

/**
 * A **m-ld** clone represents domain data to an app.
 *
 * The Javascript clone engine uses a database engine, for which in-memory,
 * on-disk and in-browser persistence options are available (see
 * [Getting&nbsp;Started](/#getting-started)).
 *
 * The raw API methods of this class are augmented with convenience methods for
 * use by an app in the [`MeldApi`](/classes/meldapi.html) class.
 *
 * @see https://spec.m-ld.org/interfaces/meldclone.html
 */
export interface MeldClone {
  /**
   * Actively writes data to, or reads data from, the domain.
   *
   * The transaction executes once, asynchronously, and the results are notified
   * to subscribers of the returned stream.
   *
   * For write requests, the query executes as soon as possible. The result is
   * only completion or error of the returned observable stream – no Subjects
   * are signalled.
   *
   * For read requests, the query executes in response to the first subscription
   * to the returned stream, and subsequent subscribers will share the same
   * results stream.
   *
   * @param request the declarative transaction description
   * @returns an observable stream of subjects. For a write transaction, this is
   * empty, but indicates final completion or error of the transaction.
   */
  transact(request: Pattern): Observable<Subject> & HasExecTick;
  /**
   * Follow updates from the domain. All data changes are signalled through the
   * returned stream, strictly ordered according to the clone's logical clock.
   * The updates can therefore be correctly used to maintain some other view of
   * data, for example in a user interface or separate database.
   *
   * In this engine, the returned stream will signal all updates after the event
   * loop tick on which the stream is subscribed. To ensure that all updates are
   * observed, call this method immediately on receipt of the clone object, and
   * synchronously subscribe.
   *
   * This method will include the notification of 'rev-up' updates after a
   * connect to the domain. To change this behaviour, also subscribe to `status`
   * changes and ignore updates while the status is marked as `outdated`.
   *
   * @returns an observable stream of updates from the domain.
   */
  follow(): Observable<MeldUpdate>;
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
 * Interface provided to a {@link MeldConstraint} to read data during checking
 * and application of a constraint.
 * @param request the Read request, e.g. a Select or Describe
 * @returns an observable stream of found Subjects
 */
export type MeldReader = <R extends Read>(request: R) => Observable<Subject>;

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
   * @param read a way to read data from the clone at the time of the update
   * @returns a rejection if the constraint is violated (or fails)
   */
  check(update: MeldUpdate, read: MeldReader): Promise<unknown>;
  /**
   * Applies the constraint to an update being applied to the data. If the
   * update would cause a violation, this method must provide an Update which
   * resolves the violation.
   * @param update the provisional update, prior to application to the data
   * @param read a way to read data from the clone at the time of the update
   * @returns `null` if no violation is found. Otherwise, an Update the resolves
   * the violation so that the constraint invariant is upheld.
   */
  apply(update: MeldUpdate, read: MeldReader): Promise<Update | null>;
}

/**
 * Utility to normalise a property value according to **m-ld**
 * [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics), from a missing
 * value (`null` or `undefined`), a single value, or an array of values, to an
 * array of values (empty for missing values). This can simplify processing of
 * property values in common cases.
 * @param value the value to normalise to an array
 */
export function array<T>(value?: T | T[]): T[] {
  return value == null ? [] : ([] as T[]).concat(value).filter(v => v != null);
}

/**
 * Utility to generate a short Id according to the given spec.
 * @param spec If a number, a random Id will be generated with the given length.
 * If a string, an obfuscated Id will be deterministically generated for the
 * string (passing the same spec again will generate the same Id).
 * @return a string identifier that is safe to use as an HTML (& XML) element Id
 */
export function shortId(spec: number | string = 8) {
  let genChar: () => number, len: number;
  if (typeof spec == 'number') {
    let d = new Date().getTime();
    genChar = () => (d + Math.random() * 16);
    len = spec;
  } else {
    let i = 0;
    genChar = () => spec.charCodeAt(i++);
    len = spec.length;
  }
  return ('a' + 'x'.repeat(len - 1)).replace(/[ax]/g, c =>
    (genChar() % (c == 'a' ? 6 : 16) + (c == 'a' ? 10 : 0) | 0).toString(16));
}

/**
 * Utility to generate a unique UUID for use in a MeldConfig
 */
export function uuid() {
  // This is indirected for documentation (do not just re-export generate)
  return generate();
}

/**
 * The **m-ld** API represents domain data to an app.
 *
 * This is an augmentation of the basic {@link MeldClone} interface for
 * Javascript/Typescript app development. In particular:
 * - Methods are typed to ensure that app code is aware of **m-ld**
 *   [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics). See the
 *   [Resource](/#resource) type for more details.
 * - Static utility methods are provided to help update app views of data based
 *   on updates notified via the {@link follow} method.
 *
 * The `get` and `delete` methods are intentionally suggestive of a REST API,
 * which could be implemented by the class when used in a service environment.
 *
 * > 🚧 `put`, `post` and `patch` methods will be available in a future release.
 */
export class MeldApi implements MeldClone {

  /** @internal */
  constructor(
    private readonly context: Context,
    private readonly store: MeldClone) {
  }

  /** @inheritDoc */
  close(err?: any): Promise<unknown> {
    return this.store.close(err);
  }

  /**
   * Shorthand method for retrieving a single Subject by its `@id`, if it exists.
   * @param path the Subject `@id`
   * @returns a promise resolving to the requested Subject, or `undefined` if not found
   */
  get(path: string): Promise<Subject | undefined> & HasExecTick {
    const result = this.transact<Describe>({ '@describe': path });
    return Object.assign(result.pipe(take(1)).toPromise(), { tick: result.tick });
  }

  /**
   * Shorthand method for deleting a single Subject by its `@id`. This will also
   * remove references to the given Subject from other Subjects.
   * @param path the Subject `@id`
   */
  delete(path: string): PromiseLike<unknown> & HasExecTick {
    const asSubject: Subject = { '@id': path, [any()]: any() };
    const asObject: Subject = { '@id': any(), [any()]: { '@id': path } };
    return this.transact<Update>({
      '@delete': [asSubject, asObject],
      '@where': { '@union': [asSubject, asObject] }
    });
  }

  // TODO: post, put

  // @inheritDoc for this method is broken
  // https://github.com/TypeStrong/typedoc/issues/793
  /**
   * Actively writes data to, or reads data from, the domain.
   *
   * The transaction executes once, asynchronously, and the results are notified
   * to subscribers of the returned stream.
   *
   * For write requests, the query executes as soon as possible. The result is
   * only completion or error of the returned observable stream – no Subjects
   * are signalled.
   *
   * For read requests, the query executes in response to the first subscription
   * to the returned stream, and subsequent subscribers will share the same
   * results stream.
   *
   * @typeParam P the pattern type being transacted, e.g. {@link Subject} or {@link Update}
   * @typeParam S the app-specific subject type
   * @param request the declarative transaction description
   * @returns an observable stream of subjects. For a write transaction, this is
   * empty, but indicates final completion or error of the transaction. The
   * return value is also a promise of the stream materialised as an array, for
   * convenience.
   */
  transact<P = Pattern, S = Subject>(request: P & Pattern):
    Observable<Resource<S>> & PromiseLike<Resource<S>[]> & HasExecTick {
    const result = this.store.transact({
      ...request,
      // Apply the domain context to the request, explicit context wins
      '@context': { ...this.context, ...request['@context'] || {} }
    });
    const subjects: Observable<Resource<S>> = result.pipe(map((subject: Subject) => {
      // Strip the domain context from the request
      return <Resource<S>>this.stripDomainContext(subject);
    }));
    const then: PromiseLike<Resource<S>[]>['then'] = (onfulfilled, onrejected) =>
      subjects.pipe(rxToArray()).toPromise().then(onfulfilled, onrejected);
    return Object.assign(subjects, { then, tick: result.tick });
  }

  /** @inheritDoc */
  get status(): Observable<MeldStatus> & LiveStatus {
    return this.store.status;
  }

  /** @inheritDoc */
  follow(): Observable<MeldUpdate> {
    return this.store.follow().pipe(flatMap(async update => ({
      '@ticks': update['@ticks'],
      '@delete': await this.regroup(update['@delete']),
      '@insert': await this.regroup(update['@insert'])
    })));
  }

  private async regroup(subjects: Subject[]): Promise<Subject[]> {
    const graph: any = await flatten(subjects, this.context);
    return graph['@graph'];
  }

  private stripDomainContext(jsonld: Subject): Subject {
    const { '@context': context, ...rtn } = jsonld;
    if (context)
      Object.keys(this.context).forEach((k: keyof Context) => delete context[k]);
    return context && Object.keys(context).length ? { ...rtn, '@context': context } : rtn;
  }
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

export namespace MeldApi {
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
    function isSubjectOrRef(v: Value): v is Subject | Reference {
      return typeof value == 'object' && !isValueObject(value);
    }
    if (isSubjectOrRef(value)) {
      return !!value['@id'] && set.filter(isSubjectOrRef).map(v => v['@id']).includes(value['@id']);
    } else {
      return set.includes(value);
    }
  }
}