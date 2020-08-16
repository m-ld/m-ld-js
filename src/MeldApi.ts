import { MeldClone, MeldUpdate, LiveStatus, MeldStatus, HasExecTick, shortId, array } from '.';
import {
  Context, Subject, Describe, Pattern, Update, Value, isValueObject, Reference, Variable
} from './jrql-support';
import { Observable } from 'rxjs';
import { map, flatMap, toArray as rxToArray, take } from 'rxjs/operators';
import { flatten } from 'jsonld';

export { MeldUpdate, array };

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