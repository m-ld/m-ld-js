import { MeldClone, MeldUpdate, LiveStatus, MeldStatus, HasExecTick } from '.';
import {
  Context, Subject, Describe, Pattern, Update, Value, isValueObject, Reference, Variable
} from './jrql-support';
import { Observable } from 'rxjs';
import { map, flatMap, toArray as rxToArray, take } from 'rxjs/operators';
import { flatten } from 'jsonld';
import { array, shortId } from './engine/util';

export { MeldUpdate, array };

export interface DeleteInsert<T> {
  '@delete': T;
  '@insert': T;
}

export function any(): Variable {
  return `?${shortId(4)}`;
}

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

  get(path: string): Promise<Subject | undefined> & HasExecTick {
    const result = this.transact<Describe>({ '@describe': path });
    return Object.assign(result.pipe(take(1)).toPromise(), { tick: result.tick });
  }

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
 * Concrete, e.g. no variables or filters
 * [], undefined and null are equivalent
 * m-ld never outputs null
 * Use `array`
 */
export type Resource<T> = Subject & Reference & {
  [P in keyof T]: T[P] extends Array<unknown> ? T[P] | undefined : T[P] | T[P][] | undefined;
};

export namespace MeldApi {
  export function asSubjectUpdates(update: DeleteInsert<Subject[]>): SubjectUpdates {
    return bySubject(update, '@insert', bySubject(update, '@delete'));
  }

  export type SubjectUpdates = { [id: string]: DeleteInsert<Subject> };

  function bySubject(update: DeleteInsert<Subject[]>,
    key: '@insert' | '@delete', bySubject: SubjectUpdates = {}): SubjectUpdates {
    return update[key].reduce((byId, subject) =>
      ({ ...byId, [subject['@id'] ?? '*']: { ...byId[subject['@id'] ?? '*'], [key]: subject } }), bySubject);
  }

  export function update<T>(msg: Resource<T>, update: DeleteInsert<Subject>): Resource<T> {
    // Allow for undefined/null ids
    const inserts = update['@insert'] && msg['@id'] == update['@insert']['@id'] ? update['@insert'] : {};
    const deletes = update['@delete'] && msg['@id'] == update['@delete']['@id'] ? update['@delete'] : {};
    new Set(Object.keys(msg).concat(Object.keys(inserts))).forEach(key => {
      switch (key) {
        case '@id': break;
        default: msg[key as keyof Resource<T>] =
          updateProperty(msg[key], inserts[key], deletes[key]);
      }
    });
    return msg;
  }

  function updateProperty(value: any, insertVal: any, deleteVal: any): any {
    let rtn = array(value).filter(v => !includesValue(array(deleteVal), v));
    rtn = rtn.concat(array(insertVal).filter(v => !includesValue(rtn, v)));
    return rtn.length == 1 && !Array.isArray(value) ? rtn[0] : rtn;
  }

  export function includesValue(arr: Value[], value: Value): boolean {
    // TODO support value objects
    function isSubjectOrRef(v: Value): v is Subject | Reference {
      return typeof value == 'object' && !isValueObject(value);
    }
    if (isSubjectOrRef(value)) {
      return !!value['@id'] && arr.filter(isSubjectOrRef).map(v => v['@id']).includes(value['@id']);
    } else {
      return arr.includes(value);
    }
  }
}