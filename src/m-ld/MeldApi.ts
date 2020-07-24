import { MeldStore, MeldUpdate, DeleteInsert, LiveStatus } from '.';
import {
  Context, Subject, Describe, Pattern, Update, Value, isValueObject, Reference, Variable
} from '../dataset/jrql-support';
import { Observable } from 'rxjs';
import { map, flatMap, toArray as rxToArray, take } from 'rxjs/operators';
import { flatten } from 'jsonld';
import { toArray, shortId } from '../util';
import { Iri } from 'jsonld/jsonld-spec';
import { DomainContext } from './MeldJson';

export { MeldUpdate };
  
export function any(): Variable {
  return `?${shortId(4)}`;
}

export class MeldApi implements MeldStore {
  private readonly context: Context;

  constructor(domain: string, context: Context | null, readonly store: MeldStore) {
    if (!/^[a-z0-9_]+([\-.][a-z0-9_]+)*\.[a-z]{2,6}$/.test(domain))
      throw new Error('Domain not specified or not valid');
    this.context = new DomainContext(domain, context);
  }

  close(err?: any): Promise<void> {
    return this.store.close(err);
  }

  get(path: string): PromiseLike<Subject | undefined> {
    return this.transact({ '@describe': path } as Describe).pipe(take(1)).toPromise();
  }

  delete(path: string): PromiseLike<unknown> {
    const asSubject: Subject = { '@id': path, [any()]: any() };
    const asObject: Subject = { '@id': any(), [any()]: { '@id': path } };
    return this.transact<Update>({
      '@delete': [asSubject, asObject],
      '@where': { '@union': [asSubject, asObject] }
    });
  }

  // TODO: post, put

  transact<P = Pattern, S = Subject>(
    request: P & Pattern, implicitContext: Context = this.context):
    Observable<Resource<S>> & PromiseLike<Resource<S>[]> {
    const subjects: Observable<Resource<S>> = this.store.transact({
      ...request,
      // Apply the given implicit context to the request, explicit context wins
      '@context': { ...implicitContext, ...request['@context'] || {} }
    }).pipe(map((subject: Subject) => {
      // Strip the given implicit context from the request
      return <Resource<S>>this.stripImplicitContext(subject, implicitContext);
    }));
    const then: PromiseLike<Resource<S>[]>['then'] = (onfulfilled, onrejected) =>
      subjects.pipe(rxToArray()).toPromise().then(onfulfilled, onrejected);
    return Object.assign(subjects, { then });
  }

  get status(): LiveStatus {
    return this.store.status;
  }

  follow(after?: number): Observable<MeldUpdate> {
    return this.store.follow(after).pipe(flatMap(async update => ({
      '@ticks': update['@ticks'],
      '@delete': await this.regroup(update['@delete']),
      '@insert': await this.regroup(update['@insert'])
    })));
  }

  private async regroup(subjects: Subject[]): Promise<Subject[]> {
    const graph: any = await flatten(subjects, this.context);
    return graph['@graph'];
  }

  private stripImplicitContext(jsonld: Subject, implicitContext: Context): Subject {
    const { '@context': context, ...rtn } = jsonld;
    if (implicitContext && context)
      Object.keys(implicitContext).forEach((k: keyof Context) => delete context[k]);
    return context && Object.keys(context).length ? { ...rtn, '@context': context } : rtn;
  }
}

export type Resource<T> = Subject & {
  [P in keyof T]: T extends '@id' ? Iri : T[P] extends Array<unknown> ? T[P] : T[P] | T[P][];
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

  export function update<T>(msg: Resource<T>, update: DeleteInsert<Subject>): void {
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
  }

  function updateProperty(value: any, insertVal: any, deleteVal: any): any {
    let rtn = toArray(value).filter(v => !includesValue(toArray(deleteVal), v));
    rtn = rtn.concat(toArray(insertVal).filter(v => !includesValue(rtn, v)));
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