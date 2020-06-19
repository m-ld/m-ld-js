import { MeldStore, MeldUpdate, DeleteInsert } from '.';
import { Context, Subject, Describe, Pattern, Update, Group, Value, isValueObject, Reference } from 'json-rql';
import { Observable } from 'rxjs';
import { map, flatMap } from 'rxjs/operators';
import { flatten } from 'jsonld';
import { toArray } from '../util';
import { Iri } from 'jsonld/jsonld-spec';

export class MeldApi implements MeldStore {
  private readonly context: Context;

  constructor(domain: string, context: Context | null, readonly store: MeldStore) {
    if (!/^[a-z0-9_]+([\-.][a-z0-9_]+)*\.[a-z]{2,6}$/.test(domain))
      throw new Error('Domain not specified or not valid');

    this.context = { '@base': `http://${domain}/`, ...context };
    this.context['@vocab'] = this.context['@vocab'] || new URL('/#', this.context['@base']).href;
  }

  close(err?: any): Promise<void> {
    return this.store.close(err);
  }

  get(path: string): Observable<Subject> {
    return this.transact({ '@describe': path } as Describe);
  }

  delete(path: string): Observable<Subject> {
    return this.transact({
      '@delete': [
        { '@id': path },
        // BUG: This is wrong, multiple patterns INTERSECT in BGP (not UNION)
        // https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#BGPsparql
        // Requires UNION, see m-ld-core/src/main/java/org/m_ld/MeldApi.java
        { '?': { '@id': path } }
      ]
    } as Update);
  }

  // TODO: post, put

  transact(request: Pattern, implicitContext: Context = this.context): Observable<Subject> {
    return (this.store.transact({
      ...request,
      // Apply the given implicit context to the request, explicit context wins
      '@context': { ...implicitContext, ...request['@context'] || {} }
    })).pipe(map((subject: Subject) => {
      // Strip the given implicit context from the request
      return this.stripImplicitContext(subject, implicitContext);
    }));
  }

  latest(): Promise<number> {
    return this.store.latest();
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
    return bySubject(update, '@insert', bySubject(update, '@delete', {}));
  }

  export type SubjectUpdates = { [id: string]: DeleteInsert<Subject> };

  function bySubject(update: DeleteInsert<Subject[]>,
    key: '@insert' | '@delete', bySubject: SubjectUpdates): SubjectUpdates {
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