import { MeldStore, MeldUpdate } from '.';
import { Context, Subject, Describe, Pattern, Update, Group, JrqlValue, isValueObject, Reference, DeleteInsert } from './jsonrql';
import { Observable } from 'rxjs';
import { map, flatMap } from 'rxjs/operators';
import { flatten } from 'jsonld';
import { Iri } from 'jsonld/jsonld-spec';
import { toArray } from '../util';

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
      '@delete': this.stripImplicitContext(await this.regroup(update['@delete']), this.context),
      '@insert': this.stripImplicitContext(await this.regroup(update['@insert']), this.context)
    })));
  }

  private async regroup(group?: Group): Promise<Group> {
    return group ? await flatten(group, this.context) as Group : { '@graph': [] };
  }

  private stripImplicitContext(jsonld: Subject, implicitContext: Context): Subject;
  private stripImplicitContext(jsonld: Group, implicitContext: Context): Group;
  private stripImplicitContext(jsonld: Subject | Group, implicitContext: Context): Subject | Group {
    const { '@context': context, ...rtn } = jsonld;
    if (implicitContext && context)
      Object.keys(implicitContext).forEach((k: keyof Context) => delete context[k]);
    return context && Object.keys(context).length ? { ...rtn, '@context': context } : rtn;
  }
}

export namespace MeldApi {
  export type Node<T> = Subject & {
    [P in keyof T]: T extends '@id' ? Iri : T[P] extends Array<unknown> ? T[P] : T[P] | T[P][];
  }

  export function asSubjectUpdates(update: DeleteInsert<Group>): SubjectUpdates {
    return bySubject(update, '@insert', bySubject(update, '@delete', {}));
  }

  export type SubjectUpdates = { [id: string]: DeleteInsert<Subject> };

  function bySubject(update: DeleteInsert<Group>,
    key: '@insert' | '@delete', bySubject: SubjectUpdates): SubjectUpdates {
    return toArray(update[key]['@graph']).reduce((byId, subject) =>
      ({ ...byId, [subject['@id'] ?? '*']: { ...byId[subject['@id'] ?? '*'], [key]: subject } }), bySubject);
  }

  export function update<T>(msg: Node<T>, update: DeleteInsert<Subject>): void {
    // Allow for undefined/null ids
    const inserts = update['@insert'] && msg['@id'] == update['@insert']['@id'] ? update['@insert'] : {};
    const deletes = update['@delete'] && msg['@id'] == update['@delete']['@id'] ? update['@delete'] : {};
    new Set(Object.keys(msg).concat(Object.keys(inserts))).forEach(key => {
      switch (key) {
        case '@id': break;
        default: msg[key as keyof Node<T>] =
          updateProperty(msg[key], inserts[key], deletes[key]);
      }
    });
  }

  function updateProperty(value: any, insertVal: any, deleteVal: any): any {
    let rtn = toArray(value)
      .filter(v => !includesValue(toArray(deleteVal), v))
      .concat(toArray(insertVal));
    return rtn.length == 1 && !Array.isArray(value) ? rtn[0] : rtn;
  }

  export function includesValue(arr: JrqlValue[], value: JrqlValue): boolean {
    // TODO support value objects
    function isSubjectOrRef(v: JrqlValue): v is Subject | Reference {
      return typeof value == 'object' && !isValueObject(value);
    }
    if (isSubjectOrRef(value)) {
      return !!value['@id'] && arr.filter(isSubjectOrRef).map(v => v['@id']).includes(value['@id']);
    } else {
      return arr.includes(value);
    }
  }
}