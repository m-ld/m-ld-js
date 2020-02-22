import { MeldStore } from '.';
import { Context, Subject, Describe, Pattern, Update } from './jsonrql';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

export class MeldApi {
  private readonly context: Context;

  constructor(domain: string, context: Context | null, readonly store: MeldStore) {
    if (!/^[a-z0-9_]+([\-.][a-z0-9_]+)*\.[a-z]{2,6}$/.test(domain))
      throw new Error('Domain not specified or not valid');

    this.context = { '@base': `http://${domain}/`, ...context };
    this.context['@vocab'] = this.context['@vocab'] || new URL('/#', this.context['@base']).href;
  }

  get(path: string): Observable<Subject> {
    return this.transact({ '@describe': path } as Describe);
  }

  delete(path: string): Observable<Subject> {
    return this.transact({
      '@delete': [
        { '@id': path },
        // TODO: This is wrong, multiple patterns INTERSECT in BGP (not UNION)
        // https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#BGPsparql
        { '?': { '@id': path } }
      ]
    } as Update);
  }

  // TODO: post, put

  transact(request: Pattern, implicitContext?: Context): Observable<Subject> {
    implicitContext = implicitContext || this.context;
    return (this.store.transact({
      ...request,
      // Apply the given implicit context to the request, explicit context wins
      '@context': { ...implicitContext, ...request['@context'] || {} }
    })).pipe(map((subject: Subject) => {
      // Strip the given implicit context from the request
      const { '@context': context, ...rtn } = subject;
      if (implicitContext && context)
        Object.keys(implicitContext).forEach((k: keyof Context) => delete context[k]);
      return context && Object.keys(context).length ? { ...rtn, '@context': context } : rtn;
    }));
  }
}