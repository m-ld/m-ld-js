import { MeldClone, Snapshot, DeltaMessage } from './meld';
import { Pattern, Subject, Update, isSubject, isQuery, isUpdate, isGroup, isDescribe, Describe, Context } from './jsonrql';
import { Observable } from 'rxjs';
import { Hash } from './hash';
import { TreeClock } from './clocks';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { Store, Quad } from 'rdf-js';
import { toRDF, fromRDF, compact, expand } from 'jsonld';
import { namedNode } from '@rdfjs/data-model';
import { JsonLd, JsonLdObj, Iri } from 'jsonld/jsonld-spec';
const RdfStore = require('quadstore').RdfStore;
const { streamToArray, createArrayStream } = require('quadstore/lib/utils');

export class QuadStoreClone implements MeldClone {
  private readonly store: Store;

  constructor(abstractLevelDown: AbstractLevelDOWN, opts?: AbstractOpenOptions) {
    this.store = new RdfStore(abstractLevelDown, opts);
  }

  newClock(): Promise<TreeClock> {
    throw new Error('Method not implemented.');
  }

  snapshot(): Promise<Snapshot> {
    throw new Error('Method not implemented.');
  }

  revupFrom(): Promise<Observable<DeltaMessage>> {
    throw new Error('Method not implemented.');
  }

  transact(request: Pattern): Observable<Subject> {
    if (isSubject(request) || isGroup(request)) {
      return this.transact({ '@insert': request } as Update);
    } else if (isQuery(request) && !request['@where']) {
      const context = request['@context'] || {};
      if (isUpdate(request) && request['@insert'] && !request['@delete']) {
        return this.insert(request);
      } else if (isDescribe(request)) {
        return this.describe(request, context);
      }
    }
    throw new Error('Request type not supported.');
  }

  private insert(request: Update): Observable<Subject> {
    return new Observable(subs => {
      toRDF(request['@insert']).then(quads => {
        this.store.import(createArrayStream(quads))
          .on('error', err => subs.error(err))
          .on('end', () => subs.complete());
      });
    });
  }

  private describe(request: Describe, context: object): Observable<Subject> {
    return new Observable(subs => {
      this.resolve(request['@describe'], request['@context'])
        .then(iri => streamToArray(this.store.match(namedNode(iri))))
        .then((quads: Quad[]) => {
          if (quads.length)
            fromRDF(quads)
              .then((jsonld: JsonLd) => compact(jsonld, context))
              .then((jsonld: JsonLd) => {
                (Array.isArray(jsonld) ? jsonld : [jsonld]).forEach(subject => subs.next(subject));
                subs.complete();
              }, (err: any) => subs.error(err))
          else
            subs.complete();
        });
    });
  }

  private resolve(iri: string, context: Context): Promise<Iri> {
    return context ? compact({
      '@id': iri,
      'http://json-rql.org/predicate': 1,
      '@context': context
    }, {}).then((temp: any) => temp['@id']) : Promise.resolve(iri);
  }

  follow(after: Hash): Observable<Update> {
    throw new Error('Method not implemented.');
  }
}