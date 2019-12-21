import { MeldClone, Snapshot, DeltaMessage } from './meld';
import { Pattern, Subject, Update, isQuery, isUpdate, isDescribe, Describe, asGroup, isSubject, isGroup, resolve } from './jsonrql';
import { Observable } from 'rxjs';
import { Hash } from './hash';
import { TreeClock } from './clocks';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { Store, Quad } from 'rdf-js';
import { toRDF, fromRDF, compact } from 'jsonld';
import { namedNode } from '@rdfjs/data-model';
import { JsonLd, Iri } from 'jsonld/jsonld-spec';
const { RdfStore } = require('quadstore');
const { streamToArray, createArrayStream } = require('quadstore/lib/utils');

export class QuadStoreClone implements MeldClone {
  private readonly store: Store;

  constructor(abstractLevelDown: AbstractLevelDOWN, opts?: AbstractOpenOptions) {
    this.store = new RdfStore(abstractLevelDown, opts);
  }

  updates(): Observable<DeltaMessage> {
    throw new Error('Method not implemented.');
  }

  newClock(): Promise<TreeClock> {
    throw new Error('Method not implemented.');
  }

  snapshot(): Promise<Snapshot> {
    throw new Error('Method not implemented.');
  }

  revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage>> {
    throw new Error('Method not implemented.');
  }

  transact(request: Pattern): Observable<Subject> {
    if (isGroup(request) || isSubject(request)) {
      return this.transact({ '@insert': request } as Update);
    } else if (isQuery(request) && !request['@where']) {
      if (isUpdate(request) && request['@insert'] && !request['@delete']) {
        return this.insert(request);
      } else if (isDescribe(request)) {
        return this.describe(request);
      }
    }
    throw new Error('Request type not supported.');
  }

  follow(after: Hash): Observable<Update> {
    throw new Error('Method not implemented.');
  }

  private insert(request: Update): Observable<Subject> {
    return new Observable(subs => {
      toRDF(asGroup(request['@insert'], request['@context'])).then(quads => {
        this.store.import(createArrayStream(quads))
          .on('error', err => subs.error(err))
          .on('end', () => subs.complete());
      });
    });
  }

  private describe(request: Describe): Observable<Subject> {
    return new Observable(subs => {
      resolve(request['@describe'], request['@context'])
        .then(iri => streamToArray(this.store.match(namedNode(iri))))
        .then((quads: Quad[]) => {
          if (quads.length)
            fromRDF(quads)
              .then((jsonld: JsonLd) => compact(jsonld, request['@context'] || {}))
              .then((jsonld: JsonLd) => {
                (Array.isArray(jsonld) ? jsonld : [jsonld]).forEach(subject => subs.next(subject));
                subs.complete();
              }, (err: any) => subs.error(err))
          else
            subs.complete();
        });
    });
  }
}