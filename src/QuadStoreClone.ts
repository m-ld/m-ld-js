import { MeldClone, Snapshot, DeltaMessage } from './meld';
import { Pattern, Subject, Update, isSubject, isQuery, isUpdate, isGroup, isDescribe } from './jsonrql';
import { Observable } from 'rxjs';
import { Hash } from './hash';
import { TreeClock } from './clocks';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { Store, Quad } from 'rdf-js';
import { toRDF, fromRDF, compact } from 'jsonld';
import { namedNode } from '@rdfjs/data-model';
import { JsonLd } from 'jsonld/jsonld-spec';
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
      if (isUpdate(request) && request['@insert']) {
        return new Observable(subs => {
          toRDF(request['@insert']).then(quads => {
            this.store.import(createArrayStream(quads))
              .on('error', err => subs.error(err))
              .on('end', () => subs.complete());
          })
        });
      } else if (isDescribe(request)) {
        return new Observable(subs => {
          streamToArray(this.store.match(namedNode(request['@describe'])))
            .then((quads: Quad[]) => fromRDF(quads))
            .then((jsonld: JsonLd) => compact(jsonld, context))
            .then((jsonld: JsonLd) => {
              (Array.isArray(jsonld) ? jsonld : [jsonld]).forEach(subject => subs.next(subject));
              subs.complete();
            }, (err: any) => subs.error(err));
        });
      }
    }
    throw new Error('Request type not supported.');
  }

  follow(after: Hash): Observable<Update> {
    throw new Error('Method not implemented.');
  }
}