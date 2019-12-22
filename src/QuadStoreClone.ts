import { MeldClone, Snapshot, DeltaMessage, MeldDelta, UUID } from './meld';
import {
  Pattern, Subject, Update, Describe,
  isQuery, isUpdate, isDescribe, isSubject, isGroup,
  asGroup, resolve
} from './jsonrql';
import { Observable } from 'rxjs';
import { Hash } from './hash';
import { TreeClock } from './clocks';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { Quad, NamedNode } from 'rdf-js';
import { toRDF, fromRDF, compact } from 'jsonld';
import { quad as createQuad, namedNode } from '@rdfjs/data-model';
import { JsonLd } from 'jsonld/jsonld-spec';
import { RdfStore } from 'quadstore';
import { streamToArray } from 'quadstore/lib/utils';
import { v4 as uuid } from 'uuid';

export class QuadStoreClone implements MeldClone {
  private readonly store: RdfStore;

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
      toRDF(asGroup(request['@insert'], request['@context'])).then(rdf => {
        new SuSetTransaction(this.store).add(rdf as Quad[]).commit().then(
          () => subs.complete(), err => subs.error(err));
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

interface ReifiedQuad extends Quad {
  id: NamedNode;
}

const ns: (namespace: string) => (name: string) => NamedNode = require('@rdfjs/namespace');
namespace m_ld.qs {
  export const rid = ns('http://qs.m-ld.org/rid/');
  export const tid = ns('http://qs.m-ld.org/tid/');
}
const rdf = ns('http://www.w3.org/1999/02/22-rdf-syntax-ns#');

// See https://jena.apache.org/documentation/notes/reification.html
const reification = (quad: ReifiedQuad) => [
  createQuad(quad.id, rdf('type'), rdf('Statement')),
  createQuad(quad.id, rdf('subject'), quad.subject),
  createQuad(quad.id, rdf('predicate'), quad.predicate),
  createQuad(quad.id, rdf('object'), quad.object)
];

const reify = (quad: Quad) => ({ id: m_ld.qs.rid(uuid()), ...quad });

class SuSetTransaction implements MeldDelta {
  readonly tid: UUID = uuid();
  readonly insert: ReifiedQuad[] = [];
  readonly delete: ReifiedQuad[] = [];

  constructor(readonly store: RdfStore) { }

  remove(quads: Quad[]): this {
    return this;
  }

  add(quads: Quad[]): this {
    this.insert.push(...quads.map(reify));
    return this;
  }

  async commit(): Promise<this> {
    if (this.insert.length && !this.delete.length) {
      await this.store.put(this.insert.reduce((quads: Quad[], quad) => {
        quads.push(quad, ...reification(quad));
        return quads;
      }, []));
      return this;
    }
  }
}