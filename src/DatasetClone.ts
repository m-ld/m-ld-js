import { MeldClone, Snapshot, DeltaMessage } from './meld';
import {
  Pattern, Subject, Update, Describe,
  isQuery, isUpdate, isDescribe, isSubject, isGroup,
  asGroup, resolve
} from './jsonrql';
import { Observable } from 'rxjs';
import { TreeClock } from './clocks';
import { Quad } from 'rdf-js';
import { namedNode } from '@rdfjs/data-model';
import { toRDF, fromRDF, compact } from 'jsonld';
import { JsonLd } from 'jsonld/jsonld-spec';
import { SuSetTransaction } from './SuSetTransaction';
import { TreeClockMessageService } from './messages';
import { Dataset } from './Dataset';

export class DatasetClone implements MeldClone {
  private readonly messageService: TreeClockMessageService;

  constructor(
    private readonly dataset: Dataset) {
    // TODO
    this.messageService = new TreeClockMessageService(TreeClock.GENESIS);
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

  revupFrom(): Promise<Observable<DeltaMessage>> {
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

  follow(): Observable<Update> {
    throw new Error('Method not implemented.');
  }

  private insert(request: Update): Observable<Subject> {
    return new Observable(subs => {
      toRDF(asGroup(request['@insert'], request['@context'])).then(rdf => {
        this.dataset.transact(() => new SuSetTransaction(this.dataset)
          .add(rdf as Quad[])
          .commit(this.messageService.send()))
          // TODO publish the message
          .then(() => subs.complete(), err => subs.error(err));
      });
    });
  }

  private describe(request: Describe): Observable<Subject> {
    return new Observable(subs => {
      resolve(request['@describe'], request['@context'])
        .then(iri => this.dataset.model().match(namedNode(iri)))
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