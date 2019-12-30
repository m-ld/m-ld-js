import { MeldClone, Snapshot, DeltaMessage, MeldRemotes } from './meld';
import {
  Pattern, Subject, Update, Context, GroupLike,
  isQuery, isUpdate, isDescribe, isSubject, isGroup,
  asGroup, resolve
} from './jsonrql';
import { Observable } from 'rxjs';
import { TreeClock } from './clocks';
import { Quad } from 'rdf-js';
import { namedNode } from '@rdfjs/data-model';
import { toRDF, fromRDF, compact } from 'jsonld';
import { JsonLd, Iri } from 'jsonld/jsonld-spec';
import { SuSetDataset } from './SuSetDataset';
import { TreeClockMessageService } from './messages';
import { Dataset } from './Dataset';

export class DatasetClone implements MeldClone {
  private readonly dataset: SuSetDataset;
  private readonly messageService: TreeClockMessageService;
  private isGenesis: boolean = false;

  constructor(dataset: Dataset,
    private readonly remotes: MeldRemotes) {
    this.dataset = new SuSetDataset(dataset);
    // TODO
    this.messageService = new TreeClockMessageService(TreeClock.GENESIS);
  }

  set genesis(isGenesis: boolean) {
    this.isGenesis = isGenesis;
  }

  async initialise(): Promise<void> {
    await this.dataset.initialise();
    let newClone: boolean, time: TreeClock | null;
    if (this.isGenesis) {
      time = TreeClock.GENESIS;
    } else if (newClone = !(time = await this.dataset.loadClock())) {
      time = await this.remotes.newClock();
      this.dataset.saveClock(time);
    }


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
        return this.insert(request['@insert'], request['@context']);
      } else if (isDescribe(request)) {
        return this.describe(request['@describe'], request['@context']);
      }
    }
    throw new Error('Request type not supported.');
  }

  follow(): Observable<Update> {
    throw new Error('Method not implemented.');
  }

  private insert(insert: GroupLike, context?: Context): Observable<Subject> {
    return new Observable(subs => {
      toRDF(asGroup(insert, context)).then(rdf => this.dataset.transact(async txn => {
        txn.add(rdf as Quad[]);
        return this.messageService.send();
      })
        // TODO publish the DeltaMessage
        .then(() => subs.complete(), err => subs.error(err)));
    });
  }

  private describe(describe: Iri, context?: Context): Observable<Subject> {
    return new Observable(subs => {
      resolve(describe, context)
        .then(iri => this.dataset.match(namedNode(iri)))
        .then((quads: Quad[]) => {
          if (quads.length)
            fromRDF(quads)
              .then((jsonld: JsonLd) => compact(jsonld, context || {}))
              .then((jsonld: JsonLd) => {
                (Array.isArray(jsonld) ? jsonld : [jsonld]).forEach(subject => subs.next(subject));
                subs.complete();
              }, (err: unknown) => subs.error(err))
          else
            subs.complete();
        });
    });
  }
}