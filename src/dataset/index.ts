import { Quad, DefaultGraph, NamedNode, Quad_Subject, Quad_Predicate, Quad_Object } from 'rdf-js';
import { defaultGraph } from '@rdfjs/data-model';
import { RdfStore, MatchTerms } from 'quadstore';
import AsyncLock = require('async-lock');
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { Observable } from 'rxjs';

/**
 * Atomically-applied patch to a quad-store.
 */
export interface Patch {
  oldQuads: Quad[] | MatchTerms<Quad>;
  newQuads: Quad[];
}

/**
 * Specialised patch that allows concatenation.
 * Requires that the oldQuads are concrete Quads and not a MatchTerms.
 */
export class PatchQuads implements Patch {
  constructor(
    readonly oldQuads: Quad[],
    readonly newQuads: Quad[]) {
  }

  concat({ oldQuads, newQuads }: { oldQuads?: Quad[], newQuads?: Quad[] }) {
    return new PatchQuads(
      oldQuads ? this.oldQuads.concat(oldQuads) : this.oldQuads,
      newQuads ? this.newQuads.concat(newQuads) : this.newQuads);
  }
}

export type GraphName = DefaultGraph | NamedNode;

/**
 * Writeable dataset. Transactions are atomically and serially applied.
 * Note that the patch created by a transaction can span Graphs - each
 * Quad in the patch will have a graph property.
 */
export interface Dataset {
  readonly id: string;
  graph(name?: GraphName): Graph;

  /**
   * Ensures that write transactions are executed serially against the store.
   * @param prepare prepares a write operation to be performed
   */
  transact(prepare: () => Promise<Patch | undefined | void>): Promise<void>;
  transact<T>(prepare: () => Promise<[Patch | undefined, T]>): Promise<T>;

  close(): Promise<void>;
}

/**
 * Read-only utility interface for reading Quads from a Dataset.
 */
export interface Graph {
  readonly name: GraphName;

  match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Observable<Quad>;
}

export interface DatasetOptions extends AbstractOpenOptions {
  id: string;
}

export class QuadStoreDataset implements Dataset {
  readonly id: string;
  private readonly store: RdfStore;
  private readonly lock = new AsyncLock;

  constructor(private readonly abstractLevelDown: AbstractLevelDOWN, opts: DatasetOptions) {
    this.id = opts.id;
    this.store = new RdfStore(abstractLevelDown, opts);
  }

  graph(name?: GraphName): Graph {
    return new QuadStoreGraph(this.store, name || defaultGraph());
  }

  transact<T>(prepare: () => Promise<Patch | [Patch | undefined, T] | undefined | void>): Promise<T | void> {
    return this.lock.acquire(this.id, async () => {
      const prep = await prepare();
      const [patch, rtn] = Array.isArray(prep) ? prep : [prep, undefined];
      if (patch)
        await this.store.patch(patch.oldQuads, patch.newQuads);
      return rtn;
    });
  }

  close(): Promise<void> {
    return new Promise((resolve, reject) =>
      this.abstractLevelDown.close(err => err ? reject(err) : resolve()));
  }
}

class QuadStoreGraph implements Graph {
  constructor(
    readonly store: RdfStore,
    readonly name: GraphName) {
  }

  match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Observable<Quad> {
    return new Observable(subs => {
      this.store.match(subject, predicate, object, this.name)
        .on('data', quad => subs.next(quad))
        .on('error', err => subs.error(err))
        .on('end', () => subs.complete());
    });    
  }
}
