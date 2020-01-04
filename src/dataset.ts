import { Quad, DefaultGraph, NamedNode, Quad_Subject, Quad_Predicate, Quad_Object } from 'rdf-js';
import { defaultGraph } from '@rdfjs/data-model';
import { RdfStore, MatchTerms } from 'quadstore';
import AsyncLock = require('async-lock');
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';

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

  concat({ oldQuads, newQuads }: { oldQuads: Quad[], newQuads: Quad[] }) {
    return new PatchQuads(this.oldQuads.concat(oldQuads), this.newQuads.concat(newQuads));
  }
}

export type GraphName = DefaultGraph | NamedNode;

/**
 * Writeable dataset. Transactions are atomically and serially applied.
 * Note that the patch created by a transaction can span Graphs - each
 * Quad in the patch will have a graph property.
 */
export interface Dataset {
  graph(name?: GraphName): Graph;

  /**
   * Ensures that write transactions are executed serially against the store.
   * @param prepare prepares a write operation to be performed
   */
  transact<T>(prepare: () => Promise<Patch>): Promise<void>;
  transact<T>(prepare: () => Promise<[Patch, T]>): Promise<T>;
  transact<T>(prepare: () => Promise<Patch | [Patch, T]>): Promise<T>;
}

/**
 * Read-only utility interface for reading Quads from a Dataset.
 */
export interface Graph {
  readonly name: GraphName;

  match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Promise<Quad[]>;
}

export interface DatasetOptions extends AbstractOpenOptions {
  id: string;
}

export class QuadStoreDataset implements Dataset {
  readonly id: string;
  private readonly store: RdfStore;

  constructor(abstractLevelDown: AbstractLevelDOWN, opts: DatasetOptions) {
    this.id = opts.id;
    this.store = new RdfStore(abstractLevelDown, opts);
  }

  graph(name?: GraphName): Graph {
    return new QuadStoreGraph(this.store, name || defaultGraph());
  }

  transact<T>(prepare: () => Promise<Patch>): Promise<void>;
  transact<T>(prepare: () => Promise<[Patch, T]>): Promise<T>;
  transact<T>(prepare: () => Promise<Patch | [Patch, T]>): Promise<T | void> {
    return new AsyncLock().acquire(this.id, async () => {
      const prep = await prepare();
      const [patch, rtn] = Array.isArray(prep) ? prep : [prep, undefined];
      await this.store.patch(patch.oldQuads, patch.newQuads);
      return rtn;
    });
  }
}

class QuadStoreGraph implements Graph {
  constructor(
    readonly store: RdfStore,
    readonly name: GraphName) {
  }

  async match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Promise<Quad[]> {
    return await this.store.get({ graph: this.name, subject, predicate, object });
  }
}
