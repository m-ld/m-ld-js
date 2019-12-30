import { Quad, DefaultGraph, NamedNode, Quad_Subject, Quad_Predicate, Quad_Object } from 'rdf-js';
import { defaultGraph } from '@rdfjs/data-model';
import { RdfStore, MatchTerms } from 'quadstore';
import AsyncLock = require('async-lock');
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';

export interface Patch {
  oldQuads: Quad[] | MatchTerms<Quad>;
  newQuads: Quad[];
}

export class PatchQuads implements Patch {
  constructor(
    readonly oldQuads: Quad[],
    readonly newQuads: Quad[]) {
  }

  concat = (that: PatchQuads) => new PatchQuads(
    this.oldQuads.concat(that.oldQuads),
    this.newQuads.concat(that.newQuads));
}

export type ModelName = DefaultGraph | NamedNode;

export interface Dataset {
  model(name?: ModelName): Graph;

  /**
   * Ensures that write transactions are executed serially against the store.
   * @param prepare prepares a write operation to be performed
   */
  transact<T>(prepare: () => Promise<[Patch, T]>): Promise<T>;
}

export interface Graph {
  match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Promise<Quad[]>;
}

export interface DatasetOptions extends AbstractOpenOptions {
  id: string;
}

export class QuadStoreDataset implements Dataset {
  private readonly id: string;
  private readonly store: RdfStore;

  constructor(abstractLevelDown: AbstractLevelDOWN, opts: DatasetOptions) {
    this.id = opts.id;
    this.store = new RdfStore(abstractLevelDown, opts);
  }

  model(name?: ModelName): Graph {
    return new QuadStoreGraph(this.store, name || defaultGraph());
  }

  transact<T>(prepare: () => Promise<[Patch, T]>): Promise<T> {
    return new AsyncLock().acquire(this.id, async () => {
      const [patch, rtn] = await prepare();
      await this.store.patch(patch.oldQuads, patch.newQuads);
      return rtn;
    });
  }
}

class QuadStoreGraph implements Graph {
  constructor(
    readonly store: RdfStore,
    readonly name: ModelName) {
  }

  async match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Promise<Quad[]> {
    return await this.store.get({ graph: this.name, subject, predicate, object });
  }
}
