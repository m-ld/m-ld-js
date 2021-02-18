import { InterimUpdate, DeleteInsert, MeldUpdate } from '../../api';
import { Quad } from 'rdf-js';
import { TreeClock } from '../clocks';
import { Subject, Update } from '../../jrql-support';
import { PatchQuads } from '.';
import { flatten as flatJsonLd } from 'jsonld';
import { JrqlGraph } from './JrqlGraph';
import { SubjectGraph } from '../SubjectGraph';

export class InterimUpdatePatch implements InterimUpdate {
  /** Assertions made by the constraint (not including the app patch if not mutable) */
  private assertions: PatchQuads;
  /** Entailments made by the constraint */
  private entailments = new PatchQuads();
  /** Whether to recreate the insert & delete fields from the assertions */
  private needsUpdate: PromiseLike<boolean>;
  /** Cached update */
  private _update: MeldUpdate | undefined;

  /**
   * @param patch the starting app patch (will not be mutated unless 'mutable')
   */
  constructor(
    private readonly graph: JrqlGraph,
    private readonly time: TreeClock,
    private readonly patch: PatchQuads,
    private readonly mutable?: 'mutable') {
    // If mutable, we treat the app patch as assertions
    this.assertions = mutable ? patch : new PatchQuads();
    this.needsUpdate = Promise.resolve(true);
  }

  async finalise() {
    const state = {
      update: await this.update,
      assertions: this.assertions,
      entailments: this.entailments
    };
    this.needsUpdate = { then: () => { throw 'Interim update has been finalised'; } };
    return state;
  }

  get update(): Promise<MeldUpdate> {
    return Promise.resolve(this.needsUpdate).then(needsUpdate => {
      if (needsUpdate || this._update == null) {
        const patch = this.mutable ? this.assertions :
          new PatchQuads(this.patch).append(this.assertions);
        this.needsUpdate = Promise.resolve(false);
        this._update = {
          '@ticks': this.time.ticks,
          '@delete': SubjectGraph.fromRDF(patch.oldQuads),
          '@insert': SubjectGraph.fromRDF(patch.newQuads)
        };
      }
      return this._update;
    });
  }

  assert = (update: Update) => this.mutate(async () => {
    const patch = await this.graph.write(update);
    this.assertions.append(patch);
    return !patch.isEmpty;
  });

  entail = (update: Update) => this.mutate(async () => {
    const patch = await this.graph.write(update);
    this.entailments.append(patch);
    return false;
  });

  remove = (key: keyof DeleteInsert<any>, pattern: Subject | Subject[]) =>
    this.mutate(async () => {
      const toRemove = await this.graph.definiteQuads(pattern);
      const removed = this.assertions.remove(
        key == '@delete' ? 'oldQuads' : 'newQuads', toRemove);
      return removed.length !== 0;
    });

  private mutate(fn: () => Promise<boolean>) {
    this.needsUpdate = this.needsUpdate.then(
      async (needsUpdate) => (await fn()) || needsUpdate);
  }
}
