import { InterimUpdate, DeleteInsert } from '../../api';
import { Quad } from 'rdf-js';
import { TreeClock } from '../clocks';
import { Subject, Update } from '../../jrql-support';
import { DefinitePatch, PatchQuads } from '.';
import { flatten as flatJsonLd } from 'jsonld';
import { JrqlGraph } from './JrqlGraph';
import { rdfToJson } from "../jsonld";

export class InterimUpdatePatch implements InterimUpdate {
  '@ticks': number;
  '@delete': Subject[] = [];
  '@insert': Subject[] = [];
  /** Assertions made by the constraint (not including the app patch if not mutable) */
  assertions: PatchQuads;
  /** Entailments made by the constraint */
  entailments = new PatchQuads();
  /** Whether to recreate the insert & delete fields from the assertions */
  needsUpdate: Promise<boolean>;

  /**
   * @param patch the starting app patch (will not be mutated unless 'mutable')
   */
  constructor(
    readonly graph: JrqlGraph,
    time: TreeClock,
    readonly patch: PatchQuads,
    readonly mutable?: 'mutable') {
    // If mutable, we treat the app patch as assertions
    this.assertions = mutable ? patch : new PatchQuads();
    this['@ticks'] = time.ticks;
    this.needsUpdate = Promise.resolve(true);
  }

  get ready(): Promise<InterimUpdatePatch> {
    return this.needsUpdate.then(async (needsUpdate) => {
      if (needsUpdate) {
        const newPatch = this.mutable ? this.assertions :
          new PatchQuads(this.patch).append(this.assertions);
        this.needsUpdate = Promise.resolve(false);
        return Object.assign(this, await this.asDeleteInsert(newPatch));
      } else {
        return this;
      }
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

  private async asDeleteInsert(patch: DefinitePatch) {
    return {
      '@delete': await this.toSubjects(patch.oldQuads ?? []),
      '@insert': await this.toSubjects(patch.newQuads ?? [])
    };
  }

  /**
   * @returns flattened subjects compacted with no context
   * @see https://www.w3.org/TR/json-ld11/#flattened-document-form
   */
  private async toSubjects(quads: Iterable<Quad>): Promise<Subject[]> {
    // The flatten function is guaranteed to create a graph object
    const graph: any = await flatJsonLd(await rdfToJson(quads), {});
    return graph['@graph'];
  }
}
