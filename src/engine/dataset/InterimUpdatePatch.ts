import { DeleteInsert, InterimUpdate, MeldPreUpdate } from '../../api';
import { Subject, SubjectProperty, Update } from '../../jrql-support';
import { PatchQuads } from '.';
import { JrqlGraph } from './JrqlGraph';
import { GraphAliases, SubjectGraph } from '../SubjectGraph';
import { Iri } from 'jsonld/jsonld-spec';
import { Quad } from '../quads';
import { ActiveContext, compactIri } from '../jsonld';
import { array } from '../../util';

export class InterimUpdatePatch implements InterimUpdate {
  /** If mutable, we allow mutation of the input patch */
  private readonly mutable: boolean;
  /** Assertions made by the constraint (not including the app patch if not mutable) */
  private readonly assertions: PatchQuads;
  /** Entailments made by the constraint */
  private entailments = new PatchQuads();
  /** Whether to recreate the insert & delete fields from the assertions */
  private needsUpdate: PromiseLike<boolean>;
  /** Cached interim update, lazily recreated after changes */
  private _update: MeldPreUpdate | undefined;
  /** Aliases for use in updates */
  private subjectAliases = new Map<Iri | null, { [property in '@id' | string]: SubjectProperty }>();

  /**
   * @param graph
   * @param userCtx
   * @param patch the starting app patch (will not be mutated unless 'mutable')
   * @param principalId
   * @param agree
   * @param mutable?
   */
  constructor(
    private readonly graph: JrqlGraph,
    private readonly userCtx: ActiveContext,
    private readonly patch: PatchQuads,
    private readonly principalId: Iri | null,
    private agree: any | null,
    { mutable }: { mutable: boolean }
  ) {
    this.mutable = mutable;
    // If mutable, we treat the app patch as assertions
    this.assertions = mutable ? patch : new PatchQuads();
    this.needsUpdate = Promise.resolve(true);
  }

  /** @returns the final update to be presented to the app */
  async finalise() {
    await this.needsUpdate; // Ensure up-to-date with any changes
    this.needsUpdate = {
      then: () => {
        throw 'Interim update has been finalised';
      }
    };
    // The final update to the app includes all assertions and entailments
    const finalPatch = new PatchQuads(this.allAssertions).append(this.entailments);
    return {
      userUpdate: this.createUpdate(finalPatch, this.userCtx),
      // TODO: Make the internal update conditional on anyone wanting it
      internalUpdate: this.createUpdate(finalPatch),
      assertions: this.assertions,
      entailments: this.entailments,
      agree: this.agree
    };
  }

  /** @returns an interim update to be presented to constraints */
  get update(): Promise<MeldPreUpdate> {
    return Promise.resolve(this.needsUpdate).then(needsUpdate => {
      if (needsUpdate || this._update == null) {
        this.needsUpdate = Promise.resolve(false);
        this._update = this.createUpdate(this.allAssertions);
      }
      return this._update;
    });
  }

  assert = (update: Update) => this.mutate(async () => {
    let changed = false;
    if (update['@delete'] != null || update['@insert'] != null) {
      const patch = await this.graph.write(update, this.userCtx);
      this.assertions.append(patch);
      changed ||= !patch.isEmpty;
    }
    // We cannot upgrade an immutable update to an agreement
    if (update['@agree'] != null && this.mutable) {
      // A falsey agree removes the agreement, truthy accumulates
      this.agree = update['@agree'] ? this.agree == null ? update['@agree'] :
        array(this.agree).concat(update['@agree']) : null;
      changed ||= true;
    }
    return changed;
  });

  entail = (update: Update) => this.mutate(async () => {
    const patch = await this.graph.write(update, this.userCtx);
    this.entailments.append(patch);
    return false;
  });

  remove = (key: keyof DeleteInsert<any>, pattern: Subject | Subject[]) =>
    this.mutate(() => {
      const toRemove = this.graph.graphQuads(pattern, this.userCtx);
      const removed = this.assertions.remove(
        key == '@delete' ? 'deletes' : 'inserts', toRemove);
      return removed.length !== 0;
    });

  alias(subjectId: Iri | null, property: '@id' | Iri, alias: Iri | SubjectProperty): void {
    this.subjectAliases.set(subjectId, {
      ...this.subjectAliases.get(subjectId), [property]: alias
    });
  }

  private aliases: GraphAliases = (subject, property) => {
    return this.subjectAliases.get(subject)?.[property];
  };

  private createUpdate(patch: PatchQuads, ctx?: ActiveContext): MeldPreUpdate {
    return {
      '@delete': this.quadSubjects(patch.deletes, ctx),
      '@insert': this.quadSubjects(patch.inserts, ctx),
      '@principal': this.principalId != null ?
        { '@id': compactIri(this.principalId, ctx) } : undefined,
      // Note that agreement specifically checks truthy-ness, not just non-null
      '@agree': this.agree || undefined
    };
  }

  private quadSubjects(quads: Iterable<Quad>, ctx?: ActiveContext) {
    return SubjectGraph.fromRDF([...quads], { aliases: this.aliases, ctx });
  }

  private mutate(fn: () => Promise<boolean> | boolean) {
    this.needsUpdate = this.needsUpdate.then(
      async needsUpdate => (await fn()) || needsUpdate);
  }

  private get allAssertions() {
    return this.mutable ? this.assertions :
      new PatchQuads(this.patch).append(this.assertions);
  }
}
