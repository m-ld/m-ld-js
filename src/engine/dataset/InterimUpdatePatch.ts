import { Assertions, InterimUpdate, MeldPreUpdate } from '../../api';
import { Reference, SubjectProperty, Update, Value } from '../../jrql-support';
import { JrqlGraph } from './JrqlGraph';
import { jrqlValue, RdfOptions, SubjectGraph } from '../SubjectGraph';
import { Iri } from '@m-ld/jsonld';
import { Quad } from '../quads';
import { JsonldContext } from '../jsonld';
import { array } from '../../util';
import { Consumable } from 'rx-flowable';
import { TidsStore } from './TidsStore';
import { flatMap, ignoreIf } from 'rx-flowable/operators';
import { consume } from 'rx-flowable/consume';
import { map } from 'rxjs/operators';
import { JrqlContext } from '../SubjectQuads';
import { JrqlPatchQuads } from './JrqlQuads';

export class InterimUpdatePatch implements InterimUpdate {
  /** If mutable, we allow mutation of the input patch */
  private readonly mutable: boolean;
  /** Assertions made by the constraint (not including the app patch if not mutable) */
  private readonly assertions: JrqlPatchQuads;
  /** Entailments made by the constraint */
  private entailments = new JrqlPatchQuads();
  /** Whether to recreate the insert & delete fields from the assertions */
  private needsUpdate: PromiseLike<boolean>;
  /** Cached interim update, lazily recreated after changes */
  private _update: MeldPreUpdate | undefined;
  /** Aliases for use in updates */
  private subjectAliases = new Map<Iri | null, { [property in '@id' | string]: SubjectProperty }>();

  static principalRef(
    principalId: string | null,
    ctx?: JsonldContext
  ): Reference | undefined {
    return principalId != null ?
      { '@id': ctx ? ctx.compactIri(principalId) : principalId } : undefined;
  }

  /**
   * @param graph
   * @param tidsStore
   * @param userCtx
   * @param patch the starting app patch (will not be mutated unless 'mutable')
   * @param principalId
   * @param agree
   * @param mutable?
   */
  constructor(
    private readonly graph: JrqlGraph,
    private readonly tidsStore: TidsStore,
    private readonly userCtx: JrqlContext,
    private readonly patch: JrqlPatchQuads,
    private readonly principalId: Iri | null,
    private agree: any | null,
    { mutable }: { mutable: boolean }
  ) {
    this.mutable = mutable;
    // If mutable, we treat the app patch as assertions
    this.assertions = mutable ? patch : new JrqlPatchQuads();
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
    const finalPatch = new JrqlPatchQuads(this.allAssertions).append(this.entailments);
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

  remove = (assertions: Assertions) =>
    this.mutate(() => {
      let removed = false;
      if (assertions['@delete'] != null) {
        removed ||= !!this.assertions.remove('deletes',
          this.graph.graphQuads(assertions['@delete'], this.userCtx)).length;
      }
      if (assertions['@insert'] != null) {
        removed ||= !!this.assertions.remove('inserts',
          this.graph.graphQuads(assertions['@insert'], this.userCtx)).length;
      }
      return removed;
    });

  alias(subjectId: Iri | null, property: '@id' | Iri, alias: Iri | SubjectProperty): void {
    this.subjectAliases.set(subjectId, {
      ...this.subjectAliases.get(subjectId), [property]: alias
    });
  }

  // TODO: unit test
  hidden(subjectId: Iri, property: Iri): Consumable<Value> {
    const subject = this.userTerm(subjectId);
    const predicate = this.userTerm(property);
    // Hidden edges exist in the TID store...
    return this.tidsStore.findTriples(subject, predicate).pipe(
      flatMap(object => consume(
        // ... but not the graph...
        this.graph.quads.query(subject, predicate, object)
          .toArray({ limit: 1 })
          .then(([found]) => found ? null :
            this.graph.quads.quad(subject, predicate, object)))),
      // ... and have not been asserted away by this update
      ignoreIf<Quad>(quad => quad == null || this.assertions.deletes.has(quad)),
      map(({ value: quad, next }) => ({ value: jrqlValue(property, quad.object), next })));
  }

  private userTerm(iri: Iri) {
    return this.graph.rdf.namedNode(this.userCtx.expandTerm(iri));
  }

  private createUpdate(patch: JrqlPatchQuads, ctx?: JsonldContext): MeldPreUpdate {
    const opts: RdfOptions = {
      ctx, aliases: (subject, property) => this.subjectAliases.get(subject)?.[property]
    };
    return {
      '@delete': SubjectGraph.fromRDF([...patch.deletes], opts),
      '@insert': SubjectGraph.fromRDF([...patch.inserts], opts),
      '@update': SubjectGraph.fromRDF([...patch.sharedDataOps()], {
        ...opts, values: i => patch.getDataOpUpdate(i)
      }),
      '@principal': InterimUpdatePatch.principalRef(this.principalId, ctx),
      // Note that agreement specifically checks truthy-ness, not just non-null
      '@agree': this.agree || undefined
    };
  }
  private mutate(fn: () => Promise<boolean> | boolean) {
    this.needsUpdate = this.needsUpdate.then(
      async needsUpdate => (await fn()) || needsUpdate);
  }

  private get allAssertions() {
    return this.mutable ? this.assertions :
      new JrqlPatchQuads(this.patch).append(this.assertions);
  }
}
