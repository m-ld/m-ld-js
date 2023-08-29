import { Assertions, InterimUpdate, isSharedDatatype, MeldPreUpdate } from '../../api';
import { Reference, SubjectProperty, Update, Value } from '../../jrql-support';
import { JrqlGraph } from './JrqlGraph';
import { jrqlValue, RdfOptions, SubjectGraph } from '../SubjectGraph';
import { Iri } from '@m-ld/jsonld';
import { isLiteralTriple, LiteralTriple, Quad, QuadSet, TripleMap } from '../quads';
import { JsonldContext } from '../jsonld';
import { array } from '../../util';
import { Consumable, drain } from 'rx-flowable';
import { TidsStore } from './TidsStore';
import { flatMap, ignoreIf } from 'rx-flowable/operators';
import { consume } from 'rx-flowable/consume';
import { map } from 'rxjs/operators';
import { JrqlDataQuad, JrqlPatchQuads, JrqlQuadOperation } from './JrqlQuads';
import { concatIter, mapIter } from '../util';
import async from '../async';
import { TxnContext } from './index';

export class InterimUpdatePatch implements InterimUpdate {
  /** The immutable starting app patch (will be mutated via assertions if 'mutable') */
  private readonly patch: JrqlQuadOperation;
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

  constructor(
    patch: JrqlPatchQuads,
    private readonly graph: JrqlGraph,
    private readonly tidsStore: TidsStore,
    private readonly userCtx: JsonldContext,
    private readonly principalId: Iri | null,
    private agree: any,
    private txc: TxnContext,
    { mutable }: { mutable: boolean }
  ) {
    this.patch = patch;
    // If mutable, we treat the app patch as assertions
    this.assertions = mutable ? patch : new JrqlPatchQuads();
    this.needsUpdate = Promise.resolve(true);
  }

  /** @returns the final update to be presented to the app */
  async finalise() {
    await this.needsUpdate; // Ensure up-to-date with any changes
    this.needsUpdate = { then: () => { throw 'Interim update has been finalised'; } };
    // The final update to the app will include all assertions and entailments
    const finalPatch = await this.finalisePatch();
    const userUpdate = this.createUpdate(finalPatch, this.userCtx);
    // TODO: Make the internal update conditional on anyone wanting it
    const internalUpdate = this.createUpdate(finalPatch);
    const { assertions, entailments, agree } = this;
    return { userUpdate, internalUpdate, assertions, entailments, agree };
  }

  /** If mutable, we allow mutation of the input patch */
  private get mutable() {
    return this.patch === this.assertions;
  }

  private get allAssertions() {
    return this.mutable ? this.assertions :
      new JrqlPatchQuads(this.patch).append(this.assertions);
  }

  async finalisePatch() {
    const finalPatch = new JrqlPatchQuads(this.allAssertions).append(this.entailments);
    await this.processSharedData(finalPatch);
    return finalPatch;
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
      const patch = await this.graph.write(update, this.userCtx, this.txc);
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
    const patch = await this.graph.write(update, this.userCtx, this.txc);
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

  /** @todo: unit test */
  hidden(subjectId: Iri, property: Iri): Consumable<Value> {
    const subject = this.userTerm(subjectId);
    const predicate = this.userTerm(property);
    // Hidden edges exist in the TID store...
    return this.tidsStore.findTriples(subject, predicate).pipe(
      flatMap(object => consume(
        // ... but not the graph...
        this.graph.quads.query(subject, predicate, object)
          .toArray({ limit: 1 })
          .then(([found]) => found ? null : this.graph.quads.quad(subject, predicate, object)))),
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
    const updates = [...patch.updates];
    return {
      '@delete': SubjectGraph.fromRDF([...patch.deletes], opts),
      '@insert': SubjectGraph.fromRDF([...patch.inserts], opts),
      '@update': SubjectGraph.fromRDF(updates.map(([triple]) => triple), {
        ...opts, values: i => updates[i][1].update
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

  private async processSharedData(patch: JrqlPatchQuads) {
    // Ensure the final patch knows about triples with data attached
    await this.graph.jrql.loadHasData(
      concatIter(patch.deletes, patch.inserts));
    // Ensure that every property with a shared literal is single-valued
    // The 'winning' literal is defined as the shared data with the highest UUID
    // Everything else should be either deleted or entailed away
    await Promise.all(mapIter(await this.finalSharedState(patch), async ([, quads]) => {
      // Load has-data state for any objects recovered from the backend
      await this.graph.jrql.loadHasData(quads);
      const { topQuad, conflicts } = this.prioritiseSharedSiblings(quads);
      if (topQuad) {
        if (this.mutable) {
          // If mutable, consider quads in the graph. If there are conflicts, fail.
          // Once established top quad, delete all others including hidden.
          if (conflicts.some(q => !q.hidden))
            throw `Multiple shared data values for ${topQuad.object.value}`;
          else
            this.assertions.append({ deletes: conflicts });
        } else {
          // If not mutable, consider quads in the graph and SU-Set. Leave the
          // top quad and hide all conflicts.
          this.entailments.append({ deletes: conflicts });
        }
        patch.append({ deletes: conflicts });
      }
    }));
  }

  private prioritiseSharedSiblings(quads: QuadSet<MaybeHiddenQuad>) {
    let topQuad: (MaybeHiddenQuad & LiteralTriple) | null = null;
    const conflicts: MaybeHiddenQuad[] = [...quads];
    for (let quad of quads) {
      if (this.isShared(quad) && (topQuad == null || quad.object.value > topQuad.object.value))
        topQuad = quad;
    }
    return { topQuad, conflicts: topQuad ? conflicts.filter(q => q !== topQuad) : conflicts };
  }

  private async finalSharedState(patch: JrqlPatchQuads) {
    // Note that assertions and entailments both make their way into the graph
    // so we don't have to distinguish them in the patch itself.
    const state = new TripleMap<QuadSet<MaybeHiddenQuad>>();
    const inState = async ({ subject: s, predicate: p }: Quad) => {
      const key = this.graph.rdf.quad(s, p, this.graph.rdf.variable('any'));
      let quadState = state.get(key);
      if (quadState == null) {
        quadState = await this.loadQuadState(s, p);
        state.set(key, quadState);
      }
      return quadState;
    }
    for (let quad of patch.deletes) {
      if (this.isShared(quad))
        (await inState(quad)).delete(quad);
    }
    for (let quad of patch.inserts) {
      if (this.isShared(quad))
        (await inState(quad)).add(quad);
    }
    return state;
  }

  private async loadQuadState(s: Quad['subject'], p: Quad['predicate']) {
    // Get quads from the graph
    const quadState = new QuadSet(await async.wrap(this.graph.quads.match(s, p)).toArray());
    // Add quads from the SU-Set with the hidden flag
    for (let o of await drain(this.tidsStore.findTriples(s, p))) {
      const quad: MaybeHiddenQuad = this.graph.quads.quad(s, p, o);
      quad.hidden = quadState.add(quad); // Hidden if not already there
    }
    return quadState;
  }

  private isShared(quad: JrqlDataQuad): quad is LiteralTriple & MaybeHiddenQuad {
    return isLiteralTriple(quad) && (
      (!!quad.hasData && quad.hasData.shared) || // if loaded from state
      (quad.object.typed?.type != null &&
        isSharedDatatype(quad.object.typed.type)) // if in patch
    );
  }
}

type MaybeHiddenQuad = JrqlDataQuad & { hidden?: boolean };
