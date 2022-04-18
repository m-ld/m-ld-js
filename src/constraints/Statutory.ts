import {
  AgreementCondition, GraphSubject, InterimUpdate, MeldConstraint, MeldExtensions, MeldPreUpdate,
  MeldReadState
} from '../api';
import { Shape } from '../shacl';
import { M_LD } from '../ns';
import { Describe, Reference, Subject } from '../jrql-support';
import { OrmDomain, OrmState, OrmSubject } from '../orm';
import { MeldError } from '../engine/MeldError';
import { Iri } from 'jsonld/jsonld-spec';

export class Statutory extends OrmDomain implements MeldExtensions {
  /**
   * Extension declaration. Insert into the domain data to install the
   * extension. For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(Statutory.declare(0));
   * ```
   *
   * @param priority the preferred index into the existing list of extensions
   * (lower value is higher priority).
   */
  static declare = (priority: number): Subject => ({
    '@id': M_LD.extensions,
    '@list': {
      [priority]: {
        '@id': `${M_LD.EXT.$base}constraints/Statutory`,
        '@type': M_LD.JS.commonJsModule,
        [M_LD.JS.require]: '@m-ld/m-ld/dist/constraints/Statutory',
        [M_LD.JS.className]: 'Statutory'
      }
    }
  });

  /**
   * Declares that some set of shapes are statutory, that is, they cannot change
   * without agreement. In order to have agreement, the given conditions must be
   * met.
   *
   * For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(Statutory.declareStatute(
   *   'https://alice.example/profile#me',
   *   { '@id': 'documentStateShape' }
   * ));
   * ```
   *
   * @param statuteId the new statute's identity. Omit to auto-generate.
   * @param statutoryShapes shape Subjects, or References to pre-existing shapes
   * @param sufficientConditions References to pre-existing agreement conditions
   */
  static declareStatute = ({ statuteId, statutoryShapes, sufficientConditions }: {
    statuteId?: Iri,
    statutoryShapes: (Subject | Reference)[],
    sufficientConditions: Reference | Reference[]
  }): Subject => ({
    '@id': statuteId,
    '@type': M_LD.Statute,
    [M_LD.statutoryShape]: statutoryShapes,
    [M_LD.sufficientCondition]: sufficientConditions
  });

  /**
   * Declares a principal to have authority over some shape, for example
   * (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(Statutory.declareAuthority(
   *   'https://alice.example/profile#me',
   *   { '@id': 'documentStateShape' }
   * ));
   * ```
   *
   * @param principalIri the principal's identity. As for all domain data, this
   * can be a relative IRI, e.g. `'fred'`.
   * @param shape the shape Subject, or a Reference to a pre-existing shape
   */
  static declareAuthority = (principalIri: Iri, shape: Subject | Reference): Subject => ({
    '@id': principalIri,
    [M_LD.hasAuthority]: shape
  });

  private statutes: Statute[] = [];

  readonly constraints: MeldConstraint[] = [{
    check: (state, update) => this.withActiveState(state, () =>
      Promise.all(this.statutes.map(statute => statute.check(state, update))))
  }];

  readonly agreementConditions: AgreementCondition[] = [{
    test: (state, update) => this.withActiveState(state, () =>
      Promise.all(this.statutes.map(statute => statute.test(state, update))))
  }];

  async initialise(state: MeldReadState) {
    await this.withActiveState(state, async orm => {
      // Read the available statutes
      await state.read<Describe>({
        '@describe': '?statute',
        '@where': { '@id': '?statute', '@type': M_LD.Statute }
      }).each(src => this.statutes.push(new Statute(src, orm)));
      await orm.update();
    });
  }

  onUpdate(update: MeldPreUpdate, state: MeldReadState) {
    return this.withActiveState(state, async orm => {
      // Capture any new statutes
      for (let src of update['@insert'])
        if (src['@type'] === M_LD.Statute)
          this.statutes.push(new Statute(src, orm));
      await orm.update(update);
      // Remove any deleted statutes
      this.statutes = this.statutes.filter(statute => !statute.deleted);
    });
  }
}

/**
 * A scope of data for which an agreement is required, if the data is to change
 */
export class Statute extends OrmSubject implements MeldConstraint, AgreementCondition {
  /** shapes describing statutory graph content */
  statutoryShapes: Shape[];
  sufficientConditions: AgreementProver[];

  constructor(
    src: GraphSubject,
    orm: OrmState,
    prover = AgreementProver.from // For unit tests
  ) {
    super(src);
    this.initSrcProperty(src, M_LD.statutoryShape, [Array, Subject],
      () => this.statutoryShapes.map(s => s.src),
      async (v: GraphSubject[]) => this.statutoryShapes = await orm.get(v, Shape.from));
    this.initSrcProperty(src, M_LD.sufficientCondition, [Array, Subject],
      () => this.sufficientConditions.map(c => c.src),
      async (v: GraphSubject[]) => this.sufficientConditions = await orm.get(v, prover));
  }

  async check(state: MeldReadState, interim: InterimUpdate) {
    // Detect a change to the shape in the update
    const update = await interim.update;
    await this.withAffected(state, update, async affected => {
      for (let condition of this.sufficientConditions) {
        const proof = await condition.prove(state, affected, update['@principal']);
        if (proof) // Test for sufficient truthiness
          return interim.assert({ '@agree': proof });
      }
      throw `Agreement not provable for ${this.statutoryShapes}`;
    });
  }

  async test(state: MeldReadState, update: MeldPreUpdate) {
    await this.withAffected(state, update, async affected => {
      for (let condition of this.sufficientConditions) {
        if (await condition.test(state, affected, update['@agree'], update['@principal']))
          return;
      }
      throw `Inadequate agreement proof for ${this.statutoryShapes}`;
    });
  }

  private async withAffected(
    state: MeldReadState,
    update: MeldPreUpdate,
    proc: (affected: Shape[]) => Promise<unknown>
  ) {
    const affected: Shape[] = [];
    for (let statutory of this.statutoryShapes)
      affected.push(...await statutory.affected(state, update));
    if (affected.length > 0)
      return proc(affected);
  }
}

export abstract class AgreementProver extends OrmSubject {
  static from(src: GraphSubject, orm: OrmState): AgreementProver {
    if (src['@id'] === M_LD.hasAuthority)
      return new HasAuthority(src, orm);
    else
      throw new TypeError(`${src['@id']} is not an agreement condition`);
  }

  /** @returns a truthy proof, or a falsey lack of proof */
  abstract prove(
    state: MeldReadState,
    affected: Shape[],
    principal?: Reference
  ): Promise<any>;

  abstract test(
    state: MeldReadState,
    affected: Shape[],
    proof: any,
    principal?: Reference
  ): Promise<boolean>;
}

class HasAuthority extends AgreementProver {
  constructor(src: GraphSubject, readonly orm: OrmState) {
    super(src);
  }
  /**
   * Does the update's responsible principal have authority over the statutory
   * shapes being updated?
   */
  async prove(state: MeldReadState, affected: Shape[], principalRef?: Reference) {
    if (principalRef == null)
      throw new MeldError('Unauthorised', 'No identified principal');
    // Load the principal's authorities if we don't already have them
    const principal = await this.orm.get(
      principalRef, src => new Principal(src, this.orm));
    await this.orm.update();
    // For each affected shape, see if it refines an authority we have
    return !principal.deleted && affected.every(affectedShape =>
      principal.hasAuthority.some(authorisedShape =>
        affectedShape.refines(authorisedShape)));
  }

  test(state: MeldReadState, affected: Shape[], proof: any, principalRef?: Reference) {
    // We don't care about the proof - just check authority
    return this.prove(state, affected, principalRef);
  }
}

class Principal extends OrmSubject {
  hasAuthority: Shape[];

  constructor(src: GraphSubject, orm: OrmState) {
    super(src);
    this.initSrcProperty(src, M_LD.hasAuthority, [Array, Subject],
      () => this.hasAuthority.map(s => s.src),
      async (v: GraphSubject[]) => this.hasAuthority = await orm.get(v, Shape.from));
  }
}