import {
  AgreementCondition, DeleteInsert, GraphSubject, GraphUpdate, InterimUpdate, MeldConstraint,
  MeldExtensions, MeldPreUpdate, MeldReadState
} from '../api';
import { M_LD } from '../ns';
import { Describe, isPropertyObject, isReference, Reference, Subject } from '../jrql-support';
import { ExtensionSubject, OrmSubject, OrmUpdating } from '../orm';
import { MeldError } from '../engine/MeldError';
import { Iri } from '@m-ld/jsonld';
import { SubjectGraph } from '../engine/SubjectGraph';
import { asSubjectUpdates } from '../updates';
import { Logger } from 'loglevel';
import { getIdLogger } from '../engine/logging';
import { JsType } from '../js-support';
import { property } from '../orm/OrmSubject';
import { CacheMissListener, OrmScope } from '../orm/OrmDomain';
import { Shape } from '../shacl/Shape';
import { ExtensionSubjectInstance } from '../orm/ExtensionSubject';

/**
 * This extension allows an app to require that certain changes, such as changes
 * to access controls, are _agreed_ before they are shared in the domain. This
 * can be important for security and data integrity. See the white paper link
 * below for more details.
 *
 * - The extension can be declared in the data using {@link declare}.
 * - _Statutes_ can be declared in the data using {@link declareStatute}.
 * - _Authorities_ can be declared in the data using {@link declareAuthority}.
 *
 * @see [the white paper](https://github.com/m-ld/m-ld-security-spec/blob/main/design/suac.md)
 * @category Experimental
 * @experimental
 * @noInheritDoc
 */
export class Statutory implements ExtensionSubjectInstance, MeldExtensions {
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
      [priority]: ExtensionSubject.declareMeldExt(
        'statutes', 'Statutory')
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
  static declareStatute = ({
    statuteId,
    statutoryShapes,
    sufficientConditions = [{ '@id': M_LD.hasAuthority }]
  }: {
    statuteId?: Iri,
    statutoryShapes: Subject | Reference | (Subject | Reference)[],
    sufficientConditions: Subject | Reference | (Subject | Reference)[]
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

  /** @internal */
  private readonly statutes = new Map<Iri, Statute>();
  /** @internal */
  private /*readonly*/ scope: OrmScope;
  /** @internal */
  private /*readonly*/ log: Logger;

  async initialise(_src: GraphSubject, orm: OrmUpdating): Promise<this> {
    const config = orm.domain.config;
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.scope = orm.domain.createScope()
      .on('deleted', this.onSubjectDeleted)
      .on('cacheMiss', this.onSubjectInserted);
    await orm.latch(state => state.read<Describe>({
      '@describe': '?statute',
      '@where': { '@id': '?statute', '@type': M_LD.Statute }
    }).each(src => this.loadStatute(src, orm)));
    return this;
  }

  invalidate(): void {
    this.scope.invalidate();
  }

  private onSubjectDeleted = (deleted: OrmSubject) =>
    // Remove any deleted statutes
    this.statutes.delete(deleted.src['@id']);

  private onSubjectInserted: CacheMissListener = async (insert, orm) => {
    // Capture any new statutes (the type has been inserted)
    if (insert['@type'] === M_LD.Statute)
      await this.loadStatute(insert, orm);
  };

  private async loadStatute(src: GraphSubject, orm: OrmUpdating) {
    // Putting into both our statutes map and the domain cache
    this.statutes.set(src['@id'], await orm.get(src, src =>
      new Statute(src, orm, this.scope, async src => {
        if (src['@id'] === M_LD.hasAuthority)
          return new HasAuthority(src, this.scope, this.log);
        else
          return ExtensionSubject.instance(src, orm);
      }), this.scope));
  }

  constraints = [{
    check: (state: MeldReadState, update: InterimUpdate) =>
      Promise.all(Array.from(this.statutes.values(),
        statute => statute.check(state, update)))
  }];

  agreementConditions = [{
    test: (state: MeldReadState, update: MeldPreUpdate) =>
      Promise.all(Array.from(this.statutes.values(),
        statute => statute.test(state, update)))
  }];
}

/**
 * A scope of data for which an agreement is required, if the data is to change
 * @internal
 */
export class Statute extends OrmSubject implements MeldConstraint, AgreementCondition {
  /** shapes describing statutory graph content */
  @property(JsType.for(Array, Subject), M_LD.statutoryShape)
  statutoryShapes: Shape[];
  @property(JsType.for(Array, Subject), M_LD.sufficientCondition)
  sufficientConditions: ShapeAgreementCondition[];

  constructor(
    src: GraphSubject,
    orm: OrmUpdating,
    scope: OrmScope,
    prover: (src: GraphSubject) => Promise<ShapeAgreementCondition & OrmSubject>
  ) {
    super(src);
    this.initSrcProperties(src, {
      statutoryShapes: { orm, construct: Shape.from },
      sufficientConditions: { orm, scope, construct: prover }
    });
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
      const failures: string[] = [];
      for (let condition of this.sufficientConditions) {
        const { '@agree': proof, '@principal': principal } = update;
        const testResult = await condition.test(state, affected, proof, principal);
        if (testResult === true)
          return;
        failures.push(testResult);
      }
      throw `Inadequate agreement proof for ${this.statutoryShapes}, ` +
      `failures: ${[...failures]}`;
    });
  }

  private async withAffected(
    state: MeldReadState,
    update: MeldPreUpdate,
    proc: (affected: GraphUpdate) => Promise<unknown>
  ) {
    const affected = new AffectedUpdate;
    for (let statutory of this.statutoryShapes)
      affected.add(await statutory.affected(state, update));
    if (!affected.isEmpty)
      return proc(affected.asUpdate());
  }
}

/** @internal */
class AffectedUpdate {
  affected: {
    [id: string]: {
      '@delete'?: Subject & Reference;
      '@insert'?: Subject & Reference;
    }
  } = {};

  constructor(update?: GraphUpdate) {
    if (update != null)
      this.affected = asSubjectUpdates(update);
  }

  add(update: GraphUpdate) {
    this.addKey('@delete', update);
    this.addKey('@insert', update);
  }

  subtract(update: GraphUpdate) {
    this.subtractKey('@delete', update);
    this.subtractKey('@insert', update);
  }

  get isEmpty() {
    return Object.keys(this.affected).length === 0;
  }

  asUpdate() {
    return {
      '@delete': this.keyGraph('@delete'),
      '@insert': this.keyGraph('@insert')
    };
  }

  private addKey(key: keyof DeleteInsert<any>, update: GraphUpdate) {
    for (let subject of update[key]) {
      (this.affected[subject['@id']] ??= {})[key] ??= { '@id': subject['@id'] };
      // FIXME: this assumes the values are irrelevant!
      Object.assign(this.affected[subject['@id']][key]!, subject);
    }
  }

  private subtractKey(key: keyof DeleteInsert<any>, update: GraphUpdate) {
    for (let subject of update[key]) {
      const ourSubject = this.affected[subject['@id']]?.[key];
      if (ourSubject != null) {
        // FIXME: this assumes the values are irrelevant!
        for (let property in subject)
          if (isPropertyObject(property, subject[property]))
            delete ourSubject[property];
        if (isReference(ourSubject))
          delete this.affected[subject['@id']][key];
        if (Object.values(this.affected[subject['@id']]).every(v => v == null))
          delete this.affected[subject['@id']];
      }
    }
  }

  private keyGraph(key: keyof DeleteInsert<any>) {
    return new SubjectGraph(Object
      .values(this.affected)
      .map(subjectUpdate => subjectUpdate[key])
      .filter((subject): subject is GraphSubject => subject != null));
  }
}

/** @internal */
export interface ShapeAgreementCondition {
  /** @returns a truthy proof, or a falsey lack of proof */
  prove(
    state: MeldReadState,
    affected: GraphUpdate,
    principal?: Reference
  ): Promise<any>;

  /** @returns `true` if condition passed, otherwise error string */
  test(
    state: MeldReadState,
    affected: GraphUpdate,
    proof: any,
    principal?: Reference
  ): Promise<true | string>;
}

/** @internal */
export class HasAuthority extends OrmSubject implements ShapeAgreementCondition {
  constructor(
    src: GraphSubject,
    readonly scope: OrmScope,
    readonly log?: Logger
  ) {
    super(src);
  }

  /**
   * Does the update's responsible principal have authority over the statutory
   * shapes being updated?
   */
  async prove(state: MeldReadState, affected: GraphUpdate, principalRef?: Reference) {
    if (principalRef == null)
      throw new MeldError('Unauthorised', 'No identified principal');
    // Load the principal's authorities if we don't already have them
    const principal = await this.scope.domain.updating(state, orm => orm.get(
      principalRef, src => new Principal(src, orm), this.scope));
    if (!principal.deleted) {
      // Every affected subject must be covered by an authority assignment.
      // For each authority, subtract anything affected by that authority.
      const remaining = new AffectedUpdate(affected);
      for (let authority of principal.hasAuthority)
        remaining.subtract(await authority.affected(state, affected));
      return remaining.isEmpty;
    } else {
      this.log?.debug('No proof possible for affected', affected);
      return false;
    }
  }

  async test(state: MeldReadState, affected: GraphUpdate, proof: any, principalRef?: Reference) {
    // We don't care about the proof - just check authority
    return await this.prove(state, affected, principalRef) || 'Principal does not have authority';
  }
}

/** @internal */
class Principal extends OrmSubject {
  @property(JsType.for(Array, Subject), M_LD.hasAuthority)
  hasAuthority: Shape[];

  constructor(src: GraphSubject, orm: OrmUpdating) {
    super(src);
    this.initSrcProperties(src, {
      hasAuthority: { orm, construct: Shape.from }
    });
  }
}