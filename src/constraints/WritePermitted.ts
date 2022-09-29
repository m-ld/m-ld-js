import { OrmDomain, OrmSubject, OrmUpdating } from '../orm';
import {
  GraphSubject, InterimUpdate, MeldExtensions, MeldPreUpdate, MeldReadState, StateManaged
} from '../api';
import { Shape } from '../shacl/index';
import { Describe, Reference, Subject } from '../jrql-support';
import { M_LD } from '../ns';
import { MeldError } from '../engine/MeldError';
import { Iri } from '@m-ld/jsonld';
import { JsType } from '../js-support';
import { property } from '../orm/OrmSubject';

/**
 * This extension allows an app to declare that certain security principals
 * (users) have permission to change certain data, identified by data {@link
  * Shape shapes}.
 *
 * - The extension can be declared in the data using {@link declare}.
 * - Controlled shapes can be declared in the data using {@link declareControlled}.
 * - Permissions can be assigned to principals using {@link declarePermission}.
 *
 * @see [the white paper](https://github.com/m-ld/m-ld-security-spec/blob/main/design/suac.md)
 * @category Experimental
 * @experimental
 * @noInheritDoc
 */
export class WritePermitted extends OrmDomain implements StateManaged<MeldExtensions> {
  /**
   * Extension declaration. Insert into the domain data to install the
   * extension. For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(WritePermitted.declare(0));
   * ```
   *
   * @param priority the preferred index into the existing list of extensions
   * (lower value is higher priority).
   */
  static declare = (priority: number): Subject => ({
    '@id': M_LD.extensions,
    '@list': {
      [priority]: {
        '@id': `${M_LD.EXT.$base}constraints/WritePermitted`,
        '@type': M_LD.JS.commonJsExport,
        [M_LD.JS.require]: '@m-ld/m-ld/ext/constraints/WritePermitted',
        [M_LD.JS.className]: 'WritePermitted'
      }
    }
  });

  /**
   * Declares that some set of shapes are controlled by a write permission, that
   * is, they cannot change without the principal being assigned this
   * permission.
   *
   * For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(Statutory.declareControlled(
   *   'namePermission', { '@id': 'documentStateShape' }
   * ));
   * ```
   *
   * @param permissionIri the permission's identity (e.g. for use in {@link declarePermission})
   * @param controlledShapes shape Subjects, or References to pre-existing shapes
   */
  static declareControlled = (
    permissionIri: Iri,
    ...controlledShapes: (Subject | Reference)[]
  ): Subject => ({
    '@id': permissionIri,
    '@type': M_LD.WritePermission,
    [M_LD.controlledShape]: controlledShapes
  });

  /**
   * Declares a principal to have permission to write some shape, for example
   * (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(Statutory.declarePermission(
   *   'https://alice.example/profile#me',
   *   { '@id': 'namePermission' }
   * ));
   * ```
   *
   * @param principalIri the principal's identity. As for all domain data, this
   * can be a relative IRI, e.g. `'fred'`.
   * @param permission the permission Reference
   */
  static declarePermission = (principalIri: Iri, permission: Reference): Subject => ({
    '@id': principalIri,
    [M_LD.hasPermission]: permission
  });

  private permissions = new Map<Iri, WritePermission>();

  /** @internal */
  checkPermissions = (state: MeldReadState, update: InterimUpdate) =>
    Promise.all(Array.from(this.permissions.values(),
      permission => permission.check(state, update)));

  /** @internal */
  ready(): Promise<MeldExtensions> {
    return this.upToDate().then(() => ({
      constraints: [{
        check: this.checkPermissions,
        apply: this.checkPermissions
      }]
    }));
  }

  /** @internal */
  initialise(state: MeldReadState) {
    // Read the available permissions
    return this.updating(state, orm =>
      state.read<Describe>({
        '@describe': '?permission',
        '@where': { '@id': '?permission', '@type': M_LD.WritePermission }
      }).each(src => this.loadPermission(src, orm)));
  }

  /** @internal */
  onUpdate(update: MeldPreUpdate, state: MeldReadState) {
    return this.updating(state, orm =>
      orm.updated(update, deleted => {
        // Remove any deleted permissions
        this.permissions.delete(deleted.src['@id']);
      }, async insert => {
        // Capture any new permissions
        if (insert['@type'] === M_LD.WritePermission)
          await this.loadPermission(insert, orm);
      }));
  }

  private async loadPermission(src: GraphSubject, orm: OrmUpdating) {
    // Putting into both our permissions map and the domain cache
    this.permissions.set(src['@id'],
      await orm.get(src, src => new WritePermission(src, orm)));
  }
}

/** @internal */
export class WritePermission extends OrmSubject {
  @property(JsType.for(Array, Subject), M_LD.controlledShape)
  controlledShapes: Shape[];
  domain: OrmDomain;

  constructor(src: GraphSubject, readonly orm: OrmUpdating) {
    super(src);
    this.initSrcProperties(src, {
      controlledShapes: { orm, construct: Shape.from }
    });
    this.domain = orm.domain;
  }

  async check(state: MeldReadState, interim: InterimUpdate) {
    const update = await interim.update;
    for (let shape of this.controlledShapes) {
      const affected = await shape.affected(state, update);
      if (affected['@delete'].length > 0 || affected['@insert'].length > 0)
        return this.checkPrincipal(state, update['@principal'], shape);
    }
  }

  private async checkPrincipal(
    state: MeldReadState,
    principalRef: Reference | undefined,
    shape: Shape
  ) {
    if (principalRef == null)
      throw new MeldError('Unauthorised',
        `No identified principal for controlled shape ${shape}`);
    // Load the principal's permissions if we don't already have them
    const principal = await this.domain.updating(state, orm => orm.get(
      principalRef, src => new Principal(src)));
    if (!principal.permissions.has(this.src['@id']))
      throw new MeldError('Unauthorised');
  }
}

/** @internal */
class Principal extends OrmSubject {
  @property(JsType.for(Array, Reference), M_LD.hasPermission)
  permissions: Set<Iri>;

  constructor(src: GraphSubject) {
    super(src);
    this.initSrcProperties(src, {
      permissions: {
        get: () => [...this.permissions].map(id => ({ '@id': id })),
        set: (v: Reference[]) => this.permissions = new Set(v.map(ref => ref['@id']))
      }
    });
  }
}