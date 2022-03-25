import { OrmDomain, OrmState, OrmSubject } from '../orm';
import {
  GraphSubject, InterimUpdate, MeldConstraint, MeldExtensions, MeldPreUpdate, MeldReadState
} from '../api';
import { Shape } from '../shacl/index';
import { Describe, Reference, Subject } from '../jrql-support';
import { M_LD } from '../ns';
import { MeldError } from '../engine/MeldError';
import { Iri } from 'jsonld/jsonld-spec';

export class WritePermitted extends OrmDomain implements MeldExtensions {
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
        '@type': M_LD.JS.commonJsModule,
        [M_LD.JS.require]: '@m-ld/m-ld/dist/constraints/WritePermitted',
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
  static declareControlled = (permissionIri: Iri, ...controlledShapes: (Subject | Reference)[]): Subject => ({
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

  private permissions: WritePermission[] = [];

  checkPermissions = (state: MeldReadState, update: InterimUpdate) =>
    this.withActiveState(state, () =>
      Promise.all(this.permissions.map(permission => permission.check(state, update))));

  readonly constraints: MeldConstraint[] = [{
    check: this.checkPermissions,
    apply: this.checkPermissions
  }];

  async initialise(state: MeldReadState) {
    await this.withActiveState(state, async orm => {
      // Read the available permissions
      await state.read<Describe>({
        '@describe': '?permission',
        '@where': { '@id': '?permission', '@type': M_LD.WritePermission }
      }).each(src => this.permissions.push(new WritePermission(src, orm)));
      await orm.update();
    });
  }

  onUpdate(update: MeldPreUpdate, state: MeldReadState) {
    return this.withActiveState(state, async orm => {
      // Capture any new permissions
      for (let src of update['@insert'])
        if (src['@type'] === M_LD.WritePermission)
          this.permissions.push(new WritePermission(src, orm));
      await orm.update(update);
      // Remove any deleted permissions
      this.permissions = this.permissions.filter(permission => !permission.deleted);
    });
  }
}

export class WritePermission extends OrmSubject {
  controlledShapes: Shape[];

  constructor(src: GraphSubject, readonly orm: OrmState) {
    super(src);
    this.initSrcProperty(src, M_LD.controlledShape, [Array, Subject],
      () => this.controlledShapes.map(s => s.src),
      async (v: GraphSubject[]) => this.controlledShapes = await orm.get(v, Shape.from));
  }

  async check(state: MeldReadState, interim: InterimUpdate) {
    const update = await interim.update;
    for (let shape of this.controlledShapes) {
      const affected = await shape.affected(state, update);
      if (affected.length > 0)
        return this.checkPrincipal(update['@principal'], shape);
    }
  }

  private async checkPrincipal(principalRef: Reference | undefined, shape: Shape) {
    if (principalRef == null)
      throw new MeldError('Unauthorised',
        `No identified principal for controlled shape ${shape}`);
    // Load the principal's permissions if we don't already have them
    const principal = await this.orm.get(
      principalRef, src => new Principal(src));
    if (!principal.permissions.has(this.src['@id']))
      throw new MeldError('Unauthorised');
  }
}

class Principal extends OrmSubject {
  permissions: Set<Iri>;

  constructor(src: GraphSubject) {
    super(src);
    this.initSrcProperty(src, M_LD.hasPermission, [Array, Reference],
      () => [...this.permissions].map(id => ({ '@id': id })),
      v => this.permissions = new Set(v.map(ref => ref['@id'])));
  }
}