import { OrmDomain, OrmState, OrmSubject } from '../orm';
import {
  GraphSubject, InterimUpdate, MeldExtensions, MeldPreUpdate, MeldReadState, StateManaged
} from '../api';
import { Shape } from '../shacl/index';
import { Describe, Reference, Subject } from '../jrql-support';
import { M_LD } from '../ns';
import { MeldError } from '../engine/MeldError';
import { Iri } from 'jsonld/jsonld-spec';

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
        '@type': M_LD.JS.commonJsModule,
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

  checkPermissions = (state: MeldReadState, update: InterimUpdate) =>
    this.updating(state, () =>
      Promise.all([...this.permissions.values()]
        .map(permission => permission.check(state, update))));

  ready(): Promise<MeldExtensions> {
    return this.upToDate().then(() => ({
      constraints: [{
        check: this.checkPermissions,
        apply: this.checkPermissions
      }]
    }));
  }

  async initialise(state: MeldReadState) {
    await this.updating(state, async orm => {
      // Read the available permissions
      await state.read<Describe>({
        '@describe': '?permission',
        '@where': { '@id': '?permission', '@type': M_LD.WritePermission }
      }).each(src => this.loadPermission(src, orm));
      await orm.update();
    });
  }

  onUpdate(update: MeldPreUpdate, state: MeldReadState) {
    return this.updating(state, async orm => {
      await orm.update(update, deleted => {
        // Remove any deleted permissions
        this.permissions.delete(deleted.src['@id']);
      }, async insert => {
        // Capture any new permissions
        if (insert['@type'] === M_LD.WritePermission)
          await this.loadPermission(insert, orm);
      });
    });
  }

  private async loadPermission(src: GraphSubject, orm: OrmState) {
    // Putting into both our permissions map and the domain cache
    this.permissions.set(src['@id'], await orm.get(src, src => new WritePermission(src, orm)));
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
      if (affected['@delete'].length > 0 || affected['@insert'].length > 0)
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