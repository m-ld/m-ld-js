import {
  GraphSubject, InterimUpdate, MeldConstraint, MeldPlugin, MeldPreUpdate, MeldReadState
} from '../api';
import { ExtensionSubject, OrmUpdating } from '../orm';
import { Shape } from './Shape';
import { isReference, Reference, Subject } from '../jrql-support';
import { M_LD } from '../ns';
import { ExtensionSubjectInstance } from '../orm/ExtensionSubject';
import { JsProperty, JsType } from '../js-support';
import { Iri } from '@m-ld/jsonld';
import { SubjectUpdater } from '../updates';

/**
 * This extension allows an app to declare that the domain data must conform to
 * some defined {@link Shape shapes}. A collection of shapes is like a 'schema'
 * or 'object model'.
 *
 * The extension can be declared in the data using {@link declare}, or
 * instantiated and provided to the [clone function](/#clone) in the `app`
 * parameter, e.g.
 *
 * ```typescript
 * api = await clone(
 *   new MemoryLevel, MqttRemotes, config,
 *   new ShapeConstrained(new PropertyShape({
 *     path: 'http://ex.org/#name', count: 1
 *   })));
 * ```
 *
 * > Note that properties and types provided as initialisation parameters to
 * shapes (e.g. `path` above) must be fully-qualified IRIs according to the
 * vocabulary of the domain. See {@link MeldContext} for more information.
 *
 * If combining shape constraints with other extensions, it may be necessary
 * (and is safe) to use the {@link constraints} member of this class directly as
 * a sublist in a list of constraints.
 *
 * @see https://www.w3.org/TR/shacl/
 * @category Experimental
 * @experimental
 * @noInheritDoc
 */
export class ShapeConstrained implements ExtensionSubjectInstance, MeldPlugin {
  /**
   * Extension declaration. Insert into the domain data to install the
   * extension. For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(ShapeConstrained.declare(0, PropertyShape.declare({
   *   path: 'name', count: 1
   * })));
   * ```
   *
   * Note that declaration of shapes will not retrospectively apply constraints
   * to any existing subjects in the domain. It's the app's responsibility to
   * correct existing data, if necessary.
   *
   * @param priority the preferred index into the existing list of extensions
   * (lower value is higher priority).
   * @param controlledShapes shape Subjects, or References to pre-existing shapes
   */
  static declare = (
    priority: number,
    ...controlledShapes: (Subject | Reference)[]
  ): Subject => ({
    '@id': M_LD.extensions,
    '@list': {
      [priority]: ExtensionSubject.declareMeldExt(
        'shacl', 'ShapeConstrained', {
          [M_LD.controlledShape]: controlledShapes
        })
    }
  });

  /**
   * The shapes to which domain data must conform.
   */
  shapes: Shape[];

  /**
   * @param shapes The shapes to which domain data must conform
   */
  constructor(...shapes: Shape[]) {
    this.shapes = shapes;
  }

  /**
   * Note special case constraint handling for subject deletion.
   * @internal
   */
  constraints: MeldConstraint[] = [{
    check: async (state: MeldReadState, interim: InterimUpdate) => {
      // When checking, allow a subject to not conform to individual shape
      // if its final state is empty – a "full delete".
      const detector = new DeleteDetector(state, await interim.update);
      for (let shape of this.shapes) {
        for (let result of await shape.check(state, interim)) {
          if (!(await detector.isFullyDeleted(result.focusNode)))
            throw result.resultMessage;
        }
      }
    },
    apply: async (state: MeldReadState, interim: InterimUpdate) => {
      // When applying, individual shape checks may re-assert deleted properties.
      // If the original update caused a full delete, override those assertions.
      const detector = new DeleteDetector(state, await interim.update);
      for (let shape of this.shapes) {
        for (let result of await shape.apply(state, interim)) {
          if (result.correction && await detector.isFullyDeleted(result.focusNode))
            interim.remove(result.correction);
        }
      }
    }
  }];

  /** @internal */
  initFromData(src: GraphSubject, orm: OrmUpdating, ext: ExtensionSubject<this>) {
    // We know we're a singleton; add our controlled shapes property
    ext.initSrcProperty(src,
      [this, 'shapes'],
      new JsProperty(M_LD.controlledShape, JsType.for(Array, Subject)),
      { orm, construct: Shape.from });
  }
}

class DeleteDetector extends SubjectUpdater {
  private readonly fullyDeletedIds = new Set<Iri>();

  constructor(
    private readonly state: MeldReadState,
    update: MeldPreUpdate
  ) {
    super(update);
  }

  async isFullyDeleted(focusNode: GraphSubject) {
    const id = focusNode['@id'];
    if (this.fullyDeletedIds.has(id)) {
      return true;
    } else {
      // Don't load if there are properties in the non-conforming state
      if (isReference(focusNode) && isReference(await this.loadFinal(id))) {
        this.fullyDeletedIds.add(id);
        return true;
      }
    }
    return false;
  }

  private async loadFinal(id: Iri) {
    return this.update(await this.state.get(id) ?? { '@id': id });
  }
}