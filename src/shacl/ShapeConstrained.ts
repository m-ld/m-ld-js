import {
  GraphSubject, InterimUpdate, MeldConstraint, MeldExtensions, MeldPreUpdate, MeldReadState
} from '../api';
import { ExtensionSubject, OrmUpdating } from '../orm/index';
import { Shape } from './Shape';
import { isReference, Reference, Subject } from '../jrql-support';
import { M_LD } from '../ns';
import { ExtensionSubjectInstance } from '../orm/ExtensionSubject';
import { JsProperty, JsType } from '../js-support';
import { Iri } from '@m-ld/jsonld';
import { SubjectUpdater } from '../updates';

export class ShapeConstrained implements ExtensionSubjectInstance, MeldExtensions {
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

  /** @internal */
  shapes: Shape[];

  /**
   * Note special case constraint handling for subject deletion.
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
  initialise(src: GraphSubject, orm: OrmUpdating, ext: ExtensionSubject<this>): this {
    // We know we're a singleton; add our controlled shapes property
    ext.initSrcProperty(src,
      [this, 'shapes'],
      new JsProperty(M_LD.controlledShape, JsType.for(Array, Subject)),
      { orm, construct: Shape.from });
    return this;
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