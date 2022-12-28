import { GraphSubject, MeldExtensions } from '../api';
import { ExtensionSubject, OrmUpdating } from '../orm/index';
import { Shape } from './Shape';
import { Reference, Subject } from '../jrql-support';
import { M_LD } from '../ns';
import { ExtensionSubjectInstance } from '../orm/ExtensionSubject';
import { JsProperty, JsType } from '../js-support';

export class ShapeConstrained implements ExtensionSubjectInstance, MeldExtensions {
  /**
   * Extension declaration. Insert into the domain data to install the
   * extension. For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(ShapeConstrained.declare(0, { '@id': 'myShape' }));
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
  constraints: Shape[];

  /** @internal */
  initialise(src: GraphSubject, orm: OrmUpdating, ext: ExtensionSubject<this>): this {
    // We know we're a singleton; add our controlled shapes property
    ext.initSrcProperty(src,
      [this, 'constraints'],
      new JsProperty(M_LD.controlledShape, JsType.for(Array, Subject)),
      { orm, construct: Shape.from });
    return this;
  }
}