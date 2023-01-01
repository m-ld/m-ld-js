import { ExtensionSubject, OrmSubject, OrmUpdating } from '../orm/index';
import { property } from '../orm/OrmSubject';
import { JsType } from '../js-support';
import { VocabReference } from '../jrql-support';
import { GraphSubject, GraphUpdate, InterimUpdate, MeldConstraint, MeldReadState } from '../api';
import { SH } from '../ns';

/**
 * Shapes are used to define patterns of data, which can be used to match or
 * validate state and operations.
 *
 * Declaration of a shape does not provide any runtime function by itself, but
 * they can be used by other extensions and app code.
 *
 * @see https://www.w3.org/TR/shacl/#constraints-section
 * @noInheritDoc
 * @category Experimental
 * @experimental
 */
export abstract class Shape extends OrmSubject implements MeldConstraint {
  /** @internal */
  @property(JsType.for(Set, VocabReference), SH.targetClass)
  targetClass: Set<VocabReference>;

  /** @see https://www.w3.org/TR/shacl/#terminology */
  static from(src: GraphSubject, orm: OrmUpdating): Shape | Promise<Shape> {
    if (SH.path in src) {
      const { PropertyShape } = require('./PropertyShape');
      return new PropertyShape(src);
    } else {
      return ExtensionSubject.instance(src, orm);
    }
  }

  /**
   * Capture precisely the data being affected by the given update which matches
   * this shape, either before or after the update is applied to the state.
   *
   * @returns filtered updates where the affected subject matches this shape
   */
  abstract affected(state: MeldReadState, update: GraphUpdate): Promise<GraphUpdate>;

  /** @inheritDoc */
  abstract check(state: MeldReadState, update: InterimUpdate): Promise<unknown>;
  /** @inheritDoc */
  abstract apply(state: MeldReadState, update: InterimUpdate): Promise<unknown>;
}

