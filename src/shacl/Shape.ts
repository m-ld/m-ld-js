import { ExtensionSubject, OrmSubject, OrmUpdating } from '../orm/index';
import { property } from '../orm/OrmSubject';
import { JsType } from '../js-support';
import { VocabReference } from '../jrql-support';
import {
  Assertions, GraphSubject, GraphUpdate, InterimUpdate, MeldConstraint, MeldReadState
} from '../api';
import { SH } from '../ns';
import { ConstraintComponent } from '../ns/sh';

/**
 * Shapes are used to define patterns of data, which can be used to match or
 * validate state and operations.
 *
 * Declaration of a shape does not provide any runtime function by itself, but
 * they can be used by other extensions and app code.
 *
 * While this class implements `MeldConstraint`, shape checking is semantically
 * weaker than constraint {@link check checking}, as a violation
 * (non-conformance) does not produce an exception but instead resolves a
 * validation result. However, the constraint {@link apply} will attempt a
 * correction in response to a non-conformance.
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

  /**
   * @returns a SHACL validation result if the check produces a non-conformance
   * @throws only if the validation fails
   * @see {@link Shape class} comments about constraints
   * @see https://www.w3.org/TR/shacl/#validation-report
   * @override
   */
  abstract check(state: MeldReadState, update: InterimUpdate): Promise<ValidationResult[]>;

  /**
   * @returns a SHACL validation result if the apply produced a non-conformance,
   * which was corrected
   * @see {@link Shape class} comments about constraints
   * @see https://www.w3.org/TR/shacl/#validation-report
   * @override
   */
  abstract apply(state: MeldReadState, update: InterimUpdate): Promise<ValidationResult[]>;
}

/**
 * SHACL defines ValidationResult to report individual SHACL validation results.
 * @see https://www.w3.org/TR/shacl/#result
 */
export interface ValidationResult {
  /**
   * The focus node that has caused the result. This is the focus node that was
   * validated when the validation result was produced.
   *
   * Note that the subject contains only the non-conforming properties.
   *
   * @see https://www.w3.org/TR/shacl/#results-focus-node
   */
  focusNode: GraphSubject;
  /**
   * The constraint component that caused the result. For example, results
   * produced due to a violation of a constraint based on a value of sh:minCount
   * would have the source constraint component sh:MinCountConstraintComponent.
   * @see https://www.w3.org/TR/shacl/#results-source-constraint-component
   */
  sourceConstraintComponent: ConstraintComponent;
  /**
   * Communicates additional textual details to humans.
   * @see https://www.w3.org/TR/shacl/#results-message
   */
  resultMessage: string;
  /**
   * Any correction assertions that were made by the shape to make the focus
   * nodes final state conform.
   */
  correction?: Assertions;
}