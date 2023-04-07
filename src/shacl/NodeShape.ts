import { array, uuid } from '../util';
import { GraphSubject, GraphSubjects, GraphUpdate, InterimUpdate, MeldReadState } from '../api';
import { Shape, ShapeSpec, ValidationResult } from './Shape';
import { inflate } from '../engine/util';
import { firstValueFrom } from 'rxjs';
import { filter, toArray } from 'rxjs/operators';
import { SubjectGraph } from '../engine/SubjectGraph';

/**
 * @see https://www.w3.org/TR/shacl/#node-shapes
 * @category Experimental
 * @experimental
 */
export class NodeShape extends Shape {
  /**
   * Shape declaration. Insert into the domain data to install the shape. For
   * example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(NodeShape.declare({ path: 'name', count: 1 }));
   * ```
   * @param spec shape specification object
   */
  static declare = ShapeSpec.declareShape;

  constructor(spec?: ShapeSpec) {
    const src = spec?.src ?? { '@id': uuid() };
    super(src);
    this.initSrcProperties(src, {
      targetClass: this.targetClassAccess(spec)
    });
  }

  async affected(state: MeldReadState, update: GraphUpdate): Promise<GraphUpdate> {
    const loaded: { [id: string]: GraphSubject | undefined } = {};
    async function loadType(subject: GraphSubject) {
      return subject['@id'] in loaded ? loaded[subject['@id']] :
        loaded[subject['@id']] = await state.get(subject['@id'], '@type');
    }
    // Find all updated subjects that have the target classes
    const filterUpdate = async (updateElement: GraphSubjects) =>
      new SubjectGraph(await firstValueFrom(inflate(
        updateElement, async subject =>
          this.hasTargetClass(subject) ||
          this.hasTargetClass(await loadType(subject)) ? subject : null
      ).pipe(filter((s): s is GraphSubject => s != null), toArray())));
    return {
      '@delete': await filterUpdate(update['@delete']),
      '@insert': await filterUpdate(update['@insert'])
    };
  }

  private hasTargetClass(subject: GraphSubject | undefined) {
    return subject && array(subject['@type']).some(type => this.targetClass.has(type));
  }

  async check(state: MeldReadState, interim: InterimUpdate): Promise<ValidationResult[]> {
    return []; // TODO Constraint checking applies nested property shapes
  }

  async apply(state: MeldReadState, interim: InterimUpdate): Promise<ValidationResult[]> {
    return []; // TODO Constraint checking applies nested property shapes
  }

  toString(): string {
    return `[Node Shape] target=${this.targetClass.toString()}`;
  }
}
