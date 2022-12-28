import { ExtensionSubject, OrmSubject, OrmUpdating } from '../orm/index';
import { property } from '../orm/OrmSubject';
import { JsAtomType, JsType, noMerge, Optional } from '../js-support';
import { Subject, VocabReference } from '../jrql-support';
import {
  GraphSubject, GraphSubjects, GraphUpdate, InterimUpdate, MeldConstraint, MeldReadState
} from '../api';
import { Iri } from '@m-ld/jsonld';
import { array } from '../util';
import { SubjectGraph } from '../engine/SubjectGraph';
import { SH } from '../ns';
import { SubjectPropertyValues } from '../subjects';

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
    if (SH.path in src)
      return new PropertyShape(src);
    else
      return ExtensionSubject.instance(src, orm);
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

/** Property cardinality specification */
export type PropertyCardinality = {
  count: number;
} | {
  minCount?: number;
  maxCount?: number;
}

/** Convenience specification for a property shape */
export type PropertyShapeSpec = {
  shapeId?: Iri,
  path: Iri;
  targetClass?: Iri | Iri[];
  name?: string | string[];
} & PropertyCardinality;

/**
 * @see https://www.w3.org/TR/shacl/#property-shapes
 * @category Experimental
 * @experimental
 */
export class PropertyShape extends Shape {
  /** @internal */
  @property(new JsAtomType(VocabReference, noMerge), SH.path)
  path: Iri; // | List etc.
  /** @internal */
  @property(JsType.for(Array, String), SH.name)
  name: string[];
  /** @internal */
  @property(JsType.for(Optional, Number), SH.minCount)
  minCount?: number;
  /** @internal */
  @property(JsType.for(Optional, Number), SH.maxCount)
  maxCount?: number;

  /**
   * Shape declaration. Insert into the domain data to install the shape. For
   * example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(PropertyShape.declare({ path: 'name', count: 1 }));
   * ```
   * @param spec shape specification object
   */
  static declare = (spec: Iri | PropertyShapeSpec): Subject => {
    if (typeof spec == 'object') {
      const { minCount, maxCount } = 'count' in spec ? {
        minCount: spec.count, maxCount: spec.count
      } : spec;
      return {
        '@id': spec.shapeId,
        [SH.path]: { '@vocab': spec.path },
        [SH.targetClass]: array(spec.targetClass).map(iri => ({ '@vocab': iri })),
        [SH.name]: spec.name,
        [SH.minCount]: minCount,
        [SH.maxCount]: maxCount
      };
    } else {
      return {
        [SH.path]: { '@vocab': spec }
      };
    }
  };

  constructor(
    src: GraphSubject,
    init?: Partial<PropertyShape>
  ) {
    super(src);
    this.initSrcProperties(src, {
      path: {
        get: () => ({ '@vocab': this.path }),
        set: (v: VocabReference) => this.path = v['@vocab'],
        init: init?.path ? { '@vocab': init.path } : undefined
      },
      name: { init: init?.name },
      targetClass: { init: init?.targetClass },
      minCount: { init: init?.minCount },
      maxCount: { init: init?.maxCount }
    });
  }

  /**
   * Updated subjects for a property shape will only contain the single property
   * which matches this property shape's path.
   *
   * @inheritDoc
   * @todo inverse properties: which subject is returned?
   * @todo respect targetClass
   */
  async affected(state: MeldReadState, update: GraphUpdate): Promise<GraphUpdate> {
    return {
      '@delete': this.filterGraph(update['@delete']),
      '@insert': this.filterGraph(update['@insert'])
    };
  }

  async check(state: MeldReadState, interim: InterimUpdate) {
    const update = await interim.update;
    // Fail fast if there are too many inserts for maxCount
    for (let spv of this.genSubjectValues(update['@insert']))
      this.checkMaxCount(spv);
    // Load existing values and apply the update
    // Note that deletes may not match so minCount cannot be shortcut
    const updated: { [id: Iri]: SubjectPropertyValues<GraphSubject> } = {};
    for (let { subject: { '@id': id }, values } of this.genSubjectValues(update['@delete']))
      updated[id] = (await this.loadSubjectValues(state, id)).delete(...values);
    for (let { subject: { '@id': id }, values } of this.genSubjectValues(update['@insert']))
      updated[id] = (updated[id] ?? await this.loadSubjectValues(state, id)).insert(...values);
    for (let spv of Object.values(updated)) {
      this.checkMinCount(spv);
      this.checkMaxCount(spv);
      // TODO: Delete hidden values
    }
  }

  private checkMinCount(spv: SubjectPropertyValues<GraphSubject>) {
    if (this.minCount != null && spv.values.length < this.minCount)
      throw this.failure(spv, 'minCount');
  }

  private checkMaxCount(spv: SubjectPropertyValues<GraphSubject>) {
    if (this.maxCount != null && spv.values.length > this.maxCount)
      throw this.failure(spv, 'maxCount');
  }

  private async loadSubjectValues(state: MeldReadState, id: Iri) {
    const existing = (await state.get(id, this.path)) ?? { '@id': id };
    return new SubjectPropertyValues(existing, this.path);
  }

  apply(state: MeldReadState, update: InterimUpdate): Promise<unknown> {
    throw new Error('Method not implemented.');
  }

  private filterGraph(graph: GraphSubjects) {
    return new SubjectGraph(this.genMinimalSubjects(graph));
  }

  private *genSubjectValues(subjects: SubjectGraph) {
    for (let subject of subjects) {
      if (this.path in subject)
        yield new SubjectPropertyValues(subject, this.path);
    }
  }

  private *genMinimalSubjects(subjects: SubjectGraph): Generator<GraphSubject> {
    for (let spv of this.genSubjectValues(subjects))
      yield { '@id': spv.subject['@id'], [this.path]: spv.values };
  }

  private failure(
    spv: SubjectPropertyValues<GraphSubject>,
    constraint: 'minCount' | 'maxCount'
  ) {
    const mode = {
      minCount: 'Too few',
      maxCount: 'Too many'
    }[constraint];
    return `${mode} values for ${spv.subject['@id']}: ${this.path}
    ${spv.values}`;
  }

  toString(): string {
    return this.name.length ? this.name.toString() : this.path;
  }
}