import { property } from '../orm/OrmSubject';
import { JsAtomType, JsType, noMerge, Optional } from '../js-support';
import { Subject, VocabReference } from '../jrql-support';
import { Iri } from '@m-ld/jsonld';
import { array, uuid } from '../util';
import {
  Assertions, GraphSubject, GraphSubjects, GraphUpdate, InterimUpdate, MeldPreUpdate, MeldReadState
} from '../api';
import { sortValues, SubjectPropertySet } from '../subjects';
import { drain } from 'rx-flowable';
import { SubjectGraph } from '../engine/SubjectGraph';
import { Shape, ShapeSpec, ValidationResult } from './Shape';
import { SH } from '../ns';
import { mapIter } from '../engine/util';
import { ConstraintComponent } from '../ns/sh';
import { MeldAppContext } from '../config';

/**
 * Property cardinality specification
 * @category Experimental
 * @experimental
 */
export type PropertyCardinality = {
  count: number;
} | {
  minCount?: number;
  maxCount?: number;
}
/**
 * Convenience specification for a {@link PropertyShape}
 * @category Experimental
 * @experimental
 */
export type PropertyShapeSpec = ShapeSpec & {
  path: Iri;
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
  static declare = (spec: PropertyShapeSpec): Subject => {
    const { minCount, maxCount } = 'count' in spec ? {
      minCount: spec.count, maxCount: spec.count
    } : spec;
    return {
      [SH.path]: { '@vocab': spec.path },
      [SH.name]: spec.name,
      [SH.minCount]: minCount,
      [SH.maxCount]: maxCount,
      ...ShapeSpec.declareShape(spec)
    };
  };

  constructor(spec?: PropertyShapeSpec) {
    const src = spec?.src ?? { '@id': uuid() };
    super(src);
    this.initSrcProperties(src, {
      path: {
        get: () => ({ '@vocab': this.path }),
        set: (v: VocabReference) => this.path = v['@vocab'],
        init: spec?.path ? { '@vocab': spec.path } : undefined
      },
      name: { init: spec?.name },
      targetClass: this.targetClassAccess(spec),
      minCount: { init: spec != null && 'count' in spec ? spec.count : spec?.minCount },
      maxCount: { init: spec != null && 'count' in spec ? spec.count : spec?.maxCount }
    });
  }

  /** @internal */
  setExtensionContext(appContext: MeldAppContext) {
    super.setExtensionContext(appContext);
    this.path = appContext.context.expandTerm(this.path, { vocab: true });
  }

  /**
   * Updated subjects for a property shape will only contain the single property
   * which matches this property shape's path.
   *
   * @inheritDoc
   * @todo inverse properties: which subject is returned?
   * @todo respect targetClass
   */
  async affected(_state: MeldReadState, update: GraphUpdate): Promise<GraphUpdate> {
    return {
      '@delete': this.filterGraph(update['@delete']),
      '@insert': this.filterGraph(update['@insert']),
      '@update': this.filterGraph(update['@update'])
    };
  }

  private static Constrain = class Constrain {
    constructor(
      readonly shape: PropertyShape,
      readonly state: MeldReadState,
      readonly interim: InterimUpdate
    ) {}

    async loadFinalValues(update: MeldPreUpdate) {
      const updated: {
        [id: Iri]: Record<'final' | 'old', SubjectPropertySet<GraphSubject>>
      } = {};
      const loadUpdated = async (id: Iri) => {
        if (id in updated)
          return updated[id];
        const old = await this.loadSubjectValues(id);
        return updated[id] = { old, final: old.clone() };
      };
      await Promise.all(mapIter(this.shape.genSubjectValues(update['@delete']), async del =>
        (await loadUpdated(del.subject['@id'])).final.delete(...del.values())));
      await Promise.all(mapIter(this.shape.genSubjectValues(update['@insert']), async ins =>
        (await loadUpdated(ins.subject['@id'])).final.insert(...ins.values())));
      return Object.values(updated);
    }

    async loadSubjectValues(id: Iri) {
      const existing = (await this.state.get(id, this.shape.path)) ?? { '@id': id };
      return new SubjectPropertySet(existing, this.shape.path);
    }

    sort(values: any[]) {
      return sortValues(this.shape.path, values).reverse(); // Descending order
    }

    nonConformance(
      spv: SubjectPropertySet<GraphSubject>,
      sourceConstraintComponent: ConstraintComponent,
      correction?: Assertions
    ): ValidationResult {
      const mode = {
        [ConstraintComponent.MinCount]: 'Too few',
        [ConstraintComponent.MaxCount]: 'Too many'
      }[sourceConstraintComponent];
      return {
        focusNode: spv.subject,
        sourceConstraintComponent,
        resultMessage: `${mode} values for ${spv}`,
        correction
      };
    }
  };

  private static Check = class Check extends PropertyShape.Constrain {
    async check(): Promise<ValidationResult[]> {
      const update = await this.interim.update;
      // Shortcut if there are too many inserts for maxCount
      const results = array([...mapIter(
        this.shape.genSubjectValues(update['@insert']), spv => this.checkMaxCount(spv))]);
      if (results.length > 0)
        return results;
      // Load existing values and apply the update
      // Note that deletes may not match so minCount cannot be shortcut
      const finalValues = await this.loadFinalValues(update);
      return array(await Promise.all(
        finalValues.map(({ final }) => this.checkValues(final))));
    }

    private async checkValues(
      spv: SubjectPropertySet<GraphSubject>
    ): Promise<ValidationResult | undefined> {
      const result = this.checkMinCount(spv) ?? this.checkMaxCount(spv);
      // Delete all hidden values for this subject & property
      const id = spv.subject['@id'];
      const hidden = await drain(this.interim.hidden(id, spv.property));
      this.interim.assert({ '@delete': { '@id': id, [spv.property]: hidden } });
      return result;
    }

    private checkMinCount(spv: SubjectPropertySet<GraphSubject>) {
      if (this.shape.minCount != null && spv.values().length < this.shape.minCount)
        return this.nonConformance(spv, ConstraintComponent.MinCount);
    }

    private checkMaxCount(spv: SubjectPropertySet<GraphSubject>) {
      if (this.shape.maxCount != null && spv.values().length > this.shape.maxCount)
        return this.nonConformance(spv, ConstraintComponent.MaxCount);
    }
  };

  private static Apply = class Apply extends PropertyShape.Constrain {
    async apply(): Promise<ValidationResult[]> {
      // Load existing values and apply the update
      const finalValues = await this.loadFinalValues(await this.interim.update);
      return array(await Promise.all(finalValues.map(async ({ old, final }) => {
        // Final values is (prior + hidden) - deleted + inserted
        const values = this.sort(final.values()), { minCount, maxCount } = this.shape;
        // If not enough, re-assert prior maximal values that were deleted
        if (minCount != null && values.length < minCount) {
          // We need the values that were actually deleted
          const reinstate = this.sort(final.deletes(old.values()))
            .slice(0, minCount - values.length);
          const correction = { '@insert': final.minimalSubject(reinstate) as GraphSubject };
          this.interim.assert(correction);
          return this.nonConformance(final, ConstraintComponent.MinCount, correction);
        } else {
          // Anything that was hidden needs to be re-instated by entailment.
          // Do this by just entailing insert of everything up to max count.
          const limit = maxCount ?? values.length;
          const remove = values.slice(limit);
          this.interim.entail({
            '@delete': final.minimalSubject(remove),
            '@insert': final.minimalSubject(values.slice(0, limit))
          });
          if (remove.length)
            return this.nonConformance(final, ConstraintComponent.MaxCount);
        }
      })));
    }

    async loadSubjectValues(id: Iri) {
      // Add any hidden values into consideration
      const [spv, hiddenValues] = await Promise.all([
        super.loadSubjectValues(id),
        drain(this.interim.hidden(id, this.shape.path))
      ]);
      return spv.insert(...hiddenValues);
    }
  };

  async check(state: MeldReadState, interim: InterimUpdate) {
    return new PropertyShape.Check(this, state, interim).check();
  }

  async apply(state: MeldReadState, interim: InterimUpdate) {
    return new PropertyShape.Apply(this, state, interim).apply();
  }

  private filterGraph(graph: GraphSubjects) {
    return new SubjectGraph(this.genMinimalSubjects(graph));
  }

  private *genSubjectValues(subjects: SubjectGraph) {
    for (let subject of subjects) {
      if (this.path in subject)
        yield new SubjectPropertySet(subject, this.path);
    }
  }

  private *genMinimalSubjects(subjects: SubjectGraph): Generator<GraphSubject> {
    for (let spv of this.genSubjectValues(subjects))
      yield <GraphSubject>spv.minimalSubject();
  }

  toString(): string {
    return this.name.length ? this.name.toString() : this.path;
  }
}
