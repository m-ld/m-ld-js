import { property } from '../orm/OrmSubject';
import { JsAtomType, JsType, noMerge, Optional } from '../js-support';
import { Subject, VocabReference } from '../jrql-support';
import { Iri } from '@m-ld/jsonld';
import { array } from '../util';
import {
  GraphSubject, GraphSubjects, GraphUpdate, InterimUpdate, MeldPreUpdate, MeldReadState
} from '../api';
import { sortValues, SubjectPropertyValues } from '../subjects';
import { drain } from 'rx-flowable';
import { SubjectGraph } from '../engine/SubjectGraph';
import { Shape } from './Shape';
import { SH } from '../ns';
import { mapIter } from '../engine/util';

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

  private static Constrain = class Constrain {
    constructor(
      readonly shape: PropertyShape,
      readonly state: MeldReadState,
      readonly interim: InterimUpdate
    ) {}

    async loadFinalValues(update: MeldPreUpdate) {
      const updated: {
        [id: Iri]: Record<'final' | 'old', SubjectPropertyValues<GraphSubject>>
      } = {};
      const loadUpdated = async (id: Iri) => {
        if (id in updated)
          return updated[id];
        const old = await this.loadSubjectValues(id);
        return updated[id] = { old, final: old.clone() };
      };
      await Promise.all(mapIter(this.shape.genSubjectValues(update['@delete']), async del => {
        const { subject: { '@id': id }, values } = del;
        (await loadUpdated(id)).final.delete(...values);
      }));
      await Promise.all(mapIter(this.shape.genSubjectValues(update['@insert']), async ins => {
        const { subject: { '@id': id }, values } = ins;
        (await loadUpdated(id)).final.insert(...values);
      }));
      return Object.values(updated);
    }

    async loadSubjectValues(id: Iri) {
      const existing = (await this.state.get(id, this.shape.path)) ?? { '@id': id };
      return new SubjectPropertyValues(existing, this.shape.path);
    }

    sort(values: any[]) {
      return sortValues(this.shape.path, values).reverse(); // Descending order
    }

    failure(
      spv: SubjectPropertyValues<GraphSubject>,
      constraint: 'minCount' | 'maxCount'
    ) {
      const mode = { minCount: 'Too few', maxCount: 'Too many' }[constraint];
      return `${mode} values for ${spv.subject['@id']} ${this.shape.path}:
      ${spv.values}`;
    }
  };

  private static Check = class Check extends PropertyShape.Constrain {
    async check() {
      const update = await this.interim.update;
      // Shortcut if there are too many inserts for maxCount
      for (let spv of this.shape.genSubjectValues(update['@insert']))
        this.checkMaxCount(spv);
      // Load existing values and apply the update
      // Note that deletes may not match so minCount cannot be shortcut
      const finalValues = await this.loadFinalValues(update);
      return Promise.all(finalValues.map(({ final }) => this.checkValues(final)));
    }

    private async checkValues(
      spv: SubjectPropertyValues<GraphSubject>
    ) {
      this.checkMinCount(spv);
      this.checkMaxCount(spv);
      // Delete all hidden values for this subject & property
      const id = spv.subject['@id'];
      const hidden = await drain(this.interim.hidden(id, spv.property));
      this.interim.assert({ '@delete': { '@id': id, [spv.property]: hidden } });
    }

    private checkMinCount(spv: SubjectPropertyValues<GraphSubject>) {
      if (this.shape.minCount != null && spv.values.length < this.shape.minCount) {
        // TODO: If count is zero, allow a fully-deleted subject
        throw this.failure(spv, 'minCount');
      }
    }

    private checkMaxCount(spv: SubjectPropertyValues<GraphSubject>) {
      if (this.shape.maxCount != null && spv.values.length > this.shape.maxCount)
        throw this.failure(spv, 'maxCount');
    }
  };

  private static Apply = class Apply extends PropertyShape.Constrain {
    async apply() {
      // Load existing values and apply the update
      const finalValues = await this.loadFinalValues(await this.interim.update);
      return Promise.all(finalValues.map(async ({ old, final }) => {
        // Final values is (prior + hidden) - deleted + inserted
        const values = this.sort(final.values), { minCount, maxCount } = this.shape;
        // If not enough, re-assert prior maximal values that were deleted
        // TODO: unless count is 0 and subject is fully deleted
        if (minCount != null && values.length < minCount) {
          // We need the values that were actually deleted
          const reinstate = this.sort(final.deletes(old.values))
            .slice(0, minCount - values.length);
          this.interim.assert({ '@insert': final.minimalSubject(reinstate) });
        }
        // Anything that was hidden needs to be re-instated by entailment.
        // Do this by just entailing insert of everything up to max count.
        const limit = maxCount ?? values.length;
        this.interim.entail({
          '@delete': final.minimalSubject(values.slice(limit)),
          '@insert': final.minimalSubject(values.slice(0, limit))
        });
      }));
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
        yield new SubjectPropertyValues(subject, this.path);
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
