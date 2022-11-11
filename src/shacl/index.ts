import { Subject, VocabReference } from '../jrql-support';
import { GraphSubject, GraphUpdate, MeldReadState } from '../api';
import { SH } from '../ns/index';
import { ExtensionSubject, OrmSubject, OrmUpdating } from '../orm';
import { Iri } from '@m-ld/jsonld';
import { SubjectGraph } from '../engine/SubjectGraph';
import { array } from '../util';
import { JsAtomType, JsType, noMerge } from '../js-support';
import { property } from '../orm/OrmSubject';

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
export abstract class Shape extends OrmSubject {
  /** @internal */
  @property(JsType.for(Set, VocabReference), SH.targetClass)
  targetClass: Set<VocabReference>;

  /** @see https://www.w3.org/TR/shacl/#terminology */
  static from(src: GraphSubject, orm: OrmUpdating): Shape | Promise<Shape> {
    if (SH.path in src)
      return new PropertyShape(src);
    else
      return ExtensionSubject.instance({ src, orm });
  }

  /**
   * Capture precisely the data being affected by the given update which matches
   * this shape, either before or after the update is applied to the state.
   *
   * @returns filtered updates where the affected subject matches this shape
   */
  abstract affected(state: MeldReadState, update: GraphUpdate): Promise<GraphUpdate>;
}

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

  static declare = (spec: Iri | {
    shapeId?: Iri,
    path: Iri;
    targetClass?: Iri | Iri[];
    name?: string | string[];
  }): Subject => typeof spec == 'object' ? {
    '@id': spec.shapeId,
    [SH.path]: { '@vocab': spec.path },
    [SH.targetClass]: array(spec.targetClass).map(iri => ({ '@vocab': iri })),
    [SH.name]: spec.name
  } : {
    [SH.path]: { '@vocab': spec }
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
      targetClass: { init: init?.targetClass }
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
      '@delete': this.filterSubjects(update['@delete']),
      '@insert': this.filterSubjects(update['@insert'])
    };
  }

  private filterSubjects(subjects: SubjectGraph) {
    return new SubjectGraph(subjects
      .filter(s => this.path in s)
      .map<GraphSubject>(s => ({
        '@id': s['@id'],
        [this.path]: s[this.path]
      })));
  }

  toString(): string {
    return this.name.length ? this.name.toString() : this.path;
  }
}