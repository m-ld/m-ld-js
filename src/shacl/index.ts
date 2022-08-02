import { Subject, VocabReference } from '../jrql-support';
import { GraphSubject, GraphUpdate, MeldReadState } from '../api';
import { SH } from '../ns/index';
import { OrmSubject } from '../orm';
import { Iri } from '@m-ld/jsonld';
import { SubjectGraph } from '../engine/SubjectGraph';
import { array } from '../util';

/**
 * @see https://www.w3.org/TR/shacl/#constraints-section
 */
export abstract class Shape extends OrmSubject {
  targetClass: Set<VocabReference>;

  /** @see https://www.w3.org/TR/shacl/#terminology */
  static from(src: GraphSubject): Shape {
    if (SH.path in src)
      return new PropertyShape(src);
    else
      throw new TypeError(`${src['@id']} is not a Shape`);
  }

  protected constructor(src: GraphSubject, targetClass?: Set<VocabReference>) {
    super(src);
    this.initSrcProperty(src, SH.targetClass, [Set, VocabReference],
      () => this.targetClass, v => this.targetClass = v, targetClass);
  }

  /**
   * @returns filtered updates where the affected subject matches this shape
   * either before or after the update is applied to the state
   */
  abstract affected(state: MeldReadState, update: GraphUpdate): Promise<GraphUpdate>;
}

/**
 * @see https://www.w3.org/TR/shacl/#property-shapes
 */
export class PropertyShape extends Shape {
  path: Iri; // | List etc.
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
    super(src, init?.targetClass);
    this.initSrcProperty(src, SH.path, VocabReference,
      () => ({ '@vocab': this.path }), v => this.path = v['@vocab'],
      init?.path ? { '@vocab': init.path } : undefined);
    this.initSrcProperty(src, SH.name, [Array, String],
      () => this.name, v => this.name = v, init?.name);
  }

  /**
   * Updated subjects for a property shape will only contain the single property
   * which matches this property shape's path.
   *
   * @inheritDoc
   * @todo inverse properties: which subject is returned?
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