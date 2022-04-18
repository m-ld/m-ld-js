import { Subject, VocabReference } from '../jrql-support';
import { GraphSubject, MeldPreUpdate, MeldReadState } from '../api';
import { SH } from '../ns/index';
import { OrmSubject } from '../orm';
import { compareValues } from '../engine/jsonld';
import { Iri } from 'jsonld/jsonld-spec';

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
   * @returns an array of shapes affected by the given update. All returned
   * shapes will {@link refines refine} this Shape.
   */
  abstract affected(state: MeldReadState, update: MeldPreUpdate): Promise<Shape[]>;

  /**
   * @returns whether this shape is a strict refinement of the other Shape, that
   * is, all focus nodes of this Shape are also focus nodes of the other Shape.
   */
  abstract refines(other: Shape): boolean;
}

/**
 * @see https://www.w3.org/TR/shacl/#property-shapes
 */
export class PropertyShape extends Shape {
  path: VocabReference; // | List etc.
  name: Set<string>;

  static declare = ({ shapeId, path, targetClass, name }: {
    shapeId: Iri,
    path: VocabReference;
    targetClass?: VocabReference | VocabReference[];
    name?: string | string[];
  }): Subject => ({
    '@id': shapeId,
    [SH.path]: path,
    [SH.targetClass]: targetClass,
    [SH.name]: name
  });

  constructor(
    src: GraphSubject,
    init?: Partial<PropertyShape>
  ) {
    super(src, init?.targetClass);
    this.initSrcProperty(src, SH.path, VocabReference,
      () => this.path, v => this.path = v, init?.path);
    this.initSrcProperty(src, SH.name, [Set, String],
      () => this.name, v => this.name = v, init?.name);
  }

  isFocus(subject: Subject) {
    return subject[this.path['@vocab']] != null;
  }

  async affected(state: MeldReadState, update: MeldPreUpdate): Promise<Shape[]> {
    for (let subject of update['@delete'])
      if (this.isFocus(subject))
        return [this];
    for (let subject of update['@insert'])
      if (this.isFocus(subject))
        return [this];
    return [];
  }

  refines(other: Shape): boolean {
    return other instanceof PropertyShape && compareValues(this.path, other.path);
  }

  toString(): string {
    return this.name.size ? this.name.toString() : this.path['@vocab'];
  }
}