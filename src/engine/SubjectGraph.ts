import { Iri } from '@m-ld/jsonld';
import {
  isReference, isValueObject, Reference, Subject, SubjectProperty, Value, ValueObject
} from '../jrql-support';
import { Quad, RdfFactory, Term, Triple } from './quads';
import { JRQL, RDF, XS } from '../ns';
import { GraphSubject, GraphSubjects } from '../api';
import { deepValues, isArray, setAtPath } from './util';
import { addPropertyObject, getContextType, toIndexNumber } from './jrql-util';
import { JsonldContext } from './jsonld';

export type GraphAliases =
  (subject: Iri | null, property: '@id' | string) => Iri | SubjectProperty | undefined;
export type ValueAliases = (i: number, triple: Triple) => Value | Value[] | undefined;

export interface RdfOptions {
  aliases?: GraphAliases,
  values?: ValueAliases,
  ctx?: JsonldContext,
  serial?: boolean,
  /** If provided, the caller is requesting resolved quads in the graph */
  rdf?: RdfFactory
}

export class SubjectGraph extends Array<GraphSubject> implements GraphSubjects {
  /** Lazy instantiation of graph */
  _graph?: ReadonlyMap<Iri, GraphSubject>;
  /** Triples, if the graph was created from RDF */
  _quads?: Quad[];

  /**
   * Re-implementation of JSON-LD fromRDF with fixed options and simplifications:
   * - No graph name handling
   * - No list conversion
   * - Direct to flattened form (not expanded)
   * - Always use native types
   * - JSON literals become strings
   * @see https://github.com/m-ld/m-ld-js/issues/3
   */
  static fromRDF(triples: Triple[], opts: RdfOptions = {}): SubjectGraph {
    const byId: Record<string, GraphSubject> = {};
    const apiQuads = [...processTriplesToSubjects(triples, byId, opts)];
    const subjectGraph = new SubjectGraph(Object.values(byId));
    if (opts.rdf)
      subjectGraph._quads = apiQuads;
    return subjectGraph;
  }

  /** numeric parameter is needed for Array constructor compliance */
  constructor(json: Iterable<GraphSubject> | 0) {
    if (typeof json == 'number')
      super(json);
    else
      super(...json);
  }

  get graph(): ReadonlyMap<Iri, GraphSubject> {
    if (this._graph == null) {
      const byId = new Map<Iri, Subject & Reference>();
      // Make a copy of each subject to reify its references
      for (let subject of this) {
        const id = subject['@id'];
        byId.set(id, { ...byId.get(id), ...subject });
      }
      // Replace json-rql References with Javascript references
      for (let subject of byId.values()) {
        for (let [path, value] of deepValues(subject, isReference)) {
          if (byId.has(value['@id']))
            setAtPath(subject, path, byId.get(value['@id']));
        }
      }
      this._graph = byId;
    }
    return this._graph;
  }

  get quads() {
    if (this._quads == null)
      throw new TypeError('Quads are not available for this graph.');
    return this._quads;
  }

  ////////////////////////////////////////////////////////////////
  // Overrides of Array mutation methods
  pop() {
    this._graph = undefined;
    return super.pop();
  }
  push(...items: GraphSubject[]) {
    this._graph = undefined;
    return super.push(...items);
  }
  shift() {
    this._graph = undefined;
    return super.shift();
  }
  splice(start: number, deleteCount?: number, ...items: GraphSubject[]) {
    this._graph = undefined;
    if (deleteCount != null)
      return super.splice(start, deleteCount, ...items);
    else
      return super.splice(start);
  }
  unshift(...items: GraphSubject[]) {
    this._graph = undefined;
    return super.unshift(...items);
  }
}

function *processTriplesToSubjects(
  triples: Triple[],
  subjects: Record<string, GraphSubject>,
  opts: RdfOptions
): Generator<Quad> {
  // FIXME: If opts.rdf, we should sort the triples so list sub-items emit in order
  for (let i = 0; i < triples.length; i++) {
    const triple: Readonly<Triple> = triples[i];
    // We output the API quad iff RDF is requested
    const apiQuad: Quad | undefined = opts.rdf ? opts.rdf.quad(
      triple.subject, triple.predicate, triple.object
    ) : undefined;

    let subjectId: Iri | undefined;
    if (triple.subject.termType === 'BlankNode') {
      subjectId = triple.subject.value;
    } else if (triple.subject.termType === 'NamedNode') {
      const maybeIri =
        opts.aliases?.(triple.subject.value, '@id') ?? triple.subject.value;
      if (!isArray(maybeIri)) {
        if (apiQuad != null && opts.rdf != null)
          apiQuad.subject = opts.rdf.namedNode(maybeIri);
        subjectId = opts.ctx ? opts.ctx.compactIri(maybeIri) : maybeIri;
      }
    }
    if (subjectId == null)
      throw new SyntaxError('Subject @id alias must be an IRI or blank');

    let property: SubjectProperty | undefined;
    if (triple.predicate.termType !== 'Variable') {
      property = opts.aliases?.(triple.subject.value, triple.predicate.value) ??
        opts.aliases?.(null, triple.predicate.value) ?? triple.predicate.value;
      if (isArray(property)) {
        if (apiQuad != null && opts.rdf != null) {
          // Construct an RDF container member
          const [, index] = property;
          apiQuad.predicate = opts.rdf.namedNode(`${RDF.$base}_${Number(index) + 1}`);
        }
      } else {
        property = jrqlProperty(triple.predicate.value, triple.object, opts.ctx);
      }
    }
    if (property == null)
      throw new SyntaxError('Subject property must be an IRI');

    const value = opts.values?.(i, triple) ??
      jrqlValue(property, triple.object, opts.ctx, opts.serial);
    addPropertyObject(subjects[subjectId] ??= { '@id': subjectId }, property, value);

    if (apiQuad != null)
      yield apiQuad;
  }
}

export function jrqlValue(
  property: SubjectProperty,
  object: Term,
  ctx = JsonldContext.NONE,
  serial?: boolean
): Value {
  if (object.termType.endsWith('Node')) {
    if (property === '@type') {
      // @type is implicitly a reference from vocabulary
      return ctx.compactIri(object.value, { vocab: true });
    } else {
      const type = getContextType(property, ctx);
      const iri = ctx.compactIri(object.value, { vocab: type === '@vocab' });
      // An IRI is always output as a Reference
      return type === '@id' || type === '@vocab' ? iri : { '@id': iri };
    }
  } else if (object.termType === 'Literal') {
    if (object.language)
      return { '@value': object.value, '@language': object.language };
    let type = object.datatype == null ?
      getContextType(property, ctx) : object.datatype.value;
    if (type == null)
      return object.value;
    let value: any = object.value, id: string | null = null;
    // If the literal has attached data, use that instead of the value
    if (object.typed != null) {
      value = object.typed.data;
      if (serial) {
        if (object.typed.type?.toJSON)
          value = object.typed.type.toJSON(value);
        id = object.value;
      } else {
        if (object.typed.type?.toValue)
          value = object.typed.type.toValue(value);
        if (isValueObject(value)) {
          type = value['@type'] ?? type;
          value = value['@value'];
        }
      }
    }
    // Always inline m-ld API compatible types
    if (id == null) {
      if (type === XS.string)
        return value;
      else if (type === XS.boolean)
        return value === true || value === 'true';
      else if (type === XS.integer)
        return parseInt(value, 10);
      else if (type === XS.double)
        return parseFloat(value);
      else if (type === XS.base64Binary)
        return Buffer.isBuffer(value) ? value :
          Buffer.from(value, 'base64');
    }
    // Construct a value object; JSON is a special case
    const valueObject: ValueObject = type === RDF.JSON ?
      { '@value': id == null ? JSON.parse(value) : value, '@type': '@json' } :
      { '@value': value, '@type': ctx.compactIri(type, { vocab: true }) };
    if (id != null)
      valueObject['@id'] = id;
    return !serial && typeof property == 'string' ?
      ctx.compactValue(property, valueObject) : valueObject;
  } else {
    throw new Error(`Cannot include ${object.termType} in a Subject`);
  }
}

/** Converts RDF predicate to json-rql keyword, Iri, or list indexes */
export function jrqlProperty(
  predicate: Iri,
  object: Triple['object'] | null,
  ctx = JsonldContext.NONE
): SubjectProperty {
  switch (predicate) {
    case RDF.type:
      return '@type';
    case JRQL.index:
      return '@index';
    case JRQL.item:
      return '@item';
  }
  const index = toIndexNumber(predicate);
  if (index != null) {
    return ['@list', ...index];
  } else {
    return ctx.compactIri(predicate, {
      vocab: true, value: object != null ? previewValue(object) : null
    });
  }
}

/**
 * This oddity helps jsonld.js determine the correct term mapping for a
 * predicate, by giving it an insight into the forthcoming value so it can match
 * the data type and language. The returned value is fully expanded.
 */
function previewValue(object: Triple['object']) {
  if (object.termType.endsWith('Node')) {
    return { '@id': object.value };
  } else if (object.termType === 'Literal') {
    const dv: ValueObject = { '@value': object.value };
    if (object.language)
      dv['@language'] = object.language;
    else
      dv['@type'] = object.datatype.value;
    return dv;
  } else {
    throw new Error(`Cannot include ${object.termType} in a Subject`);
  }
}

