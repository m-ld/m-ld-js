import { Iri } from '@m-ld/jsonld';
import {
  isReference,
  Reference,
  Subject,
  SubjectProperty,
  Value,
  ValueObject
} from '../jrql-support';
import { Quad_Subject, Term, Triple } from './quads';
import { JRQL, RDF, XS } from '../ns';
import { GraphSubject, GraphSubjects, isSharedDatatype } from '../api';
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
  serial?: true
}

export class SubjectGraph extends Array<GraphSubject> implements GraphSubjects {
  /** Lazy instantiation of graph */
  _graph?: ReadonlyMap<Iri, GraphSubject>;

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
    return new SubjectGraph(Object.values(
      triples.reduce<{ [id: string]: GraphSubject }>((byId, triple, i) => {
        const subjectId = identifySubject(triple.subject, opts);
        const subject = byId[subjectId] ??= { '@id': subjectId };
        const property = identifyProperty(triple, opts);
        const value = opts.values?.(i, triple) ??
          jrqlValue(property, triple.object, opts.ctx, opts.serial);
        addPropertyObject(subject, property, value);
        return byId;
      }, {})));
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

function identifySubject(
  subject: Quad_Subject,
  { aliases, ctx }: RdfOptions
): Iri {
  if (subject.termType === 'BlankNode') {
    return subject.value;
  } else if (subject.termType === 'NamedNode') {
    const maybeIri = aliases?.(subject.value, '@id') ?? subject.value;
    if (!isArray(maybeIri))
      return ctx ? ctx.compactIri(maybeIri) : maybeIri;
  }
  throw new SyntaxError('Subject @id alias must be an IRI or blank');
}

function identifyProperty(
  triple: Triple,
  { aliases, ctx }: RdfOptions
): SubjectProperty {
  if (triple.predicate.termType !== 'Variable') {
    const property = aliases?.(triple.subject.value, triple.predicate.value) ??
      aliases?.(null, triple.predicate.value) ?? triple.predicate.value;
    return isArray(property) ? property :
      jrqlProperty(triple.predicate.value, triple.object, ctx);
  }
  throw new SyntaxError('Subject property must be an IRI');
}

export function jrqlValue(
  property: SubjectProperty,
  object: Term,
  ctx = JsonldContext.NONE,
  serial?: true
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
    const type = object.datatype == null ?
      getContextType(property, ctx) : object.datatype.value;
    if (type == null) {
      return object.value;
    } else if (object.typed == null) {
      if (type === XS.string)
        return object.value;
      else if (type === XS.boolean)
        return object.value === 'true';
      else if (type === XS.integer)
        return parseInt(object.value, 10);
      else if (type === XS.double)
        return parseFloat(object.value);
    }
    // If the literal has attached data, use that instead of the value
    const jrqlType = type === RDF.JSON ? '@json' :
      ctx.compactIri(type, { vocab: true });
    let valueObject: ValueObject;
    if (object.typed == null) {
      valueObject = { '@value': object.value, '@type': jrqlType };
    } else {
      const { data, type: datatype } = object.typed;
      const isShared = isSharedDatatype(datatype);
      valueObject = {
        '@value': serial ?
          datatype.toJSON?.(data) ?? data :
          isShared ? datatype.toValue?.(data) ?? data : data,
        '@type': jrqlType
      };
      if (serial && isShared)
        valueObject['@id'] = object.value;
    }
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

