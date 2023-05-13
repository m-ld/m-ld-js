import { Iri } from '@m-ld/jsonld';
import {
  isReference,
  Reference,
  Subject,
  SubjectProperty,
  Value,
  ValueObject
} from '../jrql-support';
import { Quad_Predicate, Quad_Subject, Term, Triple } from './quads';
import { JRQL, RDF, XS } from '../ns';
import { GraphSubject, GraphSubjects } from '../api';
import { deepValues, isArray, setAtPath } from './util';
import { addPropertyObject, getContextType, toIndexNumber } from './jrql-util';
import { JsonldContext } from './jsonld';

export type GraphAliases =
  (subject: Iri | null, property: '@id' | string) => Iri | SubjectProperty | undefined;
export type ValueAliases = (i: number, triple: Triple) => Value | undefined;

export interface RdfOptions {
  aliases?: GraphAliases,
  values?: ValueAliases,
  ctx?: JsonldContext
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
        const property = identifyProperty(
          triple.subject.value, triple.predicate, opts);
        const value = opts.values?.(i, triple) ?? jrqlValue(property, triple.object, opts.ctx);
        addPropertyObject(byId[subjectId] ??= { '@id': subjectId }, property, value);
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
  subjectIri: Iri,
  predicate: Quad_Predicate,
  { aliases, ctx }: RdfOptions
): SubjectProperty {
  if (predicate.termType !== 'Variable') {
    const property = aliases?.(subjectIri, predicate.value) ??
      aliases?.(null, predicate.value) ?? predicate.value;
    return isArray(property) ? property : jrqlProperty(property, ctx);
  }
  throw new SyntaxError('Subject property must be an IRI');
}

function toValueObject(value: any, type: string, ctx = JsonldContext.NONE): ValueObject {
  return { '@value': value, '@type': ctx.compactIri(type, { vocab: true }) };
}

export function jrqlValue(
  property: SubjectProperty,
  object: Term,
  ctx = JsonldContext.NONE
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
    else {
      // If the literal has attached data, use that instead of the value
      const value = object.typed != null ? object.typed.data : object.value;
      const type = object.datatype == null ?
        getContextType(property, ctx) : object.datatype.value;
      if (type == null || type === XS.string)
        return value;
      else if (type === XS.boolean)
        return value === 'true';
      else if (type === XS.integer)
        return parseInt(value, 10);
      else if (type === XS.double)
        return parseFloat(value);
      else if (type === RDF.JSON)
        return toValueObject(value, '@json', ctx);
      else
        return toValueObject(value, type, ctx);
    }
  } else {
    throw new Error(`Cannot include ${object.termType} in a Subject`);
  }
}

/** Converts RDF predicate to json-rql keyword, Iri, or list indexes */
export function jrqlProperty(predicate: Iri, ctx = JsonldContext.NONE): SubjectProperty {
  switch (predicate) {
    case RDF.type:
      return '@type';
    case JRQL.index:
      return '@index';
    case JRQL.item:
      return '@item';
  }
  const index = toIndexNumber(predicate);
  return index != null ? ['@list', ...index] :
    ctx.compactIri(predicate, { relativeTo: { vocab: true } });
}

