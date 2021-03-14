import { Iri } from 'jsonld/jsonld-spec';
import {
  Context, Reference, Subject, isReference, SubjectPropertyObject, SubjectProperty
} from '../jrql-support';
import { Triple } from './quads';
import { xs, jrql, rdf } from '../ns';
import { compact } from 'jsonld';
import { GraphSubject, GraphSubjects } from '../api';
import { mapObject, deepValues, setAtPath, isArray } from './util';
import { array } from '../util';
import { addPropertyObject, listItems, toIndexDataUrl, toIndexNumber } from './jrql-util';
import { Quad_Predicate, Quad_Subject, Term } from 'rdf-js';
import { ActiveContext, getContextValue } from 'jsonld/lib/context';
import { compactIri } from './jsonld';

export type GraphAliases =
  (subject: Iri | null, property: '@id' | string) => Iri | SubjectProperty | undefined;

interface RdfOptions {
  aliases?: GraphAliases,
  ctx?: ActiveContext
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
      triples.reduce<{ [id: string]: GraphSubject }>((byId, triple) => {
        const subjectId = SubjectGraph.identifySubject(triple.subject, opts);
        const property = SubjectGraph.identifyProperty(subjectId, triple.predicate, opts);
        addPropertyObject(byId[subjectId] ??= { '@id': subjectId },
          property, jrqlValue(property, triple.object, opts.ctx));
        return byId;
      }, {})));
  }

  private static identifySubject(
    subject: Quad_Subject, { aliases, ctx }: RdfOptions): Iri {
    if (subject.termType === 'BlankNode') {
      return subject.value;
    } else if (subject.termType === 'NamedNode') {
      const maybeIri = aliases?.(subject.value, '@id') ?? subject.value;
      if (!isArray(maybeIri))
        return compactIri(maybeIri, ctx);
    }
    throw new SyntaxError('Subject @id alias must be an IRI or blank');
  }

  private static identifyProperty(subjectId: Iri,
    predicate: Quad_Predicate, { aliases, ctx }: RdfOptions): SubjectProperty {
    if (predicate.termType !== 'Variable') {
      const property = aliases?.(subjectId, predicate.value) ??
        aliases?.(null, predicate.value) ?? predicate.value;
      return isArray(property) ? property : jrqlProperty(property, ctx);
    }
    throw new SyntaxError('Subject property must be an IRI');
  }

  async withContext(context: Context | undefined): Promise<SubjectGraph> {
    if (typeof context == 'object' && Object.keys(context).length !== 0) {
      // Hide problematic json-rql keywords prior to JSON-LD compaction
      const hidden = this.map(subject => mapObject(subject, jsonldProperties));
      const graph = await compact(hidden, context, { graph: true });
      const unhidden = array(graph['@graph']).map((jsonld: object) =>
        Object.entries(jsonld).reduce<Subject>((subject, [property, value]) =>
          addPropertyObject(subject, jrqlProperty(property), value), {}));
      return new SubjectGraph(<GraphSubject[]>unhidden);
    } else {
      return this;
    }
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
      for (let subject of this)
        byId.set(subject['@id'], { ...subject });
      // Replace json-rql References with Javascript references
      for (let subject of byId.values())
        for (let [path, value] of deepValues(subject, isReference))
          if (byId.has(value['@id']))
            setAtPath(subject, path, byId.get(value['@id']));

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

function getContextType(
  property: SubjectProperty, ctx: ActiveContext | undefined): string | null {
  return typeof property == 'string' && ctx != null ?
    getContextValue(ctx, property, '@type') : null;
}

export function jrqlValue(property: SubjectProperty, object: Term, ctx?: ActiveContext) {
  if (object.termType.endsWith('Node')) {
    if (property === '@type') {
      // @type is implicitly a reference from vocabulary
      return compactIri(object.value, ctx, { vocab: true });
    } else {
      const type = getContextType(property, ctx);
      const iri = compactIri(object.value, ctx, { vocab: type === '@vocab' });
      return type === '@id' ? iri : { '@id': iri };
    }
  } else if (object.termType === 'Literal') {
    if (object.language)
      return { '@value': object.value, '@language': object.language };
    else {
      const type = object.datatype == null ?
        getContextType(property, ctx) : object.datatype.value;
      if (type == null || type === xs.string)
        return object.value;
      else if (type === xs.boolean)
        return object.value === 'true';
      else if (type === xs.integer)
        return parseInt(object.value, 10);
      else if (type === xs.double)
        return parseFloat(object.value);
      else
        return { '@value': object.value, '@type': compactIri(type, ctx, { vocab: true }) };
    }
  } else {
    throw new Error(`Cannot include ${object.termType} in a Subject`);
  }
}

/** Hides json-rql keywords and constructs that are unknown in JSON-LD */
function jsonldProperties(property: keyof Subject, value: SubjectPropertyObject): Partial<Subject> {
  switch (property) {
    case '@index':
      return { [jrql.index]: value };
    case '@item':
      return { [jrql.item]: value };
    case '@list':
      const object: Partial<Subject> = {};
      for (let [index, item] of listItems(value))
        object[toIndexDataUrl(index)] = item;
      return object;
    default:
      return { [property]: value };
  }
}

/** Converts RDF predicate to json-rql keyword, Iri, or list indexes */
export function jrqlProperty(predicate: Iri, ctx?: ActiveContext): SubjectProperty {
  switch (predicate) {
    case rdf.type: return '@type';
    case jrql.index: return '@index';
    case jrql.item: return '@item';
  }
  const index = toIndexNumber(predicate);
  return index != null ? ['@list', ...index] :
    compactIri(predicate, ctx, { relativeTo: { vocab: true } });
}

