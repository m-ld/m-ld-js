import { Iri } from 'jsonld/jsonld-spec';
import {
  Context, Reference, Subject, isReference, SubjectPropertyObject, SubjectProperty
} from '../jrql-support';
import { Triple } from './quads';
import { xs, jrql, rdf } from '../ns';
import { compact } from 'jsonld';
import { GraphSubject, GraphSubjects } from '../api';
import { mapObject, deepValues, setAtPath } from './util';
import { array } from '../util';
import { addPropertyObject, listItems, toIndexDataUrl, toIndexNumber } from './jrql-util';
import { Term } from 'rdf-js';
import { ActiveContext } from 'jsonld/lib/context';
import { compactIri } from './jsonld';
const { isArray } = Array;

export type GraphAliases =
  (subject: Iri | null, property: '@id' | string) => Iri | SubjectProperty | undefined;

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
  static fromRDF(triples: Triple[], aliases?: GraphAliases): SubjectGraph {
    return new SubjectGraph(Object.values(
      triples.reduce<{ [id: string]: GraphSubject }>((byId, triple) => {
        const { subjectId, property } = SubjectGraph.identify(
          triple.subject.value, triple.predicate.value, aliases);
        addPropertyObject(byId[subjectId] ??= { '@id': subjectId },
          property, jrqlValue(property, triple.object));
        return byId;
      }, {})));
  }

  private static identify(subjectIri: Iri, predicate: Iri, aliases?: GraphAliases) {
    const subjectId = aliases?.(subjectIri, '@id') ?? subjectIri;
    if (isArray(subjectId))
      throw new SyntaxError('Subject @id alias must be an IRI');
    const property = aliases?.(subjectIri, predicate) ??
      aliases?.(null, predicate) ?? predicate;
    return {
      subjectId, property: isArray(property) ? property : jrqlProperty(property)
    };
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

export function jrqlValue(property: SubjectProperty, object: Term, ctx?: ActiveContext) {
  if (object.termType === 'NamedNode') {
    const iri = compactIri(object.value, ctx);
    // @type is implicitly a reference
    return property === '@type' ? iri : { '@id': iri };
  } else if (object.termType === 'Literal') {
    if (object.language)
      return { '@value': object.value, '@language': object.language };
    else if (object.datatype == null || object.datatype.value === xs.string)
      return object.value;
    else if (object.datatype.value === xs.boolean)
      return object.value === 'true';
    else if (object.datatype.value === xs.integer)
      return parseInt(object.value, 10);
    else if (object.datatype.value === xs.double)
      return parseFloat(object.value);
    else
      return { '@value': object.value, '@type': object.datatype.value };
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

