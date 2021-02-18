import { Iri } from 'jsonld/jsonld-spec';
import { Context, Reference, Subject, isReference } from '../jrql-support';
import { getValues, addValue } from './jsonld';
import { Triple } from './quads';
import { rdf, xs } from '../ns';
import { compact } from 'jsonld';
import { Subjects } from '../api';

export class SubjectGraph extends Array<Readonly<Subject & Reference>> implements Subjects {
  /** Lazy instantiation of graph */
  _graph?: ReadonlyMap<Iri, Readonly<Subject & Reference>>;

  /**
   * Re-implementation of JSON-LD fromRDF with fixed options and simplifications:
   * - No graph handling
   * - No list conversion
   * - Direct to flattened form (not expanded)
   * - Always use native types
   * - JSON literals become strings
   * @see https://github.com/m-ld/m-ld-js/issues/3
   */
  static fromRDF(triples: Triple[]): SubjectGraph {
    return new SubjectGraph(Object.values(
      triples.reduce<{ [id: string]: Subject & Reference }>((byId, triple) => {
        const s = triple.subject.value;
        const p = triple.predicate.value;
        const o = triple.object;
        const subject = byId[s] ??= { '@id': s };
        if (o.termType.endsWith('Node')) {
          if (p === rdf.type)
            addValue(subject, '@type', o.value);
          else
            addValue(subject, p, { '@id': o.value });
        } else if (o.termType === 'Literal') {
          if (o.language)
            addValue(subject, p, { '@value': o.value, '@language': o.language });
          else if (o.datatype == null || o.datatype.value === xs.string)
            addValue(subject, p, o.value);
          else if (o.datatype.value === xs.boolean)
            addValue(subject, p, o.value === 'true');
          else if (o.datatype.value === xs.integer)
            addValue(subject, p, parseInt(o.value, 10));
          else if (o.datatype.value === xs.double)
            addValue(subject, p, parseFloat(o.value));
          else
            addValue(subject, p, { '@value': o.value, '@type': o.datatype.value });
        } else {
          throw new Error(`Cannot include ${o.termType} in a Subject`);
        }
        return byId;
      }, {})));
  }

  async withContext(context: Context): Promise<SubjectGraph> {
    if (typeof context == 'object' && Object.keys(context).length !== 0) {
      // TODO: Cast required due to
      // https://github.com/DefinitelyTyped/DefinitelyTyped/issues/50909
      const graph: any = await compact(<any>this, context, { graph: true });
      return new SubjectGraph(graph['@graph']);
    } else {
      return this;
    }
  }

  /** numeric parameter is needed for Array constructor compliance */
  constructor(json: (Subject & Reference)[] | number) {
    if (typeof json == 'number')
      super(json);
    else
      super(...json);
  }

  get graph(): ReadonlyMap<Iri, Readonly<Subject & Reference>> {
    if (this._graph == null) {
      const byId = new Map<Iri, Subject & Reference>();
      for (let subject of this)
        byId.set(subject['@id'], { ...subject });

      for (let subject of byId.values()) {
        for (let key of Object.keys(subject)) {
          const values = getValues(subject, key).map(value =>
            isReference(value) && byId.has(value['@id']) ? byId.get(value['@id']) : value);
          subject[key] = values.length == 1 ? values[0] : values;
        }
      }
      this._graph = byId;
    }
    return this._graph;
  }
}