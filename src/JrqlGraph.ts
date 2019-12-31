import { Iri } from 'jsonld/jsonld-spec';
import { Context, Read, Subject, GroupLike, Update, isDescribe, isGroup, isSubject, isUpdate, asGroup } from './jsonrql';
import { NamedNode, Quad } from 'rdf-js';
import { compact, fromRDF, toRDF } from 'jsonld';
import { namedNode } from '@rdfjs/data-model';
import { Graph, Patch, PatchQuads } from './Dataset';

export class JrqlGraph {
  constructor(
    private readonly graph: Graph) {
  }

  async read(query: Read): Promise<Subject[]> {
    if (!query['@where'] && isDescribe(query)) {
      const subject = await this.describe(query['@describe'], query['@context']);
      return subject['@id'] ? [subject] : [];
    }
    throw new Error('Read type not supported.');
  }

  async write(query: GroupLike | Update): Promise<PatchQuads> {
    if (Array.isArray(query) || isGroup(query) || isSubject(query)) {
      return this.write({ '@insert': query } as Update);
    } else if (isUpdate(query) && !query['@where']) {
      let patch = new PatchQuads([], []);
      if (query['@delete'])
        patch = patch.concat(await this.delete(query['@delete'], query['@context']));
      if (query['@insert'])
        patch = patch.concat(await this.insert(query['@insert'], query['@context']));
      return patch;
    }
    throw new Error('Write type not supported.');
  }

  async describe(describe: Iri, context?: Context): Promise<Subject> {
    const quads = await this.graph.match(await resolve(describe, context));
    return await compact(await fromRDF(quads), context || {}) as Subject;
  }

  async insert(insert: GroupLike, context?: Context): Promise<PatchQuads> {
    return new PatchQuads([], await this.quads(insert, context));
  }

  async delete(dels: GroupLike, context?: Context): Promise<PatchQuads> {
    return new PatchQuads(await this.quads(dels, context), []);
  }

  async quads(g: GroupLike, context?: Context): Promise<Quad[]> {
    const jsonld = asGroup(g, context) as any;
    if (this.graph.name.termType !== 'DefaultGraph')
      jsonld['@id'] = this.graph.name.value;
    return await toRDF(jsonld) as Quad[];
  }
}

export async function resolve(iri: Iri, context?: Context): Promise<NamedNode> {
  return namedNode(context ? (await compact({
    '@id': iri,
    'http://json-rql.org/predicate': 1,
    '@context': context
  }, {}) as any)['@id'] : iri);
}