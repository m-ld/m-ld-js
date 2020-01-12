import { Iri } from 'jsonld/jsonld-spec';
import {
  Context, Read, Subject, GroupLike, Update,
  isDescribe, isGroup, isSubject, isUpdate, asGroup
} from './jsonrql';
import { NamedNode, Quad } from 'rdf-js';
import { compact, fromRDF, toRDF } from 'jsonld';
import { namedNode, defaultGraph } from '@rdfjs/data-model';
import { Graph, PatchQuads } from './Dataset';
import { flatten } from './util';

/**
 * A graph wrapper that provides low-level json-rql handling for queries.
 * The write methods don't actually make changes but produce Patches which
 * can then be applied to a Dataset.
 */
export class JrqlGraph {
  constructor(
    private readonly graph: Graph,
    private readonly defaultContext: Context = {}) {
  }

  async read(query: Read, context: Context = query['@context'] || this.defaultContext): Promise<Subject[]> {
    if (!query['@where'] && isDescribe(query)) {
      const subject = await this.describe(query['@describe'], context);
      return subject ? [subject] : [];
    }
    throw new Error('Read type not supported.');
  }

  async write(query: GroupLike | Update, context?: Context): Promise<PatchQuads> {
    if (Array.isArray(query) || isGroup(query) || isSubject(query)) {
      return this.write({ '@insert': query } as Update);
    } else if (isUpdate(query) && !query['@where']) {
      return await this.update(query, context);
    }
    throw new Error('Write type not supported.');
  }

  async update(query: Update, context: Context = query['@context'] || this.defaultContext): Promise<PatchQuads> {
    let patch = new PatchQuads([], []);
    if (query['@delete'])
      patch = patch.concat(await this.delete(query['@delete'], context));
    if (query['@insert'])
      patch = patch.concat(await this.insert(query['@insert'], context));
    return patch;
  }

  async describe(describe: Iri, context: Context = this.defaultContext): Promise<Subject | undefined> {
    const quads = await this.graph.match(await resolve(describe, context));
    if (quads.length) {
      quads.forEach(quad => quad.graph = defaultGraph());
      return await compact(await fromRDF(quads), context || {});
    }
  }

  async find(subject: Subject, context: Context = subject['@context'] || this.defaultContext): Promise<Set<Iri>> {
    const isTempId = !subject['@id'];
    if (isTempId)
      subject = { '@id': 'http://json-rql.org/subject', ...subject };
    const quads = await this.quads(subject, context);
    const matches = await Promise.all(quads.map(quad => 
      this.graph.match(isTempId ? undefined : quad.subject, quad.predicate, quad.object)));
    return new Set(flatten(matches).map(quad => quad.subject.value));
  }

  async insert(insert: GroupLike, context: Context = this.defaultContext): Promise<PatchQuads> {
    return new PatchQuads([], await this.quads(insert, context));
  }

  async delete(dels: GroupLike, context: Context = this.defaultContext): Promise<PatchQuads> {
    return new PatchQuads(await this.quads(dels, context), []);
  }

  async quads(g: GroupLike, context: Context = this.defaultContext): Promise<Quad[]> {
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