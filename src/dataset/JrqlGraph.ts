import { Iri } from 'jsonld/jsonld-spec';
import {
  Context, Read, Subject, GroupLike, Update,
  isDescribe, isGroup, isSubject, isUpdate, asGroup, Group, isSelect, isGroupLike, Result, Select
} from '../m-ld/jsonrql';
import { NamedNode, Quad, Term, Variable, Quad_Subject, Quad_Predicate, Quad_Object } from 'rdf-js';
import { compact, fromRDF, toRDF } from 'jsonld';
import { namedNode, defaultGraph, variable, quad as createQuad, blankNode } from '@rdfjs/data-model';
import { Graph, PatchQuads } from '.';
import { toArray, flatMap, defaultIfEmpty } from 'rxjs/operators';
import { from, of, EMPTY } from 'rxjs';
import { toArray as array, shortId, flatten } from '../util';
import { QuadSolution } from './QuadSolution';

/**
 * A graph wrapper that provides low-level json-rql handling for queries.
 * The write methods don't actually make changes but produce Patches which
 * can then be applied to a Dataset.
 */
export class JrqlGraph {
  constructor(
    readonly graph: Graph,
    readonly defaultContext: Context = {}) {
  }

  // TODO: Make this return an Observable<Subject>
  async read(query: Read, context: Context = query['@context'] || this.defaultContext): Promise<Subject[]> {
    if (!query['@where'] && isDescribe(query)) {
      const subject = await this.describe(query['@describe'], context);
      return subject ? [subject] : [];
    } else if (isSelect(query) && query['@where'] && isGroupLike(query['@where'])) {
      const solutions = await this.matchSolutions(await this.quads(query['@where'], context));
      return from(solutions).pipe(flatMap(async solution =>
        solutionSubject(query['@select'], solution, context)), toArray()).toPromise();
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
    const quads = await this.graph.match(await resolve(describe, context)).pipe(toArray()).toPromise();
    if (quads.length) {
      quads.forEach(quad => quad.graph = defaultGraph());
      return toSubject(quads, context);
    }
  }

  async find(jrqlPattern: Subject | Group,
    context: Context = jrqlPattern['@context'] || this.defaultContext): Promise<Set<Iri>> {
    const quads = await this.findQuads(jrqlPattern, context);
    return new Set(quads.map(quad => quad.subject.value));
  }

  async findQuads(jrqlPattern: Subject | Group,
    context: Context = jrqlPattern['@context'] || this.defaultContext): Promise<Quad[]> {
    return this.matchQuads(await this.quads(jrqlPattern, context));
  }

  async insert(insert: GroupLike, context: Context = this.defaultContext): Promise<PatchQuads> {
    return new PatchQuads([], await this.quads(insert, context));
  }

  async delete(dels: GroupLike, context: Context = this.defaultContext): Promise<PatchQuads> {
    const patterns = await this.quads(dels, context);
    // If there are no variables in the delete, we don't need to find solutions
    return new PatchQuads(patterns.every(pattern => asMatchTerms(pattern).every(p => p)) ?
      patterns : await this.matchQuads(patterns), []);
  }

  async quads(g: GroupLike, context: Context = this.defaultContext): Promise<Quad[]> {
    const jsonld = asGroup(g, context);
    hideVars(jsonld['@graph']);
    const quads = await toRDF(this.graph.name.termType !== 'DefaultGraph' ?
      { ...jsonld, '@id': this.graph.name.value } : jsonld) as Quad[];
    unhideVars(quads);
    return quads;
  }

  private async matchQuads(patterns: Quad[]): Promise<Quad[]> {
    const solutions = await this.matchSolutions(patterns);
    return flatten(solutions.map(solution => solution.quads));
  }

  private async matchSolutions(patterns: Quad[]): Promise<QuadSolution[]> {
    // reduce async from a single empty solution
    return patterns.reduce(async (willSolve, pattern) => {
      const solutions = await willSolve;
      // find matching quads for each pattern quad
      return this.graph.match(...asMatchTerms(pattern)).pipe(
        defaultIfEmpty(), // TODO: Produces null if no quads, incorrect according to BGP
        // match each quad against already-found solutions
        flatMap(quad => from(solutions).pipe(flatMap(solution => {
          const matchingSolution = quad ? solution.join(pattern, quad) : solution;
          return matchingSolution ? of(matchingSolution) : EMPTY;
        }))), toArray()).toPromise();
    }, Promise.resolve([QuadSolution.EMPTY]));
  }
}

async function solutionSubject(results: Result[] | Result, solution: QuadSolution, context: Context) {
  const solutionId = blankNode();
  // Construct quads that represent the solution's variable values
  const subject = await toSubject(Object.entries(solution.vars).map(([name, term]) =>
    createQuad(solutionId, namedNode(hiddenVar(name)), term)), context);
  // Unhide the variables and strip out anything that's not selected
  return Object.assign({}, ...Object.entries(subject).map(([key, value]) => {
    const varName = matchHiddenVar(key), newKey = varName ? '?' + varName : key;
    if (isSelected(results, newKey))
      return { [newKey]: value }
  }));
}

async function toSubject(quads: Quad[], context: Context): Promise<Subject> {
  return await compact(await fromRDF(quads), context || {});
}

export async function resolve(iri: Iri, context?: Context): Promise<NamedNode> {
  return namedNode(context ? (await compact({
    '@id': iri,
    'http://json-rql.org/predicate': 1,
    '@context': context
  }, {}) as any)['@id'] : iri);
}

function asMatchTerms(quad: Quad):
  [Quad_Subject | undefined, Quad_Predicate | undefined, Quad_Object | undefined] {
  return [asTermMatch(quad.subject), asTermMatch(quad.predicate), asTermMatch(quad.object)];
}

function asTermMatch<T extends Term>(term: T): T | undefined {
  if (term.termType !== 'Variable')
    return term;
}

function hideVars(subjects: Subject | Subject[], top: boolean = true) {
  array(subjects).forEach(subject => {
    // Process predicates and objects
    Object.entries(subject).forEach(([key, value]) => {
      const varKey = hideVar(key);
      if (typeof value === 'object') // TODO: JSON-LD value object (with @value)
        hideVars(Array.isArray(value) ? value as Subject[] : value as Subject, false);
      else if (typeof value === 'string')
        value = hideVar(value);
      subject[varKey] = value;
      if (varKey !== key)
        delete subject[key];
    });
    // Identity-only subjects at top level => implicit wildcard p-o
    if (top && Object.keys(subject).every(k => k === '@id'))
      subject[genVar()] = { '@id': genVar() };
    // Anonymous subjects => wildcard subject
    if (!subject['@id'])
      subject['@id'] = genVar();
  });
}

function hideVar(token: string): string {
  const name = matchVar(token);
  // Allow anonymous variables as '?'
  return name === '' ? genVar() : name ? hiddenVar(name) : token;
}

function matchVar(token: string): string | undefined {
  const match = /^\?([\d\w]*)$/g.exec(token);
  if (match)
    return match[1];
}

function unhideVars(quads: Quad[]) {
  quads.forEach(quad => {
    quad.subject = unhideVar(quad.subject);
    quad.predicate = unhideVar(quad.predicate);
    quad.object = unhideVar(quad.object);
  });
}

function unhideVar<T extends Term>(term: T): T | Variable {
  switch (term.termType) {
    case 'NamedNode':
      const varName = matchHiddenVar(term.value);
      if (varName)
        return variable(varName);
  }
  return term;
}

function matchHiddenVar(value: string): string | undefined {
  const match = /^http:\/\/json-rql.org\/var#([\d\w]+)$/g.exec(value);
  if (match)
    return match[1];
}

function genVar() {
  return hiddenVar(shortId(4));
}

function hiddenVar(name: string) {
  return 'http://json-rql.org/var#' + name;
}

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (Array.isArray(results) ? results.includes(key) : results === key);
}