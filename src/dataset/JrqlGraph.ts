import { Iri } from 'jsonld/jsonld-spec';
import {
  Context, Read, Subject, GroupLike, Update,
  isDescribe, isGroup, isSubject, isUpdate, asGroup, Group, isSelect, isGroupLike, Result, Variable
} from '../m-ld/jsonrql';
import { NamedNode, Quad, Term, Variable as VariableNode, Quad_Subject, Quad_Predicate, Quad_Object } from 'rdf-js';
import { compact, flatten as flatJsonLd, fromRDF, toRDF } from 'jsonld';
import { namedNode, defaultGraph, variable, quad as createQuad, blankNode } from '@rdfjs/data-model';
import { Graph, PatchQuads } from '.';
import { toArray, flatMap, defaultIfEmpty, map, filter, take, distinct } from 'rxjs/operators';
import { from, of, EMPTY, Observable } from 'rxjs';
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

  read(query: Read, context: Context = query['@context'] || this.defaultContext): Observable<Subject> {
    if (isDescribe(query) && (!query['@where'] || isGroupLike(query['@where']))) {
      return this.describe(query['@describe'], query['@where'], context);
    } else if (isSelect(query) && query['@where'] && isGroupLike(query['@where'])) {
      return this.select(query['@select'], query['@where'], context);
    }
    throw new Error('Read type not supported.');
  }

  async write(query: GroupLike | Update, context?: Context): Promise<PatchQuads> {
    if (Array.isArray(query) || isGroup(query) || isSubject(query)) {
      return this.write({ '@insert': query } as Update);
    } else if (isUpdate(query) && !query['@where']) {
      return this.update(query, context);
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

  select(select: Result[] | Result, where: GroupLike, context: Context = this.defaultContext): Observable<Subject> {
    return from(this.quads(where, context).then(quads => this.matchSolutions(quads))).pipe(flatMap(solutions =>
      from(solutions).pipe(flatMap(solution => solutionSubject(select, solution, context)))));
  }

  describe(describe: Iri | Variable, where?: GroupLike, context: Context = this.defaultContext): Observable<Subject> {
    const varName = matchVar(describe);
    if (varName) {
      return from(this.quads(where || {}, context).then(quads => this.matchSolutions(quads))).pipe(flatMap(solutions =>
        from(solutions).pipe(
          map(solution => solution.vars[varName]?.value),
          filter(iri => !!iri),
          distinct(),
          flatMap(iri => this.describe(iri, undefined, context)))));
    } else {
      return from(resolve(describe, context)).pipe(
        flatMap(iri => this.graph.match(iri)),
        toArray(),
        flatMap(quads => {
          quads.forEach(quad => quad.graph = defaultGraph());
          return quads.length ? from(toSubject(quads, context)) : EMPTY;
        }));
    }
  }

  async describe1(describe: Iri, context: Context = this.defaultContext): Promise<Subject | undefined> {
    return this.describe(describe, undefined, context).pipe(take(1)).toPromise();
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
    // TODO: return Observable<QuadSolution>. The last pattern results can be streamed.
    // reduce async from a single empty solution
    const solutions = await patterns.reduce(
      async (willSolve, pattern) => {
        const solutions = await willSolve;
        // find matching quads for each pattern quad
        return this.graph.match(...asMatchTerms(pattern)).pipe(
          defaultIfEmpty(), // BUG: Produces null if no quads, incorrect according to BGP
          // match each quad against already-found solutions
          flatMap(quad => from(solutions).pipe(flatMap(solution => {
            const matchingSolution = quad ? solution.join(pattern, quad) : solution;
            return matchingSolution ? of(matchingSolution) : EMPTY;
          }))), toArray()).toPromise();
      },
      // Start the reduction with an empty quad solution
      Promise.resolve([QuadSolution.EMPTY]));
    // Remove the initial empty quad solution if it's still there
    return solutions.filter(solution => solution.quads.length);
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
      return { [newKey]: value };
  }));
}

async function rdfToJson(quads: Quad[]) {
  // Using native types to avoid unexpected value objects
  return fromRDF(quads, { useNativeTypes: true });
}

/**
 * @returns a single subject compacted against the given context
 */
export async function toSubject(quads: Quad[], context: Context): Promise<Subject> {
  return compact(await rdfToJson(quads), context || {}) as unknown as Subject;
}

/**
 * @returns a flattened group compacted against the given context
 * @see https://www.w3.org/TR/json-ld11/#flattened-document-form
 */
export async function toGroup(quads: Quad[], context: Context): Promise<Group> {
  return flatJsonLd(await rdfToJson(quads), context || {}) as unknown as Group;
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

function unhideVar<T extends Term>(term: T): T | VariableNode {
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