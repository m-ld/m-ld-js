import { Iri } from 'jsonld/jsonld-spec';
import {
  Context, Read, Subject, Update, isDescribe, isGroup, isSubject, isUpdate,
  Group, isSelect, Result, Variable, Value, isValueObject, isReference, Write
} from '../../jrql-support';
import {
  NamedNode, Quad, Term, Quad_Subject, Quad_Predicate, Quad_Object
} from 'rdf-js';
import { compact } from 'jsonld';
import { namedNode, defaultGraph, variable, quad as createQuad, blankNode } from '@rdfjs/data-model';
import { Graph, PatchQuads } from '.';
import { toArray, mergeMap, map, filter, distinct } from 'rxjs/operators';
import { from, of, EMPTY, Observable, throwError } from 'rxjs';
import { flatten, fromArrayPromise } from '../util';
import { QuadSolution, VarValues } from './QuadSolution';
import { array, shortId } from '../../util';
import { TriplePos } from '../quads';
import { activeCtx, expandTerm, jsonToRdf, rdfToJson } from "../jsonld";

/**
 * A graph wrapper that provides low-level json-rql handling for queries. The
 * write methods don't actually make changes but produce Patches which can then
 * be applied to a Dataset.
 */
export class JrqlGraph {
  constructor(
    readonly graph: Graph,
    readonly defaultContext: Context = {}) {
  }

  read(query: Read,
    context: Context = query['@context'] || this.defaultContext): Observable<Subject> {
    if (isDescribe(query) && !Array.isArray(query['@describe'])) {
      return this.describe(query['@describe'], query['@where'], context);
    } else if (isSelect(query) && query['@where'] != null) {
      return this.select(query['@select'], query['@where'], context);
    } else {
      return throwError(new Error('Read type not supported.'));
    }
  }

  async write(query: Write,
    context: Context = query['@context'] || this.defaultContext): Promise<PatchQuads> {
    // @unions not supported unless in a where clause
    if (isGroup(query) && query['@graph'] != null && query['@union'] == null) {
      return this.write({ '@insert': query['@graph'] } as Update, context);
    } else if (isSubject(query)) {
      return this.write({ '@insert': query } as Update, context);
    } else if (isUpdate(query)) {
      return this.update(query, context);
    }
    throw new Error('Write type not supported.');
  }

  select(select: Result,
    where: Subject | Subject[] | Group,
    context: Context = this.defaultContext): Observable<Subject> {
    const solutions = this.whereSolutions(where, context);
    return fromArrayPromise(solutions).pipe(
      mergeMap(solution => solutionSubject(select, solution, context)));
  }

  private async whereSolutions(where: Subject | Subject[] | Group,
    context: Context): Promise<QuadSolution[]> {
    // Find a set of solutions for every union
    return flatten(await Promise.all(unions(where).map(
      graph => this.quads(graph, context).then(quads => this.matchSolutions(quads)))));
  }

  describe(describe: Iri | Variable,
    where?: Subject | Subject[] | Group,
    context: Context = this.defaultContext): Observable<Subject> {
    const varName = matchVar(describe);
    if (varName) {
      const solutions = this.whereSolutions(where ?? {}, context);
      // Find a set of solutions for every union
      return fromArrayPromise(solutions).pipe(
        map(solution => solution.vars[varName]?.value),
        filter(iri => !!iri), distinct(),
        // FIXME: This could obtain more recent data than the solution
        mergeMap(iri => this.describe1(iri, context)),
        filter<Subject>(subject => subject != null));
    } else {
      const subject = this.describe1(describe, context);
      return from(subject).pipe(filter<Subject>(subject => subject != null));
    }
  }

  async describe1<T extends object>(describe: Iri, context: Context = this.defaultContext): Promise<T | undefined> {
    const iri = await resolve(describe, context);
    const quads = await this.graph.match(iri).pipe(toArray()).toPromise();
    quads.forEach(quad => quad.graph = defaultGraph());
    return quads.length ? <T>await toSubject(quads, context) : undefined;
  }

  async find1<T>(jrqlPattern: Partial<T> & Subject,
    context: Context = jrqlPattern['@context'] ?? this.defaultContext): Promise<Iri | ''> {
    const quads = await this.findQuads(jrqlPattern, context);
    return quads.length ? quads.map(quad => quad.subject.value)
      .reduce((rtn, id) => rtn === id ? rtn : '') : '';
  }

  async findQuads(jrqlPattern: Subject | Subject[], context: Context = this.defaultContext): Promise<Quad[]> {
    return this.matchQuads(await this.quads(jrqlPattern, context));
  }

  async update(query: Update,
    context: Context = query['@context'] || this.defaultContext): Promise<PatchQuads> {
    let patch = new PatchQuads([], []);
    // If there is a @where clause, use variable substitutions per solution.
    if (query['@where'] != null) {
      const varDelete = query['@delete'] != null ?
        await this.hiddenVarQuads(query['@delete'], context) : null;
      const varInsert = query['@insert'] != null ?
        await this.hiddenVarQuads(query['@insert'], context) : null;
      const solutions = await this.whereSolutions(query['@where'], context);
      solutions.forEach(solution => {
        function matchingQuads(hiddenVarQuads: Quad[] | null) {
          // If there are variables in the update for which there is no value in the
          // solution, or if the solution value is not compatible with the quad
          // position, then this is treated as no-match, even if this is a
          // `@delete` (i.e. DELETEWHERE does not apply).
          return hiddenVarQuads != null ? unhideVars(hiddenVarQuads, solution.vars)
            .filter(quad => !anyVarTerm(quad)) : [];
        }
        patch.append(new PatchQuads(
          matchingQuads(varDelete), matchingQuads(varInsert)));
      });
    } else {
      if (query['@delete'])
        patch.append(await this.delete(query['@delete'], context));
      if (query['@insert'])
        patch.append(await this.insert(query['@insert'], context));
    }
    return patch;
  }

  /**
   * This is shorthand for a `@insert` update with no `@where`. It requires
   * there to be no variables in the `@insert`.
   */
  async insert(insert: Subject | Subject[],
    context: Context = this.defaultContext): Promise<PatchQuads> {
    const matches = await this.quads(insert, context);
    if (anyVarTerms(matches))
      throw new Error('Cannot insert with variable content');
    return new PatchQuads([], matches);
  }

  /**
   * This is shorthand for a `@delete` update with no `@where`. It supports
   * SPARQL DELETEWHERE semantics, matching any variables against the data.
   */
  async delete(dels: Subject | Subject[],
    context: Context = this.defaultContext): Promise<PatchQuads> {
    const patterns = await this.quads(dels, context);
    // If there are no variables in the delete, we don't need to find solutions
    return new PatchQuads(anyVarTerms(patterns) ? await this.matchQuads(patterns) : patterns, []);
  }

  async quads(g: Subject | Subject[],
    context: Context = this.defaultContext): Promise<Quad[]> {
    return unhideVars(await this.hiddenVarQuads(g, context), {});
  }

  private async hiddenVarQuads(g: Subject | Subject[], context: Context): Promise<Quad[]> {
    const jsonld = { '@graph': g, '@context': context };
    hideVars(jsonld['@graph']);
    const quads = await jsonToRdf(this.graph.name.termType !== 'DefaultGraph' ?
      { ...jsonld, '@id': this.graph.name.value } : jsonld) as Quad[];
    return quads;
  }

  private async matchQuads(patterns: Quad[]): Promise<Quad[]> {
    const solutions = await this.matchSolutions(patterns);
    return flatten(solutions.map(solution => solution.quads));
  }

  private async matchSolutions(patterns: Quad[]): Promise<QuadSolution[]> {
    // TODO: The last pattern results can be streamed.
    // reduce async from a single empty solution
    const solutions = await patterns.reduce(
      // find matching quads for each pattern quad
      async (solutions, pattern) =>
        this.graph.match(...asMatchTerms(pattern)).pipe(
          // match each quad against already-found solutions
          mergeMap(quad => fromArrayPromise(solutions).pipe(mergeMap(solution => {
            const matchingSolution = quad ? solution.join(pattern, quad) : solution;
            return matchingSolution ? of(matchingSolution) : EMPTY;
          }))), toArray()).toPromise(),
      // Start the reduction with an empty quad solution
      Promise.resolve([QuadSolution.EMPTY]));
    // Remove the initial empty quad solution if it's still there
    return solutions.filter(solution => solution.quads.length);
  }
}

function anyVarTerms(patterns: Quad[]) {
  return patterns.some(anyVarTerm);
}

function anyVarTerm(pattern: Quad): unknown {
  return asMatchTerms(pattern).some(p => p == null);
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

/**
 * @returns a single subject compacted against the given context
 */
export async function toSubject(quads: Quad[], context: Context): Promise<object> {
  return compact(await rdfToJson(quads), context || {}) as unknown as Subject;
}

async function resolve(iri: Iri, context?: Context): Promise<NamedNode> {
  return namedNode(context ? expandTerm(iri, await activeCtx(context)) : iri);
}

function asMatchTerms(quad: Quad):
  [Quad_Subject | undefined, Quad_Predicate | undefined, Quad_Object | undefined] {
  return [asTermMatch(quad.subject), asTermMatch(quad.predicate), asTermMatch(quad.object)];
}

function asTermMatch<T extends Term>(term: T): T | undefined {
  if (term.termType !== 'Variable')
    return term;
}

function hideVars(values: Value | Value[], top: boolean = true) {
  array(values).forEach(value => {
    // TODO: JSON-LD value object (with @value)
    if (typeof value === 'object' && !isValueObject(value)) {
      // If this is a Reference, we treat it as a Subject
      const subject: Subject = value as Subject;
      // Process predicates and objects
      Object.entries(subject).forEach(([key, value]) => {
        if (key !== '@context') {
          const varKey = hideVar(key);
          if (typeof value === 'object') {
            hideVars(value as Value | Value[], false);
          } else if (typeof value === 'string') {
            const varVal = hideVar(value);
            if (varVal !== value)
              value = !key.startsWith('@') ? { '@id': varVal } : varVal;
          }
          subject[varKey] = value;
          if (varKey !== key)
            delete subject[key];
        }
      });
      // References at top level => implicit wildcard p-o
      if (top && isReference(subject))
        (<any>subject)[genVar()] = { '@id': genVar() };
      // Anonymous subjects => wildcard subject
      if (!subject['@id'])
        subject['@id'] = genVar();
    }
  });
}

function unions(where: Subject | Subject[] | Group): Subject[][] {
  if (Array.isArray(where)) {
    return [where];
  } else if (isGroup(where)) {
    // A Group can technically have both a @graph and a @union
    const graph = array(where['@graph']);
    if (where['@union'] != null) {
      // Top-level graph intersects with each union
      return where['@union'].map(subject => array(subject).concat(graph))
    } else {
      return [graph];
    }
  } else {
    return [[where]];
  }
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

function unhideVars(quads: Quad[], varValues: VarValues) {
  return quads.map(quad => createQuad(
    unhideVar('subject', quad.subject, varValues),
    unhideVar('predicate', quad.predicate, varValues),
    unhideVar('object', quad.object, varValues),
    quad.graph));
}

function unhideVar<P extends TriplePos>(pos: P, term: Quad[P], varValues: VarValues): Quad[P] {
  switch (term.termType) {
    case 'NamedNode':
      const varName = matchHiddenVar(term.value);
      if (varName) {
        const value = varValues[varName];
        return value != null && isPosAssignable(pos, value) ? value : variable(varName);
      }
  }
  return term;
}

function isPosAssignable<P extends TriplePos>(
  pos: P, value: Quad_Object | Quad_Predicate | Quad_Object): value is Quad[P] {
  // Subjects and Predicate don't allow literals
  if ((pos == 'subject' || pos == 'predicate') && value.termType == 'Literal')
    return false;
  // Predicates don't allow blank nodes
  if (pos == 'predicate' && value.termType == 'BlankNode')
    return false;
  return true;
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