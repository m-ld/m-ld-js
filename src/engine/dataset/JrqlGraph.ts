import { Iri } from 'jsonld/jsonld-spec';
import {
  Context, Read, Subject, Update, isDescribe, isGroup, isSubject, isUpdate,
  Group, isSelect, Result, Variable, Value, isValueObject, isReference, Write
} from '../../jrql-support';
import { NamedNode, Quad, Term } from 'rdf-js';
import { compact } from 'jsonld';
import { Graph, PatchQuads } from '.';
import { toArray, mergeMap, filter, take, groupBy } from 'rxjs/operators';
import { EMPTY, from, Observable, throwError } from 'rxjs';
import { array, uuid } from '../../util';
import { TriplePos } from '../quads';
import { activeCtx, expandTerm, jsonToRdf, rdfToJson } from "../jsonld";
import { Binding } from 'quadstore';
import { Algebra, Factory } from 'sparqlalgebrajs';
import { any } from '../../api';

/**
 * A graph wrapper that provides low-level json-rql handling for queries. The
 * write methods don't actually make changes but produce Patches which can then
 * be applied to a Dataset.
 */
export class JrqlGraph {
  sparqlFactory: Factory;

  /**
   * @param graph a quads graph to operate on
   * @param defaultContext default context for interpreting JSON patterns
   * @param base Iri for minting new Iris. Not necessarily the same as
   * `defaultContext.@base`
   */
  constructor(
    readonly graph: Graph,
    readonly defaultContext: Context = {},
    readonly base?: Iri) {
    this.sparqlFactory = new Factory(graph.dataFactory);
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
    return this.solutions(asGroup(where), this.project, context).pipe(
      mergeMap(solution => this.solutionSubject(select, solution, context)));
  }

  describe(describe: Iri | Variable,
    where?: Subject | Subject[] | Group,
    context: Context = this.defaultContext): Observable<Subject> {
    const sVarName = matchVar(describe);
    if (sVarName) {
      return this.solutions(asGroup(where ?? {}), op =>
        this.graph.query(this.sparqlFactory.createDescribe(op,
          [this.graph.dataFactory.variable(sVarName)])), context).pipe(
            groupBy(quad => quad.subject.value),
            mergeMap(async subjectQuads => toSubject(
              // FIXME: This borks streaming. Use ordering to go subject-by-subject.
              await subjectQuads.pipe(toArray()).toPromise(), context)));
    } else {
      return from(this.describe1(describe, context)).pipe(
        filter<Subject>(subject => subject != null));
    }
  }

  async describe1<T extends object>(describe: Iri, context: Context = this.defaultContext): Promise<T | undefined> {
    const quads = await this.graph.match(await this.resolve(describe, context)).pipe(toArray()).toPromise();
    quads.forEach(quad => quad.graph = this.graph.dataFactory.defaultGraph());
    return quads.length ? <T>await toSubject(quads, context) : undefined;
  }

  async find1<T>(jrqlPattern: Partial<T> & Subject,
    context: Context = jrqlPattern['@context'] ?? this.defaultContext): Promise<Iri | ''> {
    const quad = await this.findQuads(jrqlPattern, context).pipe(take(1)).toPromise();
    return quad?.subject.value ?? '';
  }

  findQuads(jrqlPattern: Subject, context: Context = this.defaultContext): Observable<Quad> {
    return from(this.quads(jrqlPattern, { query: true }, context)).pipe(
      mergeMap(quads => this.matchQuads(quads)));
  }

  async update(query: Update,
    context: Context = query['@context'] || this.defaultContext): Promise<PatchQuads> {
    let patch = new PatchQuads([], []);
    // If there is a @where clause, use variable substitutions per solution.
    if (query['@where'] != null) {
      const varDelete = query['@delete'] != null ?
        await this.hiddenVarQuads(query['@delete'], { query: true }, context) : null;
      const varInsert = query['@insert'] != null ?
        await this.hiddenVarQuads(query['@insert'], { query: false }, context) : null;
      await this.solutions(asGroup(query['@where']), this.project, context).forEach(solution => {
        const matchingQuads = (hiddenVarQuads: Quad[] | null) => {
          // If there are variables in the update for which there is no value in the
          // solution, or if the solution value is not compatible with the quad
          // position, then this is treated as no-match, even if this is a
          // `@delete` (i.e. DELETEWHERE does not apply).
          return hiddenVarQuads != null ? this.unhideVars(hiddenVarQuads, solution)
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
    const vars = new Set<string>();
    const quads = await this.quads(insert, { query: false, vars }, context);
    if (vars.size > 0)
      throw new Error('Cannot insert with variable content');
    return new PatchQuads([], quads);
  }

  /**
   * This is shorthand for a `@delete` update with no `@where`. It supports
   * SPARQL DELETEWHERE semantics, matching any variables against the data.
   */
  async delete(dels: Subject | Subject[],
    context: Context = this.defaultContext): Promise<PatchQuads> {
    const vars = new Set<string>();
    const patterns = await this.quads(dels, { query: true, vars }, context);
    // If there are no variables in the delete, we don't need to find solutions
    return new PatchQuads(vars.size > 0 ?
      await this.matchQuads(patterns).pipe(toArray()).toPromise() : patterns, []);
  }

  async quads(g: Subject | Subject[], opts: VariablesOptions,
    context: Context = this.defaultContext): Promise<Quad[]> {
    return this.unhideVars(await this.hiddenVarQuads(g, opts, context), {});
  }

  private toPattern = (quad: Quad): Algebra.Pattern => {
    return this.sparqlFactory.createPattern(
      quad.subject, quad.predicate, quad.object, quad.graph);
  }

  private matchQuads(quads: Quad[]): Observable<Quad> {
    const patterns = quads.map(this.toPattern);
    // CONSTRUCT <quads> WHERE <quads>
    return this.graph.query(this.sparqlFactory.createConstruct(
      this.sparqlFactory.createBgp(patterns), patterns));
  }

  private solutions<T>(where: Group,
    exec: (op: Algebra.Operation, vars: Iterable<string>) => Observable<T>,
    context: Context): Observable<T> {
    const vars = new Set<string>();
    return from(unions(where).reduce<Promise<Algebra.Operation | null>>(async (opSoFar, graph) => {
      const left = await opSoFar;
      const quads = await this.quads(graph, { query: true, vars }, context);
      const right = this.sparqlFactory.createBgp(quads.map(this.toPattern));
      return left != null ? this.sparqlFactory.createUnion(left, right) : right;
    }, Promise.resolve(null))).pipe(mergeMap(op => op == null ? EMPTY : exec(op, vars)));
  }

  private project = (op: Algebra.Operation, vars: Iterable<string>): Observable<Binding> => {
    return this.graph.query(this.sparqlFactory.createProject(op,
      [...vars].map(varName => this.graph.dataFactory.variable(varName))));
  }

  private async hiddenVarQuads(graph: Subject | Subject[],
    opts: VariablesOptions, context: Context): Promise<Quad[]> {
    // TODO: hideVars should not be in-place
    const jsonld = { '@graph': JSON.parse(JSON.stringify(graph)), '@context': context };
    this.hideVars(jsonld['@graph'], opts);
    const quads = await jsonToRdf(this.graph.name.termType !== 'DefaultGraph' ?
      { ...jsonld, '@id': this.graph.name.value } : jsonld) as Quad[];
    return quads;
  }

  private hideVars(
    values: Value | Value[],
    { query, vars }: VariablesOptions,
    top: boolean = true) {
    array(values).forEach(value => {
      // JSON-LD value object (with @value) cannot contain a variable
      if (typeof value === 'object' && !isValueObject(value)) {
        // If this is a Reference, we treat it as a Subject
        const subject: Subject = value as Subject;
        // Process predicates and objects
        Object.entries(subject).forEach(([key, value]) => {
          if (key !== '@context') {
            const varKey = hideVar(key, vars);
            if (typeof value === 'object') {
              this.hideVars(value as Value | Value[], { query, vars }, false);
            } else if (typeof value === 'string') {
              const varVal = hideVar(value, vars);
              if (varVal !== value)
                value = !key.startsWith('@') ? { '@id': varVal } : varVal;
            }
            subject[varKey] = value;
            if (varKey !== key)
              delete subject[key];
          }
        });
        // References at top level => implicit wildcard p-o
        if (top && query && isReference(subject))
          (<any>subject)[genVar(vars)] = { '@id': genVar(vars) };
        // Anonymous query subjects => blank node subject (match any) or skolem
        if (!subject['@id'])
          subject['@id'] = query ? this.blankId() : skolem(this.base);
      }
    });
  }

  private blankId(): string {
    // Opinions differ whether the `value` of a blank node should have the
    // protocol prefix (jsonld does not add it). Always include, for safety.
    return `_:${this.graph.dataFactory.blankNode().value}`;
  }

  private unhideVars(quads: Quad[], varValues: Binding): Quad[] {
    return quads.map(quad => this.graph.dataFactory.quad(
      this.unhideVar('subject', quad.subject, varValues),
      this.unhideVar('predicate', quad.predicate, varValues),
      this.unhideVar('object', quad.object, varValues),
      quad.graph));
  }

  private unhideVar<P extends TriplePos>(pos: P, term: Quad[P], varValues: Binding): Quad[P] {
    switch (term.termType) {
      case 'NamedNode':
        const varName = matchHiddenVar(term.value);
        if (varName) {
          const value = varValues[`?${varName}`];
          return value != null && canPosition(pos, value) ?
            value : this.graph.dataFactory.variable(varName);
        }
    }
    return term;
  }

  private async resolve(iri: Iri, context?: Context): Promise<NamedNode> {
    return this.graph.dataFactory.namedNode(context ? expandTerm(iri, await activeCtx(context)) : iri);
  }

  private async solutionSubject(results: Result[] | Result, solution: Binding, context: Context) {
    const solutionId = this.graph.dataFactory.blankNode();
    // Construct quads that represent the solution's variable values
    const subject = await toSubject(Object.entries(solution).map(([variable, term]) =>
      this.graph.dataFactory.quad(
        solutionId,
        this.graph.dataFactory.namedNode(hiddenVar(variable.slice(1))),
        inPosition('object', term))), context);
    // Unhide the variables and strip out anything that's not selected
    return Object.assign({}, ...Object.entries(subject).map(([key, value]) => {
      if (key !== '@id') { // Strip out blank node identifier
        const varName = matchHiddenVar(key), newKey = varName ? '?' + varName : key;
        if (isSelected(results, newKey))
          return { [newKey]: value };
      }
    }));
  }
}

function anyVarTerm(quad: Quad) {
  return ['subject', 'predicate', 'object']
    .some((pos: TriplePos) => quad[pos].termType === 'Variable');
}

/**
 * @returns a single subject compacted against the given context
 */
export async function toSubject(quads: Quad[], context: Context): Promise<Subject> {
  return compact(await rdfToJson(quads), context || {}) as unknown as Subject;
}

interface VariablesOptions {
  /** Whether this will be used to match quads or insert them */
  query: boolean;
  /** The variable names found (sans '?') */
  vars?: Set<string>;
}

/**
 * Generates a new skolemization IRI for a blank node. The base is allowed to be
 * `undefined` but the function will throw a `TypeError` if it is.
 * @see https://www.w3.org/TR/rdf11-concepts/#h3_section-skolemization
 */
function skolem(base: Iri | undefined): Iri {
  return new URL(`/.well-known/genid/${uuid()}`, base).href;
}

function asGroup(where: Subject | Subject[] | Group): Group {
  return Array.isArray(where) ? { '@graph': where } :
    isGroup(where) ? where : { '@graph': where };
}

function unions(where: Group): Subject[][] {
  // A Group can technically have both a @graph and a @union
  const graph = array(where['@graph']);
  if (where['@union'] != null) {
    // Top-level graph intersects with each union
    return where['@union'].map(subject => array(subject).concat(graph))
  } else {
    return [graph];
  }
}

function hideVar(token: string, vars?: Set<string>): string {
  const name = matchVar(token);
  // Allow anonymous variables as '?'
  return name === '' ? genVar(vars) : name ? hiddenVar(name, vars) : token;
}

function matchVar(token: string): string | undefined {
  const match = /^\?([\d\w]*)$/g.exec(token);
  if (match)
    return match[1];
}

function canPosition<P extends TriplePos>(pos: P, value: Term): value is Quad[P] {
  // Subjects and Predicate don't allow literals
  if ((pos == 'subject' || pos == 'predicate') && value.termType == 'Literal')
    return false;
  // Predicates don't allow blank nodes
  if (pos == 'predicate' && value.termType == 'BlankNode')
    return false;
  return true;
}

function inPosition<P extends TriplePos>(pos: P, value: Term): Quad[P] {
  if (canPosition(pos, value))
    return value;
  else
    throw new Error(`${value} cannot be used in ${pos} position`);
}

function matchHiddenVar(value: string): string | undefined {
  const match = /^http:\/\/json-rql.org\/var#([\d\w]+)$/g.exec(value);
  if (match)
    return match[1];
}

function genVar(vars?: Set<string>) {
  return hiddenVar(any().slice(1), vars);
}

function hiddenVar(name: string, vars?: Set<string>) {
  vars && vars.add(name);
  return `http://json-rql.org/var#${name}`;
}

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (Array.isArray(results) ? results.includes(key) : results === key);
}