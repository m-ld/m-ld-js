import { Iri } from 'jsonld/jsonld-spec';
import {
  Context, Read, Subject, Update, isDescribe, isGroup, isSubject, isUpdate,
  Group, isSelect, Result, Variable, Write
} from '../../jrql-support';
import { DataFactory, NamedNode, Quad, Term } from 'rdf-js';
import { Graph, PatchQuads } from '.';
import { toArray, mergeMap, filter, take, groupBy, catchError } from 'rxjs/operators';
import { EMPTY, from, Observable, throwError } from 'rxjs';
import { array } from '../../util';
import { canPosition, inPosition, TriplePos } from '../quads';
import { activeCtx, expandTerm } from "../jsonld";
import { Binding } from 'quadstore';
import { Algebra, Factory as SparqlFactory } from 'sparqlalgebrajs';
import { genVarName, jrql, JrqlQuads, matchSubVarName, matchVar } from './JrqlQuads';

/**
 * A graph wrapper that provides low-level json-rql handling for queries. The
 * write methods don't actually make changes but produce Patches which can then
 * be applied to a Dataset.
 */
export class JrqlGraph {
  sparql: SparqlFactory;
  jrql: JrqlQuads;

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
    this.sparql = new SparqlFactory(graph.dataFactory);
    this.jrql = new JrqlQuads(this.rdf, this.graph.name, base);
  }

  /** Convenience for RDF algebra construction */
  get rdf(): Required<DataFactory> {
    return this.graph.dataFactory;
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
      mergeMap(solution => this.jrql.solutionSubject(select, solution, context)));
  }

  describe(describe: Iri | Variable,
    where?: Subject | Subject[] | Group,
    context: Context = this.defaultContext): Observable<Subject> {
    const describedVarName = matchVar(describe);
    if (describedVarName) {
      const vars = {
        subject: this.any(), property: this.any(),
        value: this.any(), item: this.any(),
        described: this.rdf.variable(describedVarName)
      };
      return this.solutions(asGroup(where ?? {}), op =>
        this.graph.query(this.sparql.createProject(
          this.sparql.createJoin(
            // Sub-select DISTINCT to fetch subject ids
            this.sparql.createDistinct(
              this.sparql.createProject(
                this.sparql.createExtend(op, vars.subject,
                  this.sparql.createTermExpression(vars.described)),
                [vars.subject])),
            this.gatherSubjectData(vars)),
          [vars.subject, vars.property, vars.value, vars.item])), context).pipe(
            // TODO: Comunica bug? Cannot read property 'close' of undefined, if stream empty
            catchError(err => err instanceof TypeError ? EMPTY : throwError(err)),
            // TODO: Ordering annoyance: sometimes subjects interleave, so
            // cannot use toArrays(quad => quad.subject.value),
            groupBy(binding => this.bound(binding, vars.subject)),
            mergeMap(subjectBindings => subjectBindings.pipe(toArray())),
            mergeMap(subjectBindings => this.toSubject(subjectBindings, vars, context)));
    } else {
      return from(this.describe1(describe, context)).pipe(
        filter<Subject>(subject => subject != null));
    }
  }

  async describe1<T extends object>(describe: Iri,
    context: Context = this.defaultContext): Promise<T | undefined> {
    const subject = await this.resolve(describe, context);
    const vars = { subject, property: this.any(), value: this.any(), item: this.any() };
    const bindings = await this.graph.query(this.sparql.createProject(
      this.gatherSubjectData(vars), [vars.property, vars.value, vars.item]))
      .pipe(toArray()).toPromise();
    return bindings.length ? <T>await this.toSubject(bindings, vars, context) : undefined;
  }

  private toSubject(bindings: Binding[], terms: SubjectTerms, context: Context): Promise<Subject> {
    // Partition the bindings into plain properties and list items
    return this.jrql.toSubject(...bindings.reduce<[Quad[], Quad[]]>((quads, binding) => {
      const [propertyQuads, listItemQuads] = quads, item = this.bound(binding, terms.item);
      (item == null ? propertyQuads : listItemQuads).push(this.rdf.quad(
        inPosition('subject', this.bound(binding, terms.subject)),
        inPosition('predicate', this.bound(binding, terms.property)),
        inPosition('object', this.bound(binding, item == null ? terms.value : item)),
        this.graph.name))
      return quads;
    }, [[], []]), context);
  }

  private gatherSubjectData({ subject, property, value, item }: SubjectTerms): Algebra.Operation {
    /* {
      ?subject ?prop ?value
      optional { filter(isiri(?value)) bind(?value AS ?slot) }
      optional { ?slot <http://json-rql.org/#item> ?item }
    } */
    const slot = this.any();
    return this.sparql.createLeftJoin(
      this.sparql.createLeftJoin(
        // BGP to pick up all subject properties
        this.sparql.createBgp([this.sparql.createPattern(
          subject, property, value, this.graph.name)]),
        // Optional bind of slot variable if value is IRI
        this.sparql.createExtend(this.sparql.createBgp([]),
          slot, this.sparql.createTermExpression(value)),
        this.sparql.createOperatorExpression('isiri',
          [this.sparql.createTermExpression(value)])),
      // Optional BGP to pick up list slot items
      this.sparql.createBgp([this.sparql.createPattern(
        slot, this.rdf.namedNode(jrql.item), item, this.graph.name)]));
  }

  async find1<T>(jrqlPattern: Partial<T> & Subject,
    context: Context = jrqlPattern['@context'] ?? this.defaultContext): Promise<Iri | ''> {
    const quad = await this.findQuads(jrqlPattern, context).pipe(take(1)).toPromise();
    return quad?.subject.value ?? '';
  }

  findQuads(jrqlPattern: Subject, context: Context = this.defaultContext): Observable<Quad> {
    return from(this.jrql.quads(jrqlPattern, { query: true }, context)).pipe(
      mergeMap(quads => this.matchQuads(quads)));
  }

  async update(query: Update,
    context: Context = query['@context'] || this.defaultContext): Promise<PatchQuads> {
    let patch = new PatchQuads();
    // If there is a @where clause, use variable substitutions per solution.
    if (query['@where'] != null) {
      const deleteTemplate = query['@delete'] != null ?
        await this.jrql.quads(query['@delete'], { query: true }, context) : null;
      const insertTemplate = query['@insert'] != null ?
        await this.jrql.quads(query['@insert'], { query: false }, context) : null;
      await this.solutions(asGroup(query['@where']), this.project, context).forEach(solution => {
        const matchingQuads = (template: Quad[] | null) => {
          // If there are variables in the update for which there is no value in the
          // solution, or if the solution value is not compatible with the quad
          // position, then this is treated as no-match, even if this is a
          // `@delete` (i.e. DELETEWHERE does not apply).
          return template != null ? this.fillTemplate(template, solution)
            .filter(quad => !anyVarTerm(quad)) : [];
        }
        patch.append(new PatchQuads({
          oldQuads: matchingQuads(deleteTemplate),
          newQuads: matchingQuads(insertTemplate)
        }));
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
    return new PatchQuads({ newQuads: await this.definiteQuads(insert, context) });
  }

  async definiteQuads(pattern: Subject | Subject[],
    context: Context = this.defaultContext) {
    const vars = new Set<string>();
    const quads = await this.jrql.quads(pattern, { query: false, vars }, context);
    if (vars.size > 0)
      throw new Error('Pattern has variable content');
    return quads;
  }

  /**
   * This is shorthand for a `@delete` update with no `@where`. It supports
   * SPARQL DELETEWHERE semantics, matching any variables against the data.
   */
  async delete(dels: Subject | Subject[],
    context: Context = this.defaultContext): Promise<PatchQuads> {
    const vars = new Set<string>();
    const patterns = await this.jrql.quads(dels, { query: true, vars }, context);
    // If there are no variables in the delete, we don't need to find solutions
    return new PatchQuads({
      oldQuads: vars.size > 0 ?
        await this.matchQuads(patterns).pipe(toArray()).toPromise() : patterns, newQuads: []
    });
  }

  private toPattern = (quad: Quad): Algebra.Pattern => {
    return this.sparql.createPattern(
      quad.subject, quad.predicate, quad.object, quad.graph);
  }

  private matchQuads(quads: Quad[]): Observable<Quad> {
    const patterns = quads.map(this.toPattern);
    // CONSTRUCT <quads> WHERE <quads>
    return this.graph.query(this.sparql.createConstruct(
      this.sparql.createBgp(patterns), patterns));
  }

  private solutions<T>(where: Group,
    exec: (op: Algebra.Operation, vars: Iterable<string>) => Observable<T>,
    context: Context): Observable<T> {
    const vars = new Set<string>();
    return from(unions(where).reduce<Promise<Algebra.Operation | null>>(async (opSoFar, graph) => {
      const left = await opSoFar;
      const quads = await this.jrql.quads(graph, { query: true, vars }, context);
      const right = this.sparql.createBgp(quads.map(this.toPattern));
      return left != null ? this.sparql.createUnion(left, right) : right;
    }, Promise.resolve(null))).pipe(mergeMap(op => op == null ? EMPTY : exec(op, vars)));
  }

  private project = (op: Algebra.Operation, vars: Iterable<string>): Observable<Binding> => {
    return this.graph.query(this.sparql.createProject(op,
      [...vars].map(varName => this.rdf.variable(varName))));
  }

  private fillTemplate(quads: Quad[], binding: Binding): Quad[] {
    return quads.map(quad => this.rdf.quad(
      this.fillTemplatePos('subject', quad.subject, binding),
      this.fillTemplatePos('predicate', quad.predicate, binding),
      this.fillTemplatePos('object', quad.object, binding),
      quad.graph));
  }

  private fillTemplatePos<P extends TriplePos>(pos: P, term: Quad[P], binding: Binding): Quad[P] {
    switch (term.termType) {
      case 'Variable':
        const value = this.bound(binding, term);
        if (value != null && canPosition(pos, value))
          return value;
    }
    return term;
  }

  private async resolve(iri: Iri, context?: Context): Promise<NamedNode> {
    return this.rdf.namedNode(context ? expandTerm(iri, await activeCtx(context)) : iri);
  }

  private bound(binding: Binding, term: Term): Term | undefined {
    switch (term.termType) {
      case 'Variable':
        const value = binding[`?${term.value}`];
        if (value != null)
          return value;

        // If this variable is a sub-variable, see if the parent variable is bound
        const [varName, subVarName] = matchSubVarName(term.value);
        const genValue = subVarName != null ?
          this.jrql.genSubValue(binding[`?${varName}`], subVarName) : null;
        if (genValue != null)
          // Cache the generated value in the binding
          return binding[`?${term.value}`] = genValue;
        break; // Not bound
      
      default:
        return term;
    }
  }

  private any() {
    return this.rdf.variable(genVarName());
  }
}

interface SubjectTerms {
  subject: Term;
  property: Term;
  value: Term;
  item: Term;
}

function anyVarTerm(quad: Quad) {
  return ['subject', 'predicate', 'object']
    .some((pos: TriplePos) => quad[pos].termType === 'Variable');
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
