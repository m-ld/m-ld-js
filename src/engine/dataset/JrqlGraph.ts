import { Iri } from 'jsonld/jsonld-spec';
import {
  Constraint, Expression, Group, isConstraint, isConstruct, isDescribe, isGroup, isSelect,
  isSubject, isUpdate, operators, Read, Result, Subject, Update, Variable, VariableExpression, Write
} from '../../jrql-support';
import { Graph, PatchQuads } from '.';
import { groupBy, map, mergeMap, reduce, toArray } from 'rxjs/operators';
import { defaultIfEmpty, EMPTY, firstValueFrom, merge, Observable, of, throwError } from 'rxjs';
import {
  canPosition, inPosition, NamedNode, Quad, QueryableRdfSourceProxy, Term, TriplePos
} from '../quads';
import { ActiveContext, expandTerm, initialCtx, nextCtx } from '../jsonld';
import { Algebra, Factory as SparqlFactory } from 'sparqlalgebrajs';
import { JRQL } from '../../ns';
import { JrqlQuads } from './JrqlQuads';
import { MeldError } from '../MeldError';
import { anyName, array, GraphSubject, ReadResult, readResult } from '../..';
import { binaryFold, flatten, inflate, isArray, observeAsyncIterator } from '../util';
import { ConstructTemplate } from './ConstructTemplate';
import { Binding } from '../../rdfjs-support';

/**
 * A graph wrapper that provides low-level json-rql handling for queries. The
 * write methods don't actually make changes but produce Patches which can then
 * be applied to a Dataset.
 */
export class JrqlGraph extends QueryableRdfSourceProxy {
  readonly sparql: SparqlFactory;
  readonly jrql: JrqlQuads;

  /**
   * @param graph a quads graph to operate on
   * @param defaultCtx
   */
  constructor(
    readonly graph: Graph,
    readonly defaultCtx = initialCtx()) {
    super(graph);
    this.sparql = new SparqlFactory(graph);
    this.jrql = new JrqlQuads(graph);
  }

  read(query: Read, ctx = this.defaultCtx): ReadResult {
    return readResult(inflate(nextCtx(ctx, query['@context']), ctx => {
      if (isDescribe(query))
        return this.describe(array(query['@describe']), query['@where'], ctx);
      else if (isSelect(query) && query['@where'] != null)
        return this.select(query['@select'], query['@where'], ctx);
      else if (isConstruct(query))
        return this.construct(query['@construct'], query['@where'], ctx);
      else
        return throwError(() => new MeldError('Unsupported pattern', 'Read type not supported.'));
    }));
  }

  async write(query: Write, ctx = this.defaultCtx): Promise<PatchQuads> {
    ctx = await nextCtx(ctx, query['@context']);
    // @unions not supported unless in a where clause
    if (isGroup(query) && query['@graph'] != null && query['@union'] == null)
      return this.write({ '@insert': query['@graph'] } as Update, ctx);
    else if (isSubject(query))
      return this.write({ '@insert': query } as Update, ctx);
    else if (isUpdate(query))
      return this.update(query, ctx);
    else
      throw new MeldError('Unsupported pattern', 'Write type not supported.');
  }

  select(select: Result,
    where: Subject | Subject[] | Group,
    ctx = this.defaultCtx): Observable<GraphSubject> {
    return this.solutions(where, this.project, ctx).pipe(
      map(solution => this.jrql.solutionSubject(select, solution, ctx)));
  }

  describe(describes: (Iri | Variable)[],
    where?: Subject | Subject[] | Group,
    ctx = this.defaultCtx): Observable<GraphSubject> {
    return merge(...describes.map(describe => {
      const describedVarName = JRQL.matchVar(describe);
      if (describedVarName) {
        const vars = {
          subject: this.any(), property: this.any(),
          value: this.any(), item: this.any(),
          described: this.graph.variable(describedVarName)
        };
        return this.solutions(where ?? {}, op =>
          observeAsyncIterator(this.graph.query(this.sparql.createProject(
            this.sparql.createJoin(
              // Sub-select DISTINCT to fetch subject ids
              this.sparql.createDistinct(
                this.sparql.createProject(
                  this.sparql.createExtend(op, vars.subject,
                    this.sparql.createTermExpression(vars.described)),
                  [vars.subject])),
              this.gatherSubjectData(vars)),
            [vars.subject, vars.property, vars.value, vars.item]))), ctx).pipe(
          // TODO: Ordering annoyance: sometimes subjects interleave, so
          // cannot use toArrays(quad => quad.subject.value),
          groupBy(binding => this.bound(binding, vars.subject)),
          mergeMap(subjectBindings => subjectBindings.pipe(toArray())),
          map(subjectBindings => this.toSubject(subjectBindings, vars, ctx)));
      } else {
        return this.describe1(describe, ctx);
      }
    }));
  }

  describe1(describe: Iri, ctx = this.defaultCtx): Observable<GraphSubject> {
    const vars = {
      subject: this.resolve(describe, ctx),
      property: this.any(), value: this.any(), item: this.any()
    };
    const projection = this.sparql.createProject(
      this.gatherSubjectData(vars), [vars.property, vars.value, vars.item]);
    return observeAsyncIterator(this.graph.query(projection)).pipe(toArray(), mergeMap(bindings =>
      bindings.length ? of(this.toSubject(bindings, vars, ctx)) : EMPTY));
  }

  get(id: string) {
    return firstValueFrom(this.describe1(id).pipe(defaultIfEmpty(undefined)));
  }

  construct(construct: Subject | Subject[],
    where?: Subject | Subject[] | Group,
    ctx = this.defaultCtx): Observable<GraphSubject> {
    const template = new ConstructTemplate(construct, ctx);
    // If no where, use the construct as the pattern
    // TODO: Add ordering to allow streaming results
    return this.solutions(where ?? template.asPattern, this.project, ctx).pipe(
      reduce((template, solution) => template.addSolution(solution), template),
      mergeMap(template => template.results));
  }

  private toSubject(bindings: Binding[], terms: SubjectTerms, ctx: ActiveContext): GraphSubject {
    // Partition the bindings into plain properties and list items
    return this.jrql.toApiSubject(...bindings.reduce<[Quad[], Quad[]]>((quads, binding) => {
      const [propertyQuads, listItemQuads] = quads, item = this.bound(binding, terms.item);
      (item == null ? propertyQuads : listItemQuads).push(this.graph.quad(
        inPosition('subject', this.bound(binding, terms.subject)),
        inPosition('predicate', this.bound(binding, terms.property)),
        inPosition('object', this.bound(binding, item == null ? terms.value : item)),
        this.graph.name));
      return quads;
    }, [[], []]), ctx);
  }

  private gatherSubjectData({ subject, property, value, item }: SubjectTerms): Algebra.Operation {
    /* {
      ?subject ?prop ?value
      optional { ?value <http://json-rql.org/#item> ?item }
    } */
    return this.sparql.createLeftJoin(
      // BGP to pick up all subject properties
      this.sparql.createBgp([this.sparql.createPattern(
        subject, property, value, this.graph.name)]),
      this.sparql.createBgp([this.sparql.createPattern(
        value, this.graph.namedNode(JRQL.item), item, this.graph.name)]));
  }

  findQuads(id: Iri, ctx = this.defaultCtx): Observable<Quad> {
    return observeAsyncIterator(this.graph.query(this.resolve(id, ctx)));
  }

  private async update(query: Update, ctx: ActiveContext): Promise<PatchQuads> {
    let patch = new PatchQuads();

    const vars = new Set<string>();
    const deleteQuads = query['@delete'] != null ?
      this.jrql.quads(query['@delete'], { mode: 'match', vars }, ctx) : undefined;
    const insertQuads = query['@insert'] != null ?
      this.jrql.quads(query['@insert'], { mode: 'load' }, ctx) : undefined;

    let solutions: Observable<Binding> | null = null;
    if (query['@where'] != null) {
      // If there is a @where clause, use variable substitutions per solution
      solutions = this.solutions(query['@where'], this.project, ctx);
    } else if (deleteQuads != null && vars.size > 0) {
      // A @delete clause with no @where may be used to bind variables
      solutions = this.project(
        this.sparql.createBgp(deleteQuads.map(this.toPattern)), vars);
    }
    if (solutions != null) {
      await solutions.forEach(solution => {
        // If there are variables in the update for which there is no value in the
        // solution, or if the solution value is not compatible with the quad
        // position, then this is treated as no-match, even if this is a
        // @delete (i.e. DELETEWHERE does not apply if @where exists).
        const matchingQuads = (template?: Quad[]) => template == null ? [] :
          this.fillTemplate(template, solution).filter(quad => !anyVarTerm(quad));
        patch.append(new PatchQuads({
          deletes: matchingQuads(deleteQuads),
          inserts: matchingQuads(insertQuads)
        }));
      });
    } else if (!insertQuads?.some(anyVarTerm)) {
      // Both @delete and @insert have fixed quads, just apply them
      patch.append({ deletes: deleteQuads ?? [], inserts: insertQuads ?? [] });
    }
    return patch;
  }

  graphQuads(pattern: Subject | Subject[], ctx = this.defaultCtx) {
    const vars = new Set<string>();
    const quads = this.jrql.quads(pattern, { mode: 'graph', vars }, ctx);
    if (vars.size > 0)
      throw new Error('Pattern has variable content');
    return quads;
  }

  private toPattern = (quad: Quad): Algebra.Pattern => {
    return this.sparql.createPattern(
      quad.subject, quad.predicate, quad.object, quad.graph);
  };

  private solutions<T>(where: Subject | Subject[] | Group,
    exec: (op: Algebra.Operation, vars: Iterable<string>) => Observable<T>,
    ctx: ActiveContext): Observable<T> {
    const vars = new Set<string>();
    const op = this.operation(asGroup(where), vars, ctx);
    return op == null ? EMPTY : exec(op, vars);
  }

  private operation(where: Group | Subject,
    vars: Set<string>, ctx: ActiveContext): Algebra.Operation {
    if (isSubject(where)) {
      const quads = this.jrql.quads(where, { mode: 'match', vars }, ctx);
      return this.sparql.createBgp(quads.map(this.toPattern));
    } else {
      const graph = array(where['@graph']),
        union = array(where['@union']),
        filter = array(where['@filter']),
        values = array(where['@values']);
      const quads = graph.length ? this.jrql.quads(
        graph, { mode: 'match', vars }, ctx) : [];
      const bgp = this.sparql.createBgp(quads.map(this.toPattern));
      const unionOp = binaryFold(union,
        pattern => this.operation(pattern, vars, ctx),
        (left, right) => this.sparql.createUnion(left, right));
      const unioned = unionOp && bgp.patterns.length ?
        this.sparql.createJoin(bgp, unionOp) : unionOp ?? bgp;
      const filtered = filter.length ? this.sparql.createFilter(
        unioned, this.constraintExpr(filter, ctx)) : unioned;
      return values.length ? this.sparql.createJoin(
        this.valuesExpr(values, ctx), filtered) : filtered;
    }
  }

  private valuesExpr(
    values: VariableExpression[], ctx: ActiveContext): Algebra.Operation {
    const variableNames = new Set<string>();
    const variablesTerms = values.map<{ [variable: string]: Term }>(
      variableExpr => Object.entries(variableExpr).reduce<{ [variable: string]: Term }>(
        (variableTerms, [variable, expr]) => {
          const varName = JRQL.matchVar(variable);
          if (!varName)
            throw new Error('Variable not specified in a values expression');
          variableNames.add(varName);
          if (isConstraint(expr))
            throw new Error('Cannot use constraint in a values expression');
          return {
            ...variableTerms,
            [variable]: this.jrql.toObjectTerm(expr, ctx)
          };
        }, {}));

    return this.sparql.createValues(
      [...variableNames].map(this.graph.variable), variablesTerms);
  }

  private constraintExpr(
    constraints: Constraint[], ctx: ActiveContext): Algebra.Expression {
    const expression = binaryFold(
      // Every constraint and every entry in a constraint is ANDed
      flatten(constraints.map(constraint => Object.entries(constraint))),
      ([operator, expr]) => this.operatorExpr(operator, expr, ctx),
      (left, right) => this.sparql.createOperatorExpression('and', [left, right]));
    if (expression == null)
      throw new Error('Missing expression');
    return expression;
  }

  private operatorExpr(
    operator: string,
    expr: Expression | Expression[],
    ctx: ActiveContext): Algebra.Expression {
    if (operator in operators)
      return this.sparql.createOperatorExpression(
        (<any>operators)[operator].sparql,
        array(expr).map(expr => this.exprExpr(expr, ctx)));
    else
      throw new Error(`Unrecognised operator: ${operator}`);
  }

  private exprExpr(expr: Expression, ctx: ActiveContext): Algebra.Expression {
    if (isConstraint(expr)) {
      return this.constraintExpr([expr], ctx);
    } else {
      // noinspection SuspiciousTypeOfGuard
      const varName = typeof expr == 'string' && JRQL.matchVar(expr);
      return this.sparql.createTermExpression(varName ?
        this.graph.variable(varName) : this.jrql.toObjectTerm(expr, ctx));
    }
  }

  private project = (op: Algebra.Operation, vars: Iterable<string>): Observable<Binding> => {
    return observeAsyncIterator(this.graph.query(this.sparql.createProject(op,
      [...vars].map(varName => this.graph.variable(varName)))));
  };

  private fillTemplate(quads: Quad[], binding: Binding): Quad[] {
    return quads.map(quad => this.graph.quad(
      this.fillTemplatePos('subject', quad.subject, binding),
      this.fillTemplatePos('predicate', quad.predicate, binding),
      this.fillTemplatePos('object', quad.object, binding)));
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

  private resolve(iri: Iri, ctx = this.defaultCtx): NamedNode {
    return this.graph.namedNode(expandTerm(iri, ctx));
  }

  private bound(binding: Binding, term: Term): Term | undefined {
    switch (term.termType) {
      case 'Variable':
        const value = binding[`?${term.value}`];
        if (value != null)
          return value;

        // If this variable is a sub-variable, see if the parent variable is bound
        const [varName, subVarName] = JRQL.matchSubVarName(term.value);
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
    return this.graph.variable(anyName());
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
  return isArray(where) ? { '@graph': where } :
    isGroup(where) ? where : { '@graph': where };
}