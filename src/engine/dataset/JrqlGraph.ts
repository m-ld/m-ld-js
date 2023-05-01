import { Iri } from '@m-ld/jsonld';
import {
  Constraint,
  Expression,
  Group,
  isConstraint,
  isConstruct,
  isDescribe,
  isGroup,
  isSelect,
  isSubject,
  isUpdate,
  operators,
  Query,
  Read,
  Result,
  Subject,
  SubjectProperty,
  Update,
  Variable,
  VariableExpression,
  Write
} from '../../jrql-support';
import { Graph, PatchQuads } from '.';
import { finalize, mergeMap, reduce } from 'rxjs/operators';
import { concat, EMPTY, Observable, throwError } from 'rxjs';
import {
  asQueryVar,
  canPosition,
  Literal,
  NamedNode,
  Quad,
  QueryableRdfSourceProxy,
  Term,
  TriplePos
} from '../quads';
import { Algebra, Factory as SparqlFactory } from 'sparqlalgebrajs';
import { JRQL } from '../../ns';
import { JrqlQuads } from './JrqlQuads';
import { GraphSubject, MeldError, MeldReadState, ReadResult } from '../../api';
import { binaryFold, first, flatten, inflate, isArray } from '../util';
import { ConstructTemplate } from './ConstructTemplate';
import { Binding, QueryableRdfSource } from '../../rdfjs-support';
import { Consumable, each } from 'rx-flowable';
import { flatMap, ignoreIf } from 'rx-flowable/operators';
import { consume } from 'rx-flowable/consume';
import { constructSubject, JrqlMode } from '../jrql-util';
import { array } from '../../util';
import { readResult } from '../api-support';
import { Future } from '../Future';
import { JrqlContext } from '../SubjectQuads';

/**
 * A graph wrapper that provides low-level json-rql handling for queries. The
 * write methods don't actually make changes but produce Patches which can then
 * be applied to a Dataset.
 */
export class JrqlGraph {
  readonly sparql: SparqlFactory;
  readonly jrql: JrqlQuads;
  readonly asReadState: MeldReadState;

  /**
   * @param quads a quads graph to operate on
   */
  constructor(
    readonly quads: Graph
  ) {
    this.sparql = new SparqlFactory(quads);
    this.jrql = new JrqlQuads(quads);
    // Map ourselves to a top-level read state, used for constraints
    const jrqlGraph = this;
    this.asReadState = new (class extends QueryableRdfSourceProxy implements MeldReadState {
      // This uses the default initial context, so no prefix mappings
      ctx = new JrqlContext();
      get src(): QueryableRdfSource {
        return quads;
      }
      read<R extends Read>(request: R): ReadResult {
        return readResult(jrqlGraph.read(request, this.ctx));
      }
      get(id: string, ...properties: SubjectProperty[]) {
        return properties.length === 0 ?
          jrqlGraph.get(id, this.ctx) : jrqlGraph.pick(id, properties, this.ctx);
      }
      ask(pattern: Query) {
        return jrqlGraph.ask(pattern, this.ctx);
      }
    });
  }

  read(query: Read, ctx: JrqlContext): Consumable<GraphSubject> {
    // For describe, we have to extend the state lock for all sub-queries
    const finished = this.quads.lock.extend('state', 'read', new Future);
    return inflate(
      ctx.next(query['@context']),
      ctx => {
        if (isDescribe(query))
          return this.describe(array(query['@describe']), query['@where'], ctx);
        else if (isSelect(query) && query['@where'] != null)
          return this.select(query['@select'], query['@where'], ctx);
        else if (isConstruct(query))
          return this.construct(query['@construct'], query['@where'], ctx);
        else
          return throwError(() => new MeldError(
            'Unsupported pattern', 'Read type not supported.'));
      }
    ).pipe(finalize(finished.resolve));
  }

  async write(query: Write, ctx: JrqlContext): Promise<PatchQuads> {
    ctx = await ctx.next(query['@context']);
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

  async ask(query: Query, ctx: JrqlContext): Promise<boolean> {
    return this.quads.lock.extend('state', 'ask',
      ctx.next(query['@context']).then(activeCtx => this.quads.ask(this.sparql.createAsk(
        this.operation(query['@where'] ?? {}, activeCtx)))));
  }

  select(
    select: Result,
    where: Subject | Subject[] | Group,
    ctx: JrqlContext
  ): Consumable<GraphSubject> {
    return this.solutions(where, this.project, ctx).pipe(
      flatMap(solution =>
        consume(this.jrql.solutionSubject(select, solution, ctx)))
    );
  }

  describe(
    describes: (Iri | Variable)[],
    where: Subject | Subject[] | Group | undefined,
    ctx: JrqlContext
  ): Consumable<GraphSubject> {
    return concat(...describes.map(describe => {
      // noinspection TypeScriptValidateJSTypes
      const describedVarName = JRQL.matchVar(describe);
      if (describedVarName) {
        const described = this.quads.variable(describedVarName);
        return this.solutions(where || {}, op =>
          consume(this.quads.query(this.sparql.createDistinct(
            // Project out the subject
            this.sparql.createProject(op, [described])))), ctx).pipe(
          // For each found subject Id, describe it
          flatMap(binding =>
            consume(this.describe1(this.bound(binding, described)!, ctx)).pipe(
              ignoreIf(null))));
      } else {
        return consume(this.get(describe, ctx)).pipe(ignoreIf(null));
      }
    }));
  }

  get(id: Iri, ctx: JrqlContext): Promise<GraphSubject | undefined> {
    return this.quads.lock.extend('state', 'get',
      this.describe1(this.resolve(id, ctx), ctx));
  }

  pick(
    id: Iri,
    properties: SubjectProperty[],
    ctx: JrqlContext
  ): Promise<GraphSubject | undefined> {
    return this.quads.lock.extend('state', 'pick',
      first(this.constructResult(
        constructSubject(id, properties), undefined, ctx)));
  }

  private async describe1(subjectId: Term, ctx: JrqlContext): Promise<GraphSubject | undefined> {
    const propertyQuads: Quad[] = [], listItemQuads: Quad[] = [];
    await each(consume(this.quads.query(subjectId)), async propertyQuad => {
      let isSlot = false;
      if (propertyQuad.object.termType === 'NamedNode') {
        await each(consume(this.quads.query(
          propertyQuad.object,
          this.quads.namedNode(JRQL.item)
        )), listItemQuad => {
          listItemQuads.push(this.quads.quad(
            propertyQuad.subject,
            propertyQuad.predicate,
            listItemQuad.object,
            this.quads.name
          ));
          isSlot = true;
        });
      }
      if (!isSlot)
        propertyQuads.push(propertyQuad);
    });
    if (propertyQuads.length || listItemQuads.length)
      return this.jrql.toApiSubject(propertyQuads, listItemQuads, ctx);
  }

  construct(
    construct: Subject | Subject[],
    where: Subject | Subject[] | Group | undefined,
    ctx: JrqlContext
  ): Consumable<GraphSubject> {
    return consume(this.constructResult(construct, where, ctx));
  }

  // TODO: Add ordering to allow streaming results
  constructResult(
    construct: Subject | Subject[],
    where: Subject | Subject[] | Group | undefined,
    ctx: JrqlContext
  ): Observable<GraphSubject> {
    const template = new ConstructTemplate(construct, ctx);
    // If no where, use the construct as the pattern
    return this.solutions(where ?? template.asPattern, this.project, ctx).pipe(
      reduce((template, { value: solution, next }) => {
        try {
          return template.addSolution(solution);
        } finally {
          next(); // We are reading into the template
        }
      }, template),
      mergeMap(template => template.results()));
  }

  private async update(query: Update, ctx: JrqlContext): Promise<PatchQuads> {
    const patch = new PatchQuads(), vars = new Set<string>(),
      filters: Constraint[] = [], binds: VariableExpression[] = [];
    const deletes = this.jrql.in(JrqlMode.match, ctx)
      .on('var', v => vars.add(v))
      .on('filter', filter => filters.push(filter))
      .toQuads(query['@delete']);
    let insertsHasVar = false;
    const inserts = this.jrql.in(JrqlMode.load, ctx)
      .on('var', () => insertsHasVar = true)
      .on('bind', bind => binds.push(bind))
      .toQuads(query['@insert']);

    if (query['@update']) {
      const updater = this.jrql.in(JrqlMode.load, ctx);
      for (let uq of updater
        .on('var', v => vars.add(v))
        .on('bind', bind => binds.push(bind))
        .toQuads(query['@update'])) {
        deletes.push(this.quads.quad(
          uq.subject, uq.predicate, uq.before ?? updater.newVar()));
        inserts.push(uq);
        insertsHasVar ||= anyVarTerm(uq);
      }
    }

    const where = query['@where'];
    let solutions: Consumable<Binding> | null = null;
    if (where != null) {
      // If there is a @where clause, use variable substitutions per solution
      solutions = this.solutions(
        where, this.project, ctx, vars, filters, binds);
    } else if (vars.size > 0) {
      // A @delete clause with no @where may be used to bind variables
      const constrained = this.constrainedOperation(
        this.sparql.createBgp(deletes.map(this.toPattern)),
        ctx, vars, filters, binds, []);
      solutions = this.project(constrained, vars);
    }
    if (solutions != null) {
      await each(solutions, solution => {
        // If there are variables in the update for which there is no value in the
        // solution, or if the solution value is not compatible with the quad
        // position, then this is treated as no-match, even if this is a
        // @delete (i.e. DELETE WHERE does not apply if @where exists).
        const matchingQuads = (template?: Quad[]) => template == null ? [] :
          this.fillTemplate(template, solution).filter(quad => !anyVarTerm(quad));
        patch.include(new PatchQuads({
          deletes: matchingQuads(deletes),
          inserts: matchingQuads(inserts)
        }));
      });
    } else if (deletes.length) {
      // If the @delete has fixed quads, always apply them
      patch.include({ deletes });
    }
    if (inserts.length && where == null && !insertsHasVar) {
      // If the @insert has fixed quads (with no @where), always apply them,
      // even if the delete had no solutions, https://github.com/m-ld/m-ld-spec/issues/76
      patch.include({ inserts });
    }
    return patch;
  }

  graphQuads(pattern: Subject | Subject[], ctx: JrqlContext) {
    const vars = new Set<string>();
    const quads = this.jrql.in(JrqlMode.graph, ctx)
      .on('var', v => vars.add(v))
      .toQuads(pattern);
    if (vars.size > 0)
      throw new Error('Pattern has variable content');
    return quads;
  }

  private toPattern = (quad: Quad): Algebra.Pattern => {
    return this.sparql.createPattern(
      quad.subject, quad.predicate, quad.object, quad.graph);
  };

  private solutions<T>(
    where: Subject | Subject[] | Group,
    exec: (op: Algebra.Operation, rtnVars: Iterable<string>) => Consumable<T>,
    ctx: JrqlContext,
    rtnVars = new Set<string>(),
    filters: ReadonlyArray<Constraint> = [],
    binds: ReadonlyArray<VariableExpression> = []
  ): Consumable<T> {
    const op = this.operation(where, ctx, rtnVars, filters, binds);
    return op == null ? EMPTY : exec(op, rtnVars);
  }

  private operation(
    where: Subject | Subject[] | Group,
    ctx: JrqlContext,
    vars = new Set<string>(),
    filters: ReadonlyArray<Constraint> = [],
    binds: ReadonlyArray<VariableExpression> = []
  ): Algebra.Operation {
    const group = asGroup(where);
    const graph = array(group['@graph']),
      union = array(group['@union']),
      filter = array(group['@filter']).concat(filters),
      values = array(group['@values']),
      bind = array(group['@bind']).concat(binds);
    const quads: Quad[] = !graph.length ? [] : this.jrql.in(JrqlMode.match, ctx)
      .on('var', v => vars.add(v))
      .on('filter', f => filter.push(f))
      .toQuads(graph);
    const bgp = this.sparql.createBgp(quads.map(this.toPattern));
    const unionOp = binaryFold(union,
      pattern => this.operation(pattern, ctx, vars),
      (left, right) => this.sparql.createUnion([left, right]));
    const unioned = unionOp && bgp.patterns.length ?
      this.sparql.createJoin([bgp, unionOp]) : unionOp ?? bgp;
    return this.constrainedOperation(unioned, ctx, vars, filter, bind, values);
  }

  private constrainedOperation(
    op: Algebra.Operation,
    ctx: JrqlContext,
    rtnVars: Set<string>,
    filter: ReadonlyArray<Constraint>,
    bind: ReadonlyArray<VariableExpression>,
    values: ReadonlyArray<VariableExpression> = []
  ) {
    const filtered = filter.length ? this.sparql.createFilter(
      op, this.constraintExpr(filter, ctx)) : op;
    const valued = values.length ? this.sparql.createJoin(
      [this.valuesExpr(values, ctx), filtered]) : filtered;
    return this.extendedOperation(valued, bind, rtnVars, ctx);
  }

  private extendedOperation(
    operation: Algebra.Operation,
    bind: ReadonlyArray<VariableExpression>,
    rtnVars: Set<string>,
    ctx: JrqlContext
  ): Algebra.Operation {
    return flatten(bind.map(expr => Object.entries(expr)))
      .reduce((operation, [variable, expr]) => {
        const varName = JRQL.matchVar(variable);
        if (!varName)
          throw new Error('Variable not specified in a bind expression');
        rtnVars.add(varName);
        const varTerm = this.quads.variable(varName);
        if (isConstraint(expr)) {
          return Object.entries(expr).reduce((operation, [operator, expr]) =>
            this.sparql.createExtend(operation, varTerm,
              this.operatorExpr(operator, expr, ctx)), operation);
        } else {
          return this.sparql.createExtend(operation, varTerm,
            this.sparql.createTermExpression(this.jrql.toObjectTerm(expr, ctx)));
        }
      }, operation);
  }

  private valuesExpr(
    values: ReadonlyArray<VariableExpression>,
    ctx: JrqlContext
  ): Algebra.Operation {
    const variableNames = new Set<string>();
    const variablesTerms = values.map(
      variableExpr => Object.entries(variableExpr)
        .reduce<{ [variable: string]: Literal | NamedNode }>(
          (variableTerms, [variable, expr]) => {
            const varName = JRQL.matchVar(variable);
            if (!varName)
              throw new Error('Variable not specified in a values expression');
            variableNames.add(varName);
            if (isConstraint(expr))
              throw new Error('Cannot use a constraint in a values expression');
            const valueTerm = this.jrql.toObjectTerm(expr, ctx);
            if (valueTerm.termType !== 'NamedNode' && valueTerm.termType !== 'Literal')
              throw new Error('Invalid value in values expression');
            variableTerms[variable] = valueTerm;
            return variableTerms;
          }, {}
        ));

    return this.sparql.createValues(
      [...variableNames].map(this.quads.variable), variablesTerms);
  }

  private constraintExpr(
    constraints: ReadonlyArray<Constraint>,
    ctx: JrqlContext
  ): Algebra.Expression {
    const expression = binaryFold(
      // Every constraint and every entry in a constraint is ANDed
      flatten(constraints.map(constraint => Object.entries(constraint))),
      ([operator, expr]) => this.operatorExpr(operator, expr, ctx),
      (left, right) =>
        this.sparql.createOperatorExpression('&&', [left, right]));
    if (expression == null)
      throw new Error('Missing expression');
    return expression;
  }

  private operatorExpr(
    operator: string,
    expr: Expression | Expression[],
    ctx: JrqlContext
  ): Algebra.Expression {
    if (operator in operators)
      return this.sparql.createOperatorExpression(
        (<any>operators)[operator].sparql,
        array(expr).map(expr => this.exprExpr(expr, ctx)));
    else
      throw new Error(`Unrecognised operator: ${operator}`);
  }

  private exprExpr(expr: Expression, ctx: JrqlContext): Algebra.Expression {
    if (isConstraint(expr)) {
      return this.constraintExpr([expr], ctx);
    } else {
      return this.sparql.createTermExpression(this.jrql.toObjectTerm(expr, ctx));
    }
  }

  private project = (
    op: Algebra.Operation,
    rtnVars: Iterable<string>
  ): Consumable<Binding> => {
    return consume(this.quads.query(this.sparql.createProject(op,
      [...rtnVars].map(varName => this.quads.variable(varName)))));
  };

  private fillTemplate(quads: Quad[], binding: Binding): Quad[] {
    return quads.map(quad => this.quads.quad(
      this.fillTemplatePos('subject', quad.subject, binding),
      this.fillTemplatePos('predicate', quad.predicate, binding),
      this.fillTemplatePos('object', quad.object, binding)));
  }

  private fillTemplatePos<P extends TriplePos>(
    pos: P,
    term: Quad[P],
    binding: Binding
  ): Quad[P] {
    switch (term.termType) {
      case 'Variable':
        const value = this.bound(binding, term);
        if (value != null && canPosition(pos, value))
          return value;
    }
    return term;
  }

  private resolve(iri: Iri, ctx: JrqlContext): NamedNode {
    return this.quads.namedNode(ctx.expandTerm(iri));
  }

  private bound(binding: Binding, term: Term): Term | undefined {
    switch (term.termType) {
      case 'Variable':
        const value = binding[asQueryVar(term)];
        if (value != null)
          return value;

        // If this variable is a sub-variable, see if the parent variable is bound
        const [varName, subVarName] = JRQL.matchSubVarName(term.value);
        const genValue = subVarName != null ?
          this.jrql.genSubValue(binding[`?${varName}`], subVarName) : null;
        if (genValue != null)
          // Cache the generated value in the binding
          return binding[asQueryVar(term)] = genValue;
        break; // Not bound

      default:
        return term;
    }
  }
}

function anyVarTerm(quad: Quad) {
  return quad.subject.termType === 'Variable' ||
    quad.predicate.termType === 'Variable' ||
    quad.object.termType === 'Variable';
}

function asGroup(where: Subject | Subject[] | Group): Group {
  return isArray(where) ? { '@graph': where } :
    isGroup(where) ? where : { '@graph': where };
}