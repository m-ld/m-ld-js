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
import { Graph } from '.';
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
import { JrqlPatchQuads, JrqlQuads } from './JrqlQuads';
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
    this.sparql = new SparqlFactory(quads.rdf);
    this.jrql = new JrqlQuads(quads);
    // Map ourselves to a top-level read state, used for constraints
    const jrqlGraph = this;
    this.asReadState = new (class extends QueryableRdfSourceProxy implements MeldReadState {
      // This uses the default initial context, so no prefix mappings or datatypes
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
  
  get rdf() {
    return this.quads.rdf;
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

  async write(query: Write, ctx: JrqlContext): Promise<JrqlPatchQuads> {
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
        const described = this.rdf.variable(describedVarName);
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
          this.rdf.namedNode(JRQL.item)
        )), listItemQuad => {
          listItemQuads.push(this.quads.quad(
            propertyQuad.subject,
            propertyQuad.predicate,
            listItemQuad.object
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

  private async update(update: Update, ctx: JrqlContext): Promise<JrqlPatchQuads> {
    return new JrqlGraph.Update(this, ctx, update).toPatch();
  }

  private static Update = class {
    deletes: Quad[];
    inserts: Quad[];
    where: Update['@where'];
    vars = new Set<string>();
    inlineFilters: Constraint[] = [];
    inlineBinds: VariableExpression[] = [];
    updateConstraints: Binding<[Variable, Constraint]> = {};
    insertsHasVar = false;

    constructor(
      readonly graph: JrqlGraph,
      readonly ctx: JrqlContext,
      update: Update
    ) {
      this.deletes = this.graph.jrql.in(JrqlMode.match, ctx)
        .on('var', v => this.vars.add(v))
        .on('filter', filter => this.inlineFilters.push(filter))
        .toQuads(update['@delete']);
      // Process inserts, which may have inline bindings. Any found variables are
      // not used for matching, so don't count as return vars.
      this.inserts = this.graph.jrql.in(JrqlMode.load, ctx)
        .on('var', () => this.insertsHasVar = true)
        .on('bind', (returnVar, binding) => this.inlineBinds.push({ [returnVar]: binding }))
        .toQuads(update['@insert']);
      // Process updates, which have both variables and bindings
      if (update['@update']) {
        const updater = this.graph.jrql.in(JrqlMode.load, ctx)
          .on('var', v => this.vars.add(v))
          .on('bind', (returnVar, binding, queryVar, constraint) => {
            this.inlineBinds.push({ [returnVar]: binding });
            this.updateConstraints[queryVar] = [returnVar, constraint];
          });
        for (let updateQuad of updater.toQuads(update['@update'])) {
          // Create a new var if needed, will trigger 'var' listener
          updateQuad.before ??= updater.newVar();
          const { subject, predicate, before } = updateQuad;
          this.deletes.push(this.graph.quads.quad(subject, predicate, before));
          this.inserts.push(updateQuad);
          this.insertsHasVar ||= anyVarTerm(updateQuad);
        }
      }
      this.where = update['@where'];
    }

    solutions() {
      if (this.where != null) {
        // If there is a @where clause, use variable substitutions per solution
        return this.graph.solutions(
          this.where, this.graph.project, this.ctx,
          this.vars, this.inlineFilters, this.inlineBinds
        );
      } else if (this.vars.size > 0) {
        // A @delete clause with no @where may be used to bind variables
        const constrained = this.graph.constrainedOperation(
          this.graph.sparql.createBgp(this.deletes.map(this.graph.toPattern)),
          this.ctx, this.vars, this.inlineFilters, this.inlineBinds, []
        );
        return this.graph.project(constrained, this.vars);
      }
    }

    async toPatch(): Promise<JrqlPatchQuads> {
      const { deletes, inserts } = this;
      const patch = new JrqlPatchQuads();
      // Establish a stream of solutions
      const solutions = this.solutions();
      if (solutions != null) {
        await each(solutions, async solution => {
          // If there are variables in the update for which there is no value in the
          // solution, or if the solution value is not compatible with the quad
          // position, then this is treated as no-match, even if this is a
          // @delete (i.e. DELETE WHERE does not apply if @where exists).
          patch.deletes.addAll(await this.fillTemplate(
            deletes, solution, async (queryVar, quad) => {
              // If the solution has bound a shared datatype to a variable for which a
              // binding expression exists, convert the expression to a custom operation
              if (queryVar in this.updateConstraints) {
                const [returnVar, constraint] = this.updateConstraints[queryVar];
                // TODO: Bind any variables in the constraint
                const opMeta = await this.graph.jrql
                  .applyTripleUpdate(quad, constraint, this.ctx);
                if (opMeta != null) {
                  patch.sharedDataOpMeta.push(opMeta);
                  // This theoretically overrides a binding that was generated by SPARQL
                  // TODO: What is the correct behaviour if so?
                  solution[returnVar] = opMeta.triple.object;
                }
              }
            }));
          patch.inserts.addAll(await this.fillTemplate(inserts, solution));
        });
        // We have definitively matched against existing data, so re-inserts can
        // be optimised away
        patch.minimise(true);
      } else if (deletes.length) {
        // If the @delete has fixed quads, always apply them
        patch.include({ deletes });
      }
      if (inserts.length && !this.where && !this.insertsHasVar) {
        // If the @insert has fixed quads (with no @where), always apply them,
        // even if the delete had no solutions, https://github.com/m-ld/m-ld-spec/issues/76
        patch.include({ inserts });
      }
      return patch;
    }

    async fillTemplate(
      quads: Quad[],
      binding: Binding,
      onBoundLiteral?: (variable: Variable, quad: Quad) => unknown
    ): Promise<Quad[]> {
      const filledQuads: Quad[] = [];
      for (let quad of quads ?? []) {
        const filledQuad = this.graph.quads.quad(
          this.fillTemplatePos('subject', quad.subject, binding),
          this.fillTemplatePos('predicate', quad.predicate, binding),
          this.fillTemplatePos('object', quad.object, binding)
        );
        if (!anyVarTerm(filledQuad)) {
          if (onBoundLiteral != null &&
            quad.object.termType === 'Variable' &&
            filledQuad.object.termType === 'Literal') {
            await onBoundLiteral(asQueryVar(quad.object), filledQuad);
          }
          filledQuads.push(filledQuad);
        }
      }
      return filledQuads;
    }

    fillTemplatePos<P extends TriplePos>(
      pos: P,
      term: Quad[P],
      binding: Binding
    ): Quad[P] {
      switch (term.termType) {
        case 'Variable':
          const value = this.graph.bound(binding, term);
          if (value != null && canPosition(pos, value))
            return value;
      }
      return term;
    }
  };

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
    return flatten(bind.map(expr => Object.entries(expr)))
      .reduce((operation, [variable, expr]) => {
        const varName = JRQL.matchVar(variable);
        if (!varName)
          throw new Error('Variable not specified in a bind expression');
        rtnVars.add(varName);
        const varTerm = this.rdf.variable(varName);
        if (isConstraint(expr)) {
          return Object.entries(expr).reduce((operation, [operator, expr]) => {
            // If the operator is not supported by SPARQL, we just extend the
            // input variable. The custom datatype may handle it later
            const opExpr = this.operatorExpr(operator, expr, ctx) ??
              this.sparql.createTermExpression(varTerm);
            return this.sparql.createExtend(operation, varTerm, opExpr);
          }, operation);
        } else {
          return this.sparql.createExtend(operation, varTerm,
            this.sparql.createTermExpression(this.jrql.toObjectTerm(expr, ctx))
          );
        }
      }, valued);
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
      [...variableNames].map(this.rdf.variable), variablesTerms);
  }

  private constraintExpr(
    constraints: ReadonlyArray<Constraint>,
    ctx: JrqlContext
  ): Algebra.Expression {
    const expression = binaryFold(
      // Every constraint and every entry in a constraint is ANDed
      flatten(constraints.map(constraint => Object.entries(constraint))),
      ([operator, expr]) => {
        const operatorExpr = this.operatorExpr(operator, expr, ctx);
        if (operatorExpr != null)
          return operatorExpr;
        else
          throw new Error(`No SPARQL operator: ${operator}`);
      },
      (...lr) => this.sparql.createOperatorExpression('&&', lr));
    if (expression == null)
      throw new Error('Missing expression');
    return expression;
  }

  private operatorExpr(
    operator: string,
    expr: Expression | Expression[],
    ctx: JrqlContext
  ): Algebra.Expression | undefined {
    if (operator in operators) {
      const sparqlOperator = operators[operator].sparql;
      if (sparqlOperator != null)
        return this.sparql.createOperatorExpression(
          sparqlOperator, array(expr).map(expr => this.exprExpr(expr, ctx)));
      // Otherwise no SPARQL equivalent, return undefined but do not fail
    } else {
      throw new Error(`Unrecognised operator: ${operator}`);
    }
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
      [...rtnVars].map(varName => this.rdf.variable(varName)))));
  };

  private resolve(iri: Iri, ctx: JrqlContext): NamedNode {
    return this.rdf.namedNode(ctx.expandTerm(iri));
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