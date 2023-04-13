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
import { finalize, map, mergeMap, reduce } from 'rxjs/operators';
import { concat, EMPTY, Observable, throwError } from 'rxjs';
import {
  canPosition,
  Literal,
  NamedNode,
  Quad,
  QueryableRdfSourceProxy,
  Term,
  TriplePos
} from '../quads';
import { JsonldContext } from '../jsonld';
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
import { constructSubject } from '../jrql-util';
import { array } from '../../util';
import { readResult } from '../api-support';
import { Future } from '../Future';

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
      ctx = JsonldContext.initial();
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

  read(query: Read, ctx: JsonldContext): Consumable<GraphSubject> {
    return inflate(
      this.quads.lock.extend('state', 'read', ctx.next(query['@context'])),
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
      });
  }

  async write(query: Write, ctx: JsonldContext): Promise<PatchQuads> {
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

  async ask(query: Query, ctx: JsonldContext): Promise<boolean> {
    return this.quads.lock.extend('state', 'ask',
      ctx.next(query['@context']).then(activeCtx => this.quads.ask(this.sparql.createAsk(
        this.operation(asGroup(query['@where'] ?? {}), new Set, activeCtx)))));
  }

  select(
    select: Result,
    where: Subject | Subject[] | Group,
    ctx: JsonldContext
  ): Consumable<GraphSubject> {
    return this.solutions(where, this.project, ctx).pipe(
      map(({ value: solution, next }) =>
        ({ value: this.jrql.solutionSubject(select, solution, ctx), next })));
  }

  describe(
    describes: (Iri | Variable)[],
    where: Subject | Subject[] | Group | undefined,
    ctx: JsonldContext
  ): Consumable<GraphSubject> {
    // For describe, we have to extend the state lock for all sub-queries
    const finished = this.quads.lock.extend('state', 'describe', new Future);
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
    })).pipe(finalize(finished.resolve));
  }

  get(id: Iri, ctx: JsonldContext): Promise<GraphSubject | undefined> {
    return this.quads.lock.extend('state', 'get',
      this.describe1(this.resolve(id, ctx), ctx));
  }

  pick(
    id: Iri,
    properties: SubjectProperty[],
    ctx: JsonldContext
  ): Promise<GraphSubject | undefined> {
    return this.quads.lock.extend('state', 'pick',
      first(this.constructResult(
        constructSubject(id, properties), undefined, ctx)));
  }

  private async describe1(subjectId: Term, ctx: JsonldContext): Promise<GraphSubject | undefined> {
    const propertyQuads: Quad[] = [], listItemQuads: Quad[] = [];
    await each(consume(this.quads.query(subjectId)), async propertyQuad => {
      let isSlot = false;
      if (propertyQuad.object.termType === 'NamedNode') {
        await each(consume(this.quads.query(
          propertyQuad.object, this.quads.namedNode(JRQL.item))), listItemQuad => {
          listItemQuads.push(this.quads.quad(
            propertyQuad.subject, propertyQuad.predicate, listItemQuad.object, this.quads.name));
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
    ctx: JsonldContext
  ): Consumable<GraphSubject> {
    return consume(this.constructResult(construct, where, ctx));
  }

  // TODO: Add ordering to allow streaming results
  constructResult(
    construct: Subject | Subject[],
    where: Subject | Subject[] | Group | undefined,
    ctx: JsonldContext
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

  private async update(query: Update, ctx: JsonldContext): Promise<PatchQuads> {
    let patch = new PatchQuads();

    const vars = new Set<string>();
    const deletes = query['@delete'] != null ?
      this.jrql.quads(query['@delete'], { mode: 'match', vars }, ctx) : undefined;
    const inserts = query['@insert'] != null ?
      this.jrql.quads(query['@insert'], { mode: 'load' }, ctx) : undefined;

    let solutions: Consumable<Binding> | null = null;
    const where = query['@where'];
    if (where != null) {
      // If there is a @where clause, use variable substitutions per solution
      solutions = this.solutions(where, this.project, ctx);
    } else if (deletes != null && vars.size > 0) {
      // A @delete clause with no @where may be used to bind variables
      solutions = this.project(
        this.sparql.createBgp(deletes.map(this.toPattern)), vars);
    }
    if (solutions != null) {
      await each(solutions, solution => {
        // If there are variables in the update for which there is no value in the
        // solution, or if the solution value is not compatible with the quad
        // position, then this is treated as no-match, even if this is a
        // @delete (i.e. DELETE WHERE does not apply if @where exists).
        const matchingQuads = (template?: Quad[]) => template == null ? [] :
          this.fillTemplate(template, solution).filter(quad => !anyVarTerm(quad));
        patch.append(new PatchQuads({
          deletes: matchingQuads(deletes),
          inserts: matchingQuads(inserts)
        }));
      });
    } else if (deletes != null) {
      // If the @delete has fixed quads, always apply them
      patch.append({ deletes });
    }
    if (inserts != null && where == null && !inserts.some(anyVarTerm)) {
      // If the @insert has fixed quads (with no @where), always apply them,
      // even if the delete had no solutions, https://github.com/m-ld/m-ld-spec/issues/76
      patch.append({ inserts });
    }
    return patch;
  }

  graphQuads(pattern: Subject | Subject[], ctx: JsonldContext) {
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

  private solutions<T>(
    where: Subject | Subject[] | Group,
    exec: (op: Algebra.Operation, vars: Iterable<string>) => Consumable<T>,
    ctx: JsonldContext
  ): Consumable<T> {
    const vars = new Set<string>();
    const op = this.operation(asGroup(where), vars, ctx);
    return op == null ? EMPTY : exec(op, vars);
  }

  private operation(
    where: Group | Subject,
    vars: Set<string>,
    ctx: JsonldContext
  ): Algebra.Operation {
    if (isSubject(where)) {
      const quads = this.jrql.quads(where, { mode: 'match', vars }, ctx);
      return this.sparql.createBgp(quads.map(this.toPattern));
    } else {
      const graph = array(where['@graph']),
        union = array(where['@union']),
        filter = array(where['@filter']),
        values = array(where['@values']),
        binds = array(where['@bind']);
      const quads = graph.length ? this.jrql.quads(
        graph, { mode: 'match', vars }, ctx) : [];
      const bgp = this.sparql.createBgp(quads.map(this.toPattern));
      const unionOp = binaryFold(union,
        pattern => this.operation(pattern, vars, ctx),
        (left, right) => this.sparql.createUnion([left, right]));
      const unioned = unionOp && bgp.patterns.length ?
        this.sparql.createJoin([bgp, unionOp]) : unionOp ?? bgp;
      const filtered = filter.length ? this.sparql.createFilter(
        unioned, this.constraintExpr(filter, ctx)) : unioned;
      const valued = values.length ? this.sparql.createJoin(
        [this.valuesExpr(values, ctx), filtered]) : filtered;
      return this.extendedOperation(valued, binds, vars, ctx);
    }
  }

  private extendedOperation(
    operation: Algebra.Operation,
    binds: VariableExpression[],
    vars: Set<string>,
    ctx: JsonldContext
  ): Algebra.Operation {
    return flatten(binds.map(bind => Object.entries(bind)))
      .reduce((operation, [variable, expr]) => {
        const varName = JRQL.matchVar(variable);
        if (!varName)
          throw new Error('Variable not specified in a bind expression');
        vars.add(varName);
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
    values: VariableExpression[],
    ctx: JsonldContext
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
    constraints: Constraint[],
    ctx: JsonldContext
  ): Algebra.Expression {
    const expression = binaryFold(
      // Every constraint and every entry in a constraint is ANDed
      flatten(constraints.map(constraint => Object.entries(constraint))),
      ([operator, expr]) => this.operatorExpr(operator, expr, ctx),
      (left, right) =>
        this.sparql.createOperatorExpression('and', [left, right]));
    if (expression == null)
      throw new Error('Missing expression');
    return expression;
  }

  private operatorExpr(
    operator: string,
    expr: Expression | Expression[],
    ctx: JsonldContext
  ): Algebra.Expression {
    if (operator in operators)
      return this.sparql.createOperatorExpression(
        (<any>operators)[operator].sparql,
        array(expr).map(expr => this.exprExpr(expr, ctx)));
    else
      throw new Error(`Unrecognised operator: ${operator}`);
  }

  private exprExpr(expr: Expression, ctx: JsonldContext): Algebra.Expression {
    if (isConstraint(expr)) {
      return this.constraintExpr([expr], ctx);
    } else {
      return this.sparql.createTermExpression(this.jrql.toObjectTerm(expr, ctx));
    }
  }

  private project = (op: Algebra.Operation, vars: Iterable<string>): Consumable<Binding> => {
    return consume(this.quads.query(this.sparql.createProject(op,
      [...vars].map(varName => this.quads.variable(varName)))));
  };

  private fillTemplate(quads: Quad[], binding: Binding): Quad[] {
    return quads.map(quad => this.quads.quad(
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

  private resolve(iri: Iri, ctx: JsonldContext): NamedNode {
    return this.quads.namedNode(ctx.expandTerm(iri));
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