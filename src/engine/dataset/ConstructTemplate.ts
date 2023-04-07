import { Iri } from '@m-ld/jsonld';
import { anyName, blank, GraphSubject } from '../../api';
import {
  Group, isList, isPropertyObject, isSet, isSubjectObject, Subject, SubjectProperty,
  SubjectPropertyObject, Value, Variable
} from '../../jrql-support';
import { matchVar } from '../../ns/json-rql';
import { array } from '../../util';
import { addPropertyObject, listItems } from '../jrql-util';
import { JsonldContext } from '../jsonld';
import { jrqlProperty, jrqlValue } from '../SubjectGraph';
import { Binding } from '../../rdfjs-support';

export class ConstructTemplate {
  private readonly templates: SubjectTemplate[];

  constructor(construct: Subject | Subject[], ctx: JsonldContext) {
    this.templates = array(construct).map(c => new SubjectTemplate(c, ctx));
  }

  get asPattern(): Group {
    const group: Group = { '@union': [] };
    for (let template of this.templates)
      group['@union']!.push(...template.patterns());
    return group;
  }

  // noinspection JSUnusedGlobalSymbols used in JrqlGraph
  addSolution(solution: Binding): ConstructTemplate {
    // For each matched binding, populate the template, merging previous
    this.templates.forEach(template => template.addSolution(solution));
    return this;
  }

  *results(): Iterable<GraphSubject> {
    for (let template of this.templates)
      yield *template.results.values();
  }
}

/**
 * Wraps a template subject:
 * - Adds missing `@id` fields to template subjects
 * - Adds index variables to list arrays
 * - Stores variable paths for population
 */
class SubjectTemplate {
  results = new Map<Iri | undefined, GraphSubject>();
  private templateId?: Iri;
  private variableId?: Variable;
  private variableProps: [SubjectProperty, Variable][] = [];
  private variableValues: [SubjectProperty, Variable][] = [];
  private literalValues: [SubjectProperty, Value][] = [];
  private nestedSubjects: [SubjectProperty, SubjectTemplate][] = [];

  constructor(construct: Subject, readonly ctx: JsonldContext) {
    // Discover or create the subject identity variable
    if (construct['@id'] != null)
      withNamedVar(matchVar(construct['@id']),
        variable => this.variableId = variable,
        () => this.templateId = construct['@id']);
    // Discover List variables
    if (isList(construct))
      for (let [index, item] of listItems(construct['@list'], 'match'))
        withNamedVar(index, // Index may be a var name
          (variable, name) => this.addProperty(['@list', variable], name, item),
          index => this.addProperty(['@list', ...index], null, item));
    // Discover other property variables
    for (let key in construct) {
      const value = construct[key];
      if (key !== '@list' && isPropertyObject(key, value))
        withNamedVar(matchVar(key),
          (variable, name) => this.addProperty(variable, name, value),
          () => this.addProperty(key, null, value));
    }
  }
  /** Only used if the query does not have a `@where` component. */
  *patterns(): Generator<Subject> {
    const id = this.variableId ?? this.templateId;
    for (let [property, value] of this.literalValues)
      yield addPropertyObject({ '@id': id }, property, value);
    for (let [property, variable] of this.variableValues)
      yield addPropertyObject({ '@id': id }, property, variable);
    for (let [property, template] of this.nestedSubjects)
      for (let pattern of template.patterns())
        yield addPropertyObject({ '@id': id }, property, pattern);
  }

  private addProperty(property: SubjectProperty,
    propVarName: string | null, object: SubjectPropertyObject
  ) {
    // Register a property variable if present
    if (propVarName != null)
      this.variableProps.push([property, `?${propVarName}`]);
    // If the value is a nested Subject, create a template for it
    this.addObject(property, object);
  }

  private addObject(property: SubjectProperty, object: SubjectPropertyObject) {
    if (Array.isArray(object)) {
      // Sets (json-rql arrays) are flattened
      object.forEach(member => this.addObject(property, member));
    } else if (isSet(object)) {
      // TODO: this breaks the construct contract by eliding @set
      this.addObject(property, object['@set']);
    } else if (isSubjectObject(object)) {
      // Register a nested subject
      const nested = new SubjectTemplate(object, this.ctx);
      this.nestedSubjects.push([property, nested]);
    } else {
      // Register a value variable if present
      // noinspection SuspiciousTypeOfGuard
      withNamedVar(typeof object == 'string' && matchVar(object),
        variable => this.variableValues.push([property, variable]),
        () => this.literalValues.push([property, object]));
    }
  }

  addSolution(solution: Binding): Subject | undefined {
    /**
     * Options for Subject ID:
     * - Literal template ID: all solutions go in one result with ID
     * - Variable template ID:
     *   - Variable in solution: solution is matched to a result with ID
     *   - Variable not in solution: all solutions go in one result with
     *     template or blank ID
     * - No template ID: all solutions go in one result with blank ID
     */
    const sid = this.variableId != null && this.variableId in solution ?
      this.ctx.compactIri(solution[this.variableId].value) : undefined;
    // Do we already have a result for the bound Subject ID?
    const isNewResult = !this.results.has(sid);
    const result = this.results.get(sid) ?? this.createResult(sid);
    // Populate bound content: 1. Properties
    // Keep track of substitute property paths for values
    const populator = this.propertyPopulator(result, solution);
    // 2. Literal values (only include un-substituted if new)
    for (let [property, literal] of this.literalValues)
      populator(property, isNewResult).populateWith(() => literal);
    // 3. Bound values into (substitute) properties
    for (let [property, variable] of this.variableValues)
      if (variable in solution)
        populator(property).populateWith(
          resultProp => jrqlValue(resultProp, solution[variable], this.ctx));
    // 4. Nested subjects into (substitute) properties
    for (let [property, template] of this.nestedSubjects) {
      const nested = template.addSolution(solution);
      if (nested != null)
        populator(property).populateWith(() => nested);
    }
    // Ignore empty object
    for (let key in result)
      if (sid != null || this.templateId != null || key != '@id')
        return result;
  }

  private propertyPopulator(result: Subject, solution: Binding) {
    // Mapping from pattern property to result property
    const resultProps = new Map<SubjectProperty, SubjectProperty>();
    for (let [patternProp, variable] of this.variableProps)
      if (variable in solution)
        // TODO: jrqlProperty will translate any numeric to a list index
        resultProps.set(patternProp, jrqlProperty(solution[variable].value, this.ctx));
    return (patternProp: SubjectProperty, includeAll = true) => {
      const substitute = resultProps.get(patternProp);
      let populateWith: (getObject: (resultProp: SubjectProperty) => Value) => void;
      if (substitute != null)
        populateWith = getObject =>
          addPropertyObject(result, substitute, getObject(substitute), () => []);
      else if (includeAll)
        populateWith = getObject =>
          addPropertyObject(result, patternProp, getObject(patternProp), () => []);
      else
        populateWith = () => { };
      return { populateWith };
    };
  }

  private createResult(sid: string | undefined): GraphSubject {
    const result: GraphSubject = { '@id': sid ?? this.templateId ?? blank() };
    this.results.set(sid, result);
    return result;
  }
}

/**
 * Odd utility which takes an identifier that, if it is strictly a string, is a
 * variable name, and runs one of the given lambdas.
 * @param identifier a variable name if it is a string, otherwise anything
 * @param whenIsVar runs if the first parameter was a variable. If the variable
 * was the empty string, a variable name is generated.
 * @param otherwise runs if the first parameter was not a variable.
 */
function withNamedVar<T>(
  identifier: T,
  whenIsVar: (variable: Variable, name: string) => void,
  otherwise?: (target: Exclude<T, string>) => void
) {
  if (typeof identifier == 'string') {
    const varName = identifier || anyName();
    whenIsVar(`?${varName}`, varName);
  } else {
    otherwise?.(<Exclude<T, string>>identifier);
  }
}