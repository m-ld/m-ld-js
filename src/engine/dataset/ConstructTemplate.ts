import { Iri } from 'jsonld/jsonld-spec';
import { Binding } from 'quadstore';
import { from, Observable } from 'rxjs';
import { mergeMap } from 'rxjs/operators';
import { anyName } from '../../api';
import {
  isList, isPropertyObject, isSet, isSubjectObject, Subject,
  SubjectProperty, SubjectPropertyObject, Value, Variable
} from '../../jrql-support';
import { matchVar } from '../../ns/json-rql';
import { array } from '../../util';
import { addPropertyObject, listItems } from '../jrql-util';
import { ActiveContext, compactIri } from '../jsonld';
import { jrqlProperty, jrqlValue } from '../SubjectGraph';

export class ConstructTemplate {
  private templates: SubjectTemplate[];

  constructor(construct: Subject | Subject[], ctx: ActiveContext) {
    this.templates = array(construct).map(c => new SubjectTemplate(c, ctx));
  }

  get asPattern(): Subject[] {
    return this.templates.map(t => t.pattern);
  }

  addSolution(solution: Binding): ConstructTemplate {
    // For each matched binding, populate the template, merging previous
    this.templates.forEach(template => template.addSolution(solution));
    return this;
  }

  get results(): Observable<Subject> {
    return from(this.templates).pipe(
      mergeMap(template => template.results.values()));
  }
}

function withNamedVar<T>(
  varNameIfString: T,
  whenIsVar: (variable: Variable, name: string) => void,
  otherwise?: (target: Exclude<T, string>) => void) {
  if (typeof varNameIfString == 'string') {
    const varName = varNameIfString || anyName();
    whenIsVar(`?${varName}`, varName);
  } else {
    otherwise?.(<Exclude<T, string>>varNameIfString);
  }
}

/**
 * Wraps a template subject:
 * - Adds missing `@id` fields to template subjects
 * - Adds index variables to list arrays
 * - Stores variable paths for population
 */
class SubjectTemplate {
  results = new Map<Iri | undefined, Subject>();
  private templateId?: Iri;
  private variableId?: Variable;
  private variableProps: [SubjectProperty, Variable][] = [];
  private variableValues: [SubjectProperty, Variable][] = [];
  private literalValues: [SubjectProperty, Value][] = [];
  private nestedSubjects: [SubjectProperty, SubjectTemplate][] = [];

  constructor(construct: Subject, readonly ctx: ActiveContext) {
    // Discover or create the subject identity variable
    this.templateId = construct['@id'];
    if (this.templateId != null)
      withNamedVar(matchVar(this.templateId),
        variable => this.variableId = variable);
    // Discover List variables
    if (isList(construct))
      for (let [index, item] of listItems(construct['@list'], 'match'))
        withNamedVar(index,
          (variable, name) => this.addProperty(['@list', variable], name, item),
          index => this.addProperty(['@list', ...index], null, item))
    // Discover other property variables
    for (let key in construct) {
      const value = construct[key];
      if (key !== '@list' && isPropertyObject(key, value))
        withNamedVar(matchVar(key),
          (variable, name) => this.addProperty(variable, name, value),
          () => this.addProperty(key, null, value));
    }
  }

  get pattern(): Subject {
    const pattern = { '@id': this.variableId ?? this.templateId };
    for (let [property, value] of this.literalValues)
      addPropertyObject(pattern, property, value);
    for (let [property, variable] of this.variableValues)
      addPropertyObject(pattern, property, variable);
    for (let [property, template] of this.nestedSubjects)
      addPropertyObject(pattern, property, template.pattern);
    return pattern;
  }

  private addProperty(property: SubjectProperty,
    propVarName: string | null, object: SubjectPropertyObject) {
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
     *   - Variable not in solution: all solutions go in one result with template ID
     * - No template ID: all solutions go in one result with no ID
     */
    const sid = this.variableId != null && this.variableId in solution ?
      compactIri(solution[this.variableId].value, this.ctx) : undefined;
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
          addPropertyObject(result, substitute, getObject(substitute));
      else if (includeAll)
        populateWith = getObject =>
          addPropertyObject(result, patternProp, getObject(patternProp));
      else
        populateWith = () => {};
      return { populateWith };
    };
  }

  private createResult(sid: string | undefined): Subject {
    const result: Subject = {}, rid = sid ?? this.templateId;
    if (rid != null)
      result['@id'] = rid;
    this.results.set(sid, result);
    return result;
  }
}