import { compact } from 'jsonld';
import { Iri } from 'jsonld/jsonld-spec';
import { Binding } from 'quadstore';
import { DataFactory, Quad } from 'rdf-js';
import { uuid } from 'short-uuid';
import { GraphName } from '.';
import { any, array } from '../..';
import {
  Context, Subject, Result, Value, isValueObject, isReference, isContainer
} from '../../jrql-support';
import { jsonToRdf, rdfToJson } from '../jsonld';
import { canPosition, inPosition, TriplePos } from '../quads';

export interface JrqlQuadsOptions {
  /** Whether this will be used to match quads or insert them */
  query: boolean;
  /** The variable names found (sans '?') */
  vars?: Set<string>;
}

export class JrqlQuads {
  constructor(
    private readonly rdf: Required<DataFactory>,
    private readonly graphName: GraphName,
    private readonly base?: Iri) {
  }

  async solutionSubject(results: Result[] | Result, solution: Binding, context: Context) {
    const solutionId = this.rdf.blankNode();
    // Construct quads that represent the solution's variable values
    const subject = await toSubject(Object.entries(solution).map(([variable, term]) =>
      this.rdf.quad(
        solutionId,
        this.rdf.namedNode(hiddenVar(variable.slice(1))),
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

  async quads(g: Subject | Subject[], opts: JrqlQuadsOptions, context: Context): Promise<Quad[]> {
    // TODO: hideVars should not be in-place
    const jsonld = { '@graph': JSON.parse(JSON.stringify(g)), '@context': context };
    this.preProcess(jsonld['@graph'], opts);
    const quads = await jsonToRdf(this.graphName.termType !== 'DefaultGraph' ?
      { ...jsonld, '@id': this.graphName.value } : jsonld) as Quad[];
    return this.postProcess(quads);
  }

  private preProcess(
    values: Value | Value[],
    { query, vars }: JrqlQuadsOptions,
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
            if (isContainer(value)) {
              // TODO: Explicit list is always converted to json-rql indexed format
              this.preProcess('@list' in value ? value['@list'] : value['@set'], { query, vars }, false);
            } else if (typeof value === 'object') {
              this.preProcess(value as Value | Value[], { query, vars }, false);
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
          (<any>subject)[genHiddenVar(vars)] = { '@id': genHiddenVar(vars) };
        // Anonymous query subjects => blank node subject (match any) or skolem
        if (!subject['@id'])
          subject['@id'] = query ? hiddenVar(genVarName(), vars) : skolem(this.base);
      }
    });
  }

  private postProcess(quads: Quad[]): Quad[] {
    return quads.map(quad => this.rdf.quad(
      this.unhideVar('subject', quad.subject),
      this.unhideVar('predicate', quad.predicate),
      this.unhideVar('object', quad.object),
      quad.graph));
  }

  private unhideVar<P extends TriplePos>(_pos: P, term: Quad[P]): Quad[P] {
    switch (term.termType) {
      case 'NamedNode':
        const varName = matchHiddenVar(term.value);
        if (varName)
          return this.rdf.variable(varName);
    }
    return term;
  }
}

/**
 * @returns a single subject compacted against the given context
 */
export async function toSubject(quads: Quad[], context: Context): Promise<Subject> {
  return compact(await rdfToJson(quads), context || {}) as unknown as Subject;
}

function hideVar(token: string, vars?: Set<string>): string {
  const name = matchVar(token);
  // Allow anonymous variables as '?'
  return name === '' ? genHiddenVar(vars) : name ? hiddenVar(name, vars) : token;
}

export function matchVar(token: string): string | undefined {
  const match = /^\?([\d\w]*)$/g.exec(token);
  if (match)
    return match[1];
}

function matchHiddenVar(value: string): string | undefined {
  const match = /^http:\/\/json-rql.org\/var#([\d\w]+)$/g.exec(value);
  if (match)
    return match[1];
}

function genHiddenVar(vars?: Set<string>) {
  return hiddenVar(genVarName(), vars);
}

export function genVarName() {
  return any().slice(1);
}

function hiddenVar(name: string, vars?: Set<string>) {
  vars && vars.add(name);
  return `http://json-rql.org/var#${name}`;
}

/**
 * Generates a new skolemization IRI for a blank node. The base is allowed to be
 * `undefined` but the function will throw a `TypeError` if it is.
 * @see https://www.w3.org/TR/rdf11-concepts/#h3_section-skolemization
 */
function skolem(base: Iri | undefined): Iri {
  return new URL(`/.well-known/genid/${uuid()}`, base).href;
}

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (Array.isArray(results) ? results.includes(key) : results === key);
}