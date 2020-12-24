import { compact } from 'jsonld';
import { Iri, Url } from 'jsonld/jsonld-spec';
import { Binding } from 'quadstore';
import { DataFactory, Quad } from 'rdf-js';
import validDataUrl = require('valid-data-url');
import { GraphName } from '.';
import { any, array, uuid } from '../..';
import {
  Context, Subject, Result, Value, isValueObject, isReference, isSet
} from '../../jrql-support';
import { activeCtx, compactIri, jsonToRdf, rdfToJson } from '../jsonld';
import { meld } from '../MeldEncoding';
import { inPosition, TriplePos, rdf } from '../quads';

export namespace jrql {
  export const $id = 'http://json-rql.org';
  export const list = `${$id}/#list`; // Temporary list property
  export const item = `${$id}/#item`; // Slot item property
  export const hiddenVar = (name: string) => `${$id}/var#${name}`;
  export const hiddenVarRegex = new RegExp(`^${hiddenVar('([\\d\\w]+)')}$`);
}

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

  async solutionSubject(results: Result[] | Result, binding: Binding, context: Context) {
    const solutionId = this.rdf.blankNode();
    // Construct quads that represent the solution's variable values
    const subject = await this.toSubject(Object.entries(binding).map(([variable, term]) =>
      this.rdf.quad(
        solutionId,
        this.rdf.namedNode(hiddenVar(variable.slice(1))),
        inPosition('object', term))), [/* TODO: list-items */], context);
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
    new PreProcessor(opts, this.base).process(jsonld['@graph']);
    const quads = await jsonToRdf(this.graphName.termType !== 'DefaultGraph' ?
      { ...jsonld, '@id': this.graphName.value } : jsonld) as Quad[];
    return this.postProcess(quads);
  }

  private postProcess(quads: Quad[]): Quad[] {
    // TODO: Detect RDF lists and convert them to m-ld lists
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

  /**
   * @param propertyQuads subject-property-value quads
   * @param listItemQuads subject-index-item quads for list-like subjects
   * @returns a single subject compacted against the given context
   */
  async toSubject(
    propertyQuads: Quad[], listItemQuads: Quad[], context: Context): Promise<Subject> {
    const subject = (await compact(await rdfToJson(propertyQuads), context || {})) as Subject;
    if (listItemQuads.length) {
      const ctx = await activeCtx(context);
      // Sort the list items by index and construct an rdf:List
      const indexes = listItemQuads.map(iq => iq.predicate.value).sort((index1, index2) => {
        // TODO: Use list implementation-specific ordering
        const  data1 = dataUrlData(index1), data2 = dataUrlData(index2);
        return data1 != null && data2 != null ?
          data1.localeCompare(data2, undefined, { numeric: true }) :
          index1.localeCompare(index2);
      }).map(index => compactIri(index, ctx));
      // Create a subject containing only the list items
      const list = await this.toSubject(listItemQuads, [], context);
      subject['@list'] = indexes.map(index => <Value>list[index]);
    }
    return subject;
  }
}

class PreProcessor {
  query: boolean;
  vars?: Set<string>;

  constructor(
    { query, vars }: JrqlQuadsOptions,
    readonly base?: Iri) {
    this.query = query;
    this.vars = vars;
  }

  process(values: Value | Value[], top: boolean = true) {
    array(values).forEach(value => {
      // JSON-LD value object (with @value) cannot contain a variable
      if (typeof value === 'object' && !isValueObject(value)) {
        // If this is a Reference, we treat it as a Subject
        const subject: Subject = value as Subject;
        // Normalise explicit list objects: expand fully to slots
        if ('@list' in subject) {
          if (typeof subject['@list'] === 'object') {
            // This handles arrays as well as hashes
            // Normalise indexes to data URLs (throw if not convertible)
            Object.entries(subject['@list']).forEach(([index, item]) =>
              addSlot(subject, toIndexDataUrl(index), item));
          } else {
            // Singleton list value
            addSlot(subject, toIndexDataUrl(0), subject['@list']);
          }
          delete subject['@list'];
          if (!this.query && subject['@type'] == null)
            subject['@type'] = meld.rdflseq.value;
        }
        // Process predicates and objects
        Object.entries(subject).forEach(([key, value]) => {
          if (key !== '@context') {
            const varKey = hideVar(key, this.vars);
            if (isSet(value)) {
              this.process(value['@set'], false);
            } else if (typeof value === 'object') {
              this.process(value as Value | Value[], false);
            } else if (typeof value === 'string') {
              const varVal = hideVar(value, this.vars);
              if (varVal !== value)
                value = !key.startsWith('@') ? { '@id': varVal } : varVal;
            }
            subject[varKey] = value;
            if (varKey !== key)
              delete subject[key];
          }
        });
        // References at top level => implicit wildcard p-o
        if (top && this.query && isReference(subject))
          (<any>subject)[genHiddenVar(this.vars)] = { '@id': genHiddenVar(this.vars) };
        // Anonymous query subjects => blank node subject (match any) or skolem
        if (!subject['@id'])
          subject['@id'] = this.query ? hiddenVar(genVarName(), this.vars) : skolem(this.base);
      }
    });
  }
}

function addSlot(subject: Subject, index: string, item: any) {
  if (index !== '@context') {
    // Anonymous slot will have a variable or skolem @id added later
    const slot = { [jrql.item]: item };
    const existing = subject[index] as Exclude<Subject['any'], Context>;
    if (isSet(existing))
      existing['@set'] = array(existing['@set']).concat(slot);
    else
      subject[index] = array(existing).concat(slot);
  } else {
    throw new Error('Cannot include context in list');
  }
}

function hideVar(token: string, vars?: Set<string>): string {
  const name = matchVar(token);
  // Allow anonymous variables as '?'
  return name === '' ? genHiddenVar(vars) : name ? hiddenVar(name, vars) : token;
}

export function matchVar(token: string): string | undefined {
  return /^\?([\d\w]*)$/g.exec(token)?.[1];
}

function matchHiddenVar(value: string): string | undefined {
  return jrql.hiddenVarRegex.exec(value)?.[1];
}

function genHiddenVar(vars?: Set<string>) {
  return hiddenVar(genVarName(), vars);
}

export function genVarName() {
  return any().slice(1);
}

function hiddenVar(name: string, vars?: Set<string>) {
  vars && vars.add(name);
  return jrql.hiddenVar(name);
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

function toIndexDataUrl(index?: string | number): Url {
  if (index == null || index === '') {
    throw new Error('Malformed list index');
  } else {
    const n = Number(index);
    if (Number.isSafeInteger(n))
      return `data:,${n.toFixed(0)}`;
    else if (typeof index == 'number')
      throw new Error('Non-integer list index');
    else
      return toIndexDataUrl(dataUrlData(index, 'text/plain', 'application/json'));
  }
}

function dataUrlData(url: Url, ...contentTypes: string[]): string | undefined {
  const match = url.trim().match(validDataUrl.regex);
  const data = match?.[match.length - 1];
  if (data != null) {
    const contentType = match?.[1]?.split(';')[0]?.toLowerCase() || 'text/plain';
    if (contentTypes.includes(contentType))
      return data;
  }
}
