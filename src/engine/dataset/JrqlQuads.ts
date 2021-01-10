import { compact } from 'jsonld';
import { Iri, Url } from 'jsonld/jsonld-spec';
import { clone } from 'jsonld/lib/util';
import { Binding } from 'quadstore';
import { DataFactory, Quad, Term } from 'rdf-js';
import { GraphName } from '.';
import { any, array, includeValue, uuid } from '../..';
import {
  Context, Subject, Result, Value, isValueObject, isReference,
  isSet, isList, List, SubjectPropertyObject
} from '../../jrql-support';
import { activeCtx, compactIri, dataUrlData, jsonToRdf, rdfToJson } from '../jsonld';
import { inPosition, TriplePos } from '../quads';
const { isArray } = Array;

const PN_CHARS_BASE = `[A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD]|[\uD800-\uDB7F][\uDC00-\uDFFF]`;
const PN_CHARS_U = `(?:${PN_CHARS_BASE}|_)`;
const VARNAME = `(?:${PN_CHARS_U}|[0-9])(?:${PN_CHARS_U}|[0-9]|\u00B7|[\u0300-\u036F\u203F-\u2040])*`;

export namespace jrql {
  export const $id = 'http://json-rql.org';
  export const item = 'http://json-rql.org/#item'; // Slot item property
  export const index = 'http://json-rql.org/#index'; // Entailed slot index property
  export const hiddenVar = (name: string) => `${$id}/var#${name}`;

  export interface Slot extends Subject {
    '@id': Iri;
    [item]: Value | Value[];
    [index]: number;
  }

  export function isSlot(s: Subject): s is Slot {
    return s['@id'] != null &&
      s[item] != null &&
      isNaturalNumber(s[index]);
  }
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
    // The pre-processor acts on the input graph in-place
    const jsonld = { '@graph': clone(g), '@context': context };
    new PreProcessor(opts, this).process(jsonld['@graph']);
    const quads = await jsonToRdf(this.graphName.termType !== 'DefaultGraph' ?
      { ...jsonld, '@id': this.graphName.value } : jsonld) as Quad[];
    return this.postProcess(quads);
  }

  private postProcess(quads: Quad[]): Quad[] {
    // TODO: Detect RDF lists e.g. from '@container': '@list' in context, and
    // convert them to m-ld lists
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
      // Sort the list items lexically by index
      // TODO: Allow for a list implementation-specific ordering
      const indexes = new Set(listItemQuads.map(iq => iq.predicate.value).sort()
        .map(index => compactIri(index, ctx)));
      // Create a subject containing only the list items
      const list = await this.toSubject(listItemQuads, [], context);
      subject['@list'] = [...indexes].map(index => <Value>list[index]);
    }
    return subject;
  }

  /**
   * Generates a new skolemization IRI for a blank node. The base is allowed to be
   * `undefined` but the function will throw a `TypeError` if it is.
   * @see https://www.w3.org/TR/rdf11-concepts/#h3_section-skolemization
   */
  skolem(): Iri {
    return new URL(`/.well-known/genid/${uuid()}`, this.base).href;
  }

  genSubValue(parentValue: Term, subVarName: SubVarName) {
    switch (subVarName) {
      case 'listKey':
        // Generating a data URL for the index key
        return this.rdf.namedNode(toIndexDataUrl(Number(parentValue.value)));
      case 'slotId':
        // Index exists, so a slot can be made
        return this.rdf.namedNode(this.skolem());
    }
  }
}

class PreProcessor {
  query: boolean;
  vars?: Set<string>;

  constructor(
    { query, vars }: JrqlQuadsOptions,
    readonly jrql: JrqlQuads) {
    this.query = query;
    this.vars = vars;
  }

  process(object: SubjectPropertyObject, top: boolean = true) {
    array(object).forEach(value => {
      if (isArray(value)) {
        this.process(value, top);
      } else if (typeof value === 'object' && !isValueObject(value)) {
        // JSON-LD value object (with @value) cannot contain a variable or a
        // list, so ignore it. If this is a Reference, we treat it as a Subject.
        const subject: Subject = value as Subject;
        if (isList(subject))
          this.expandListSlots(subject);
        // Process predicates and objects
        this.processEntries(subject);
        // References at top level => implicit wildcard p-o
        if (top && this.query && isReference(subject))
          (<any>subject)[genHiddenVar(this.vars)] = { '@id': genHiddenVar(this.vars) };
        // Anonymous query subjects => blank node subject (match any) or skolem
        if (!subject['@id'])
          subject['@id'] = this.query ? hiddenVar(genVarName(), this.vars) : this.jrql.skolem();
      }
    });
  }

  private processEntries(subject: Subject) {
    Object.entries(subject).forEach(([key, value]: [string, SubjectPropertyObject]) => {
      if (key !== '@context' && value != null) {
        const varKey = hideVar(key, this.vars);
        if (isSet(value)) {
          this.process(value['@set'], false);
        } else if (typeof value === 'object') {
          this.process(value, false);
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
  }

  private expandListSlots(list: List) {
    // Normalise explicit list objects: expand fully to slots
    if (typeof list['@list'] === 'object') {
      // This handles arrays as well as hashes
      // Normalise indexes to data URLs (throw if not convertible)
      Object.entries(list['@list']).forEach(([indexKey, item]) => {
        // Provided index is either a variable (string) or an index number
        const index = matchVar(indexKey) ?? toIndexNumber(indexKey);
        if (index == null)
          throw new Error(`List index ${indexKey} is not a variable or number data`);
        // Check for inserting multiple sub-items at one index
        if (typeof index != 'string' && !this.query &&
          !isArray(index) && isArray(item) && !isArray(list['@list'])) {
          item.forEach((subItem, subIndex) =>
            this.addSlot(list, [index, subIndex], subItem));
        } else {
          this.addSlot(list, index, item);
        }
      });
    } else if (list['@list'] != null) {
      // Singleton list item at position zero
      this.addSlot(list, 0, list['@list']);
    }
    // Degrade the list to a plain subject for further processing
    delete (<Subject>list)['@list'];
  }

  private addSlot(list: List, index: string | number | [number, number], item: SubjectPropertyObject) {
    let indexKey: string;
    if (typeof index === 'string') {
      index ||= genVarName(); // We need the var name now to generate sub-var names
      indexKey = subVar(index, 'listKey');
    } else {
      // If the index is specified numerically in query mode, the value will be
      // matched with the slot index, and the key index can be ?any
      indexKey = this.query ? any() : toIndexDataUrl(index);
    }
    let slot: Subject;
    if (typeof item == 'object' && '@item' in item) {
      // The item is already a slot (with an @item key)
      item[jrql.item] = item['@item'];
      delete item['@item'];
      slot = item;
    } else {
      slot = { [jrql.item]: item };
    }
    // If the index is a variable, generate the slot id variable
    if (typeof index == 'string' && !('@id' in slot))
      slot['@id'] = subVar(index, 'slotId');
    // Slot index is partially redundant with index key in list (for insert)
    slot[jrql.index] = typeof index == 'string' ? `?${index}` :
      // Sub-index is not represented in index property
      isArray(index) ? index[0] : index;

    includeValue(list, indexKey, slot);
  }
}

export type SubVarName = 'listKey' | 'slotId';

function subVar(varName: string, subVarName: SubVarName) {
  return `?${varName}__${subVarName}`;
}

const MATCH_SUBVARNAME = new RegExp(`^\\?(${VARNAME})__(listKey|slotId)$`);
export function matchSubVarName(fullVarName: string): [string, SubVarName] | [] {
  const match = MATCH_SUBVARNAME.exec(fullVarName);
  return match != null ? [match[1], match[2] as SubVarName] : [];
}

function hideVar(token: string, vars?: Set<string>): string {
  const name = matchVar(token);
  // Allow anonymous variables as '?'
  return name === '' ? genHiddenVar(vars) : name ? hiddenVar(name, vars) : token;
}

const MATCH_VAR = new RegExp(`^\\?(${VARNAME})$`);
export function matchVar(token: string): string | undefined {
  return token === '?' ? '' : MATCH_VAR.exec(token)?.[1];
}

export const MATCH_HIDDEN_VAR = new RegExp(`^${hiddenVar('(' + VARNAME + ')')}$`);
function matchHiddenVar(value: string): string | undefined {
  return MATCH_HIDDEN_VAR.exec(value)?.[1];
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

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (isArray(results) ? results.includes(key) : results === key);
}

export function toIndexNumber(indexKey: any): number | [number, number] | undefined {
  if (indexKey != null && indexKey !== '') {
    if (isNaturalNumber(indexKey)) // ℕ
      return indexKey;
    switch (typeof indexKey) {
      case 'string':
        return toIndexNumber(indexKey.startsWith('data') ?
          dataUrlData(indexKey, 'text/plain') : // 'data:,ℕ' or 'data:,ℕ,ℕ'
          indexKey.includes(',') ?
            indexKey.split(',').map(Number) : // 'ℕ,ℕ'
            Number(indexKey)); // 'ℕ'
      case 'object': // [ℕ,ℕ]
        if (isArray(indexKey) &&
          indexKey.length == 2 &&
          indexKey.every(isNaturalNumber))
          return indexKey as [number, number];
    }
  }
}

export function toIndexDataUrl(index: number | [number, number]): Url {
  return `data:,${array(index).map(i => i.toFixed(0)).join(',')}`;
}

const isNaturalNumber = (n: any) =>
  typeof n == 'number' && Number.isSafeInteger(n) && n >= 0;