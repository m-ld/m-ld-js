import { Url } from 'jsonld/jsonld-spec';
import { Binding } from 'quadstore';
import { Quad, Quad_Object, Quad_Subject, Term } from 'rdf-js';
import { Graph } from '.';
import { any, array } from '../..';
import {
  Context, Subject, Result, Value, isValueObject, isReference,
  isSet, SubjectPropertyObject, isPropertyObject, Atom
} from '../../jrql-support';
import {
  activeCtx, compactIri, dataUrlData, jsonToRdf, expandTerm, canonicalDouble
} from '../jsonld';
import { inPosition } from '../quads';
import { jrql, rdf, xs } from '../../ns';
import { SubjectGraph } from '../SubjectGraph';
import { ActiveContext, getContextValue } from 'jsonld/lib/context';
import { isString, isBoolean, isDouble, isNumber } from 'jsonld/lib/types';
import { lazy } from '../util';
const { isArray } = Array;

const PN_CHARS_BASE = `[A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD]|[\uD800-\uDB7F][\uDC00-\uDFFF]`;
const PN_CHARS_U = `(?:${PN_CHARS_BASE}|_)`;
const VARNAME = `(?:${PN_CHARS_U}|[0-9])(?:${PN_CHARS_U}|[0-9]|\u00B7|[\u0300-\u036F\u203F-\u2040])*`;

export interface JrqlQuadsOptions {
  /** Whether this will be used to match quads or insert them */
  query: boolean;
  /** The variable names found (sans '?') */
  vars?: Set<string>;
}

export class JrqlQuads {
  constructor(
    readonly graph: Graph) {
  }

  async solutionSubject(results: Result[] | Result, binding: Binding, context: Context) {
    const solutionId = this.graph.blankNode();
    const pseudoPropertyQuads = Object.entries(binding).map(([variable, term]) => this.graph.quad(
      solutionId,
      this.graph.namedNode(jrql.hiddenVar(variable.slice(1))),
      inPosition('object', term)));
    // Construct quads that represent the solution's variable values
    const subject = await this.toSubject(pseudoPropertyQuads, [/* TODO: list-items */], context);
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
    return [...new QuadProcessor(opts, await activeCtx(context), this.graph).process(null, null, g)];
  }

  /**
   * @param propertyQuads subject-property-value quads
   * @param listItemQuads subject-index-item quads for list-like subjects
   * @returns a single subject compacted against the given context
   */
  async toSubject(
    propertyQuads: Quad[], listItemQuads: Quad[], context: Context): Promise<Subject> {
    const subjects = await SubjectGraph.fromRDF(propertyQuads).withContext(context);
    const subject = { ...subjects[0] };
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

  genSubValue(parentValue: Term, subVarName: SubVarName) {
    switch (subVarName) {
      case 'listKey':
        // Generating a data URL for the index key
        return this.graph.namedNode(toIndexDataUrl(Number(parentValue.value)));
      case 'slotId':
        // Index exists, so a slot can be made
        return this.graph.skolem();
    }
  }

  async toObjectTerms(
    expr: any, context: Context): Promise<Quad_Object[]> {
    return (await jsonToRdf({
      '@context': context,
      [jrql.blank]: expr
    }, this.graph)).map(quad => quad.object);
  }
}

class QuadProcessor {
  readonly query: boolean;
  readonly vars?: Set<string>;

  constructor(
    { query, vars }: JrqlQuadsOptions,
    readonly ctx: ActiveContext,
    readonly graph: Graph) {
    this.query = query;
    this.vars = vars;
  }

  *process(
    outer: Quad_Subject | null,
    property: string | null,
    object: SubjectPropertyObject): Iterable<Quad> {
    // TODO: property is @list in context
    for (let value of array(object)) {
      if (isArray(value)) {
        // Nested array is flattened
        yield* this.process(outer, property, value);
      } else if (isSet(value)) {
        // @set is elided
        yield* this.process(outer, property, value['@set']);
      } else if (typeof value === 'object' && !isValueObject(value)) {
        // TODO: @json type, nested @context object
        // If this is a Reference, we treat it as a Subject
        const subject: Subject = value as Subject;
        const sid = subject['@id'] ? this.expandNode(subject['@id']) :
          // Anonymous query subjects => blank node subject (match any) or skolem
          this.query ? this.genVar() : this.graph.skolem();

        if (outer != null && property != null)
          // Yield the outer quad referencing this subject
          yield this.graph.quad(outer, this.predicate(property), sid);
        else if (this.query && isReference(subject))
          // References at top level => implicit wildcard p-o
          yield this.graph.quad(sid, this.genVar(), this.genVar());

        // Process predicates and objects
        for (let [property, value] of Object.entries(subject))
          if (isPropertyObject(property, value))
            if (property === '@list')
              yield* this.expandListSlots(sid, value);
            else
              yield* this.process(sid, property, value);

      } else if (outer != null && property != null) {
        // This is an atom, so yield one quad
        yield this.graph.quad(outer, this.predicate(property),
          this.atomObject(property, value));
        // TODO: What if the property expands to a keyword in the context?
      } else {
        throw new Error('Cannot process top-level value');
      }
    }
  }

  private *expandListSlots(lid: Quad_Subject, list: SubjectPropertyObject): Iterable<Quad> {
    // Normalise explicit list objects: expand fully to slots
    if (typeof list === 'object') {
      // This handles arrays as well as hashes
      // Normalise indexes to data URLs (throw if not convertible)
      for (let [indexKey, item] of Object.entries(list)) {
        // Provided index is either a variable (string) or an index number
        const index = matchVar(indexKey) ?? toIndexNumber(indexKey);
        if (index == null)
          throw new Error(`List index ${indexKey} is not a variable or number data`);
        // Check for inserting multiple sub-items at one index
        if (typeof index != 'string' && !this.query &&
          !isArray(index) && isArray(item) && !isArray(list)) {
          for (let subIndex = 0; subIndex < item.length; subIndex++)
            yield* this.addSlot(lid, [index, subIndex], item[subIndex]);
        } else {
          yield* this.addSlot(lid, index, item);
        }
      }
    } else {
      // Singleton list item at position zero
      yield* this.addSlot(lid, 0, list);
    }
  }

  private *addSlot(lid: Quad_Subject,
    index: string | number | [number, number],
    item: SubjectPropertyObject): Iterable<Quad> {
    const slot = this.asSlot(item);
    let indexKey: string;
    if (typeof index === 'string') {
      // Index is a variable
      index ||= this.genVarName(); // We need the var name now to generate sub-var names
      indexKey = subVar(index, 'listKey');
      // Generate the slot id variable if not available
      if (!('@id' in slot))
        slot['@id'] = subVar(index, 'slotId');
    } else if (!this.query) {
      // Inserting at a numeric index
      indexKey = toIndexDataUrl(index);
    } else {
      // Index is specified numerically in query mode. The value will be matched
      // with the slot index below, and the key index with the slot ID, if present
      const slotVarName = slot['@id'] != null && matchVar(slot['@id']);
      indexKey = slotVarName ? subVar(slotVarName, 'listKey') : any();
    }
    // Slot index is never asserted, only entailed
    if (this.query)
      slot[jrql.index] = typeof index == 'string' ? `?${index}` :
        // Sub-index should never exist for a query
        isArray(index) ? index[0] : index;
    // This will yield the index key as a property, as well as the slot
    yield* this.process(lid, indexKey, slot);
  }

  /** @returns a mutable proto-slot object */
  private asSlot(item: SubjectPropertyObject): Subject {
    if (typeof item == 'object' && '@item' in item) {
      // The item is already a slot (with an @item key)
      const slot: Subject = { ...item, [jrql.item]: item['@item'] };
      delete slot['@item'];
      return slot;
    } else {
      // A nested list is a nested list (not flattened or a set)
      return { [jrql.item]: isArray(item) ? { '@list': item } : item };
    }
  }

  private matchVar(term: string) {
    const varName = matchVar(term);
    if (varName != null) {
      if (!varName)
        // Allow anonymous variables as '?'
        return this.genVar();
      this.vars?.add(varName);
      return this.graph.variable(varName);
    }
  }

  private predicate = lazy(property =>
    property === '@type' ? this.graph.namedNode(rdf.type) : this.expandNode(property, true));

  private expandNode(term: string, vocab = false) {
    return this.matchVar(term) ?? this.graph.namedNode(expandTerm(term, this.ctx, { vocab }));
  }

  private genVarName() {
    const varName = any().slice(1);
    this.vars?.add(varName);
    return varName;
  }

  private genVar() {
    return this.graph.variable(this.genVarName());
  }

  private atomObject(property: string, value: Atom): Quad_Object {
    if (isString(value)) {
      const variable = this.matchVar(value);
      if (variable != null)
        return variable;
    }
    let type: string | null = null, language: string | null = null;
    if (isValueObject(value)) {
      if (value['@type'])
        type = expandTerm(value['@type'], this.ctx);
      language = value['@language'] ?? null;
      value = value['@value'];
    }
    if (type == null)
      type = getContextValue(this.ctx, property, '@type');

    if (isString(value)) {
      if (property === '@type' || type === '@id' || type === '@vocab')
        return this.expandNode(value, type === '@vocab');
      language = getContextValue(this.ctx, property, '@language');
      if (language != null)
        return this.graph.literal(value, language);
      if (type === xs.double)
        value = canonicalDouble(parseFloat(value));
    } else if (isBoolean(value)) {
      value = value.toString();
      type ??= xs.boolean;
    } else if (isNumber(value)) {
      if (isDouble(value)) {
        value = canonicalDouble(value);
        type ??= xs.double;
      } else {
        value = value.toFixed(0);
        type ??= xs.integer;
      }
    }

    if (type && type !== '@none')
      return this.graph.literal(value, this.graph.namedNode(type));
    else
      return this.graph.literal(value);
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

const MATCH_VAR = new RegExp(`^\\?(${VARNAME})$`);
export function matchVar(token: string): string | undefined {
  return token === '?' ? '' : MATCH_VAR.exec(token)?.[1];
}

export const MATCH_HIDDEN_VAR = new RegExp(`^${jrql.hiddenVar('(' + VARNAME + ')')}$`);
function matchHiddenVar(value: string): string | undefined {
  return MATCH_HIDDEN_VAR.exec(value)?.[1];
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