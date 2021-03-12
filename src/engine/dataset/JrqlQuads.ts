import { Binding } from 'quadstore';
import { Quad, Quad_Object, Quad_Subject, Term } from 'rdf-js';
import { Graph } from '.';
import { any, array, anyName } from '../..';
import {
  Context, Subject, Result, Value, isValueObject, isReference,
  isSet, SubjectPropertyObject, isPropertyObject, Atom, Variable
} from '../../jrql-support';
import { activeCtx, compactIri, jsonToRdf, expandTerm, canonicalDouble } from '../jsonld';
import { inPosition } from '../quads';
import { jrql, rdf, xs } from '../../ns';
import { SubjectGraph } from '../SubjectGraph';
import { JrqlMode, ListIndex, listItems, toIndexDataUrl } from '../jrql-util';
import { ActiveContext, getContextValue } from 'jsonld/lib/context';
import { isString, isBoolean, isDouble, isNumber } from 'jsonld/lib/types';
import { lazy, mapObject } from '../util';
const { isArray } = Array;

export interface JrqlQuadsOptions {
  mode: JrqlMode;
  /** The variable names found (sans '?') */
  vars?: Set<string>;
}

export class JrqlQuads {
  constructor(
    readonly graph: Graph) {
  }

  async solutionSubject(results: Result[] | Result, solution: Binding, context: Context) {
    const solutionId = this.graph.blankNode();
    const pseudoPropertyQuads = Object.entries(solution).map(([variable, term]) => this.graph.quad(
      solutionId,
      this.graph.namedNode(jrql.hiddenVar(variable.slice(1))),
      inPosition('object', term)));
    // Construct quads that represent the solution's variable values
    const subject = await this.toApiSubject(pseudoPropertyQuads, [/* TODO: list-items */], context);
    // Unhide the variables and strip out anything that's not selected
    return mapObject(subject, (key, value) => {
      if (key !== '@id') { // Strip out blank node identifier
        const varName = jrql.matchHiddenVar(key), newKey = (varName ? '?' + varName : key);
        if (isSelected(results, newKey))
          return { [newKey]: value };
      }
    });
  }

  async quads(g: Subject | Subject[], opts: JrqlQuadsOptions, context: Context): Promise<Quad[]> {
    return [...new QuadProcessor(opts, await activeCtx(context), this.graph).process(null, null, g)];
  }

  /**
   * @param propertyQuads subject-property-value quads
   * @param listItemQuads subject-index-item quads for list-like subjects
   * @returns a single subject compacted against the given context
   */
  async toApiSubject(
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
      const list = await this.toApiSubject(listItemQuads, [], context);
      subject['@list'] = [...indexes].map(index => <Value>list[index]);
    }
    return subject;
  }

  genSubValue(parentValue: Term, subVarName: jrql.SubVarName) {
    switch (subVarName) {
      case 'listKey':
        // Generating a data URL for the index key
        return this.graph.namedNode(toIndexDataUrl([Number(parentValue.value)]));
      case 'slotId':
        // Index exists, so a slot can be made
        return this.graph.skolem();
    }
  }

  async toObjectTerms(
    expr: any, context: Context): Promise<Quad_Object[]> {
    // TODO: use QuadProcessor instead of async jsonld toRDF
    return (await jsonToRdf({
      '@context': context,
      [jrql.blank]: expr
    }, this.graph)).map(quad => quad.object);
  }
}

class QuadProcessor {
  readonly mode: JrqlQuadsOptions['mode'];
  readonly vars: JrqlQuadsOptions['vars'];

  constructor(
    { mode, vars }: JrqlQuadsOptions,
    readonly ctx: ActiveContext,
    readonly graph: Graph) {
    this.mode = mode;
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
        const sid = this.subjectId(subject);

        if (outer != null && property != null)
          // Yield the outer quad referencing this subject
          yield this.graph.quad(outer, this.predicate(property), sid);
        else if (this.mode === 'match' && isReference(subject))
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

  private subjectId(subject: Subject) {
    if (subject['@id'] != null)
      return this.expandNode(subject['@id']);
    else
      // Anonymous query subjects => blank node subject (match any) or skolem
      switch (this.mode) {
        case 'match': return this.genVar();
        case 'load': return this.graph.skolem();
        case 'graph': throw new Error('Subject is not identified in graph');
      }
  }

  private *expandListSlots(lid: Quad_Subject, list: SubjectPropertyObject): Iterable<Quad> {
    // Normalise explicit list objects: expand fully to slots
    for (let [index, item] of listItems(list, this.mode))
      yield* this.addSlot(lid, index, item);
  }

  private *addSlot(lid: Quad_Subject,
    index: string | ListIndex,
    item: SubjectPropertyObject): Iterable<Quad> {
    const slot = this.asSlot(item);
    let indexKey: string;
    if (typeof index === 'string') {
      // Index is a variable
      index ||= this.genVarName(); // We need the var name now to generate sub-var names
      indexKey = jrql.subVar(index, 'listKey');
      // Generate the slot id variable if not available
      if (!('@id' in slot))
        slot['@id'] = jrql.subVar(index, 'slotId');
    } else if (this.mode !== 'match') {
      // Inserting at a numeric index
      indexKey = toIndexDataUrl(index);
    } else {
      // Index is specified numerically in match mode. The value will be matched
      // with the slot index below, and the key index with the slot ID, if present
      const slotVarName = slot['@id'] != null && jrql.matchVar(slot['@id']);
      indexKey = slotVarName ? jrql.subVar(slotVarName, 'listKey') : any();
    }
    // Slot index is never asserted, only entailed
    if (this.mode === 'match')
      // Sub-index should never exist for matching
      slot['@index'] = typeof index == 'string' ? `?${index}` : index[0];
    // This will yield the index key as a property, as well as the slot
    yield* this.process(lid, indexKey, slot);
  }

  /** @returns a mutable proto-slot object */
  private asSlot(item: SubjectPropertyObject): Subject {
    if (isArray(item))
      // A nested list is a nested list (not flattened or a set)
      return { '@item': { '@list': item } };
    if (typeof item == 'object' && ('@item' in item || this.mode === 'graph'))
      // The item is already a slot (with an @item key)
      return { ...item };
    else
      return { '@item': item };
  }

  private matchVar(term: string) {
    if (this.mode !== 'graph') {
      const varName = jrql.matchVar(term);
      if (varName != null) {
        if (!varName)
          // Allow anonymous variables as '?'
          return this.genVar();
        this.vars?.add(varName);
        return this.graph.variable(varName);
      }
    }
  }

  private predicate = lazy(property => {
    switch (property) {
      case '@type': return this.graph.namedNode(rdf.type);
      case '@index': return this.graph.namedNode(jrql.index);
      case '@item': return this.graph.namedNode(jrql.item);
      default: return this.expandNode(property, true);
    }
  });

  private expandNode(term: string, vocab = false) {
    return this.matchVar(term) ?? this.graph.namedNode(expandTerm(term, this.ctx, { vocab }));
  }

  private genVarName() {
    const varName = anyName();
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

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (isArray(results) ? results.includes(key) : results === key);
}
