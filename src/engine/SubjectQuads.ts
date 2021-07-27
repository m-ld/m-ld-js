import { any, anyName, blank } from '../api';
import {
  Atom, isPropertyObject, isReference, isSet, isValueObject, Reference, Subject,
  SubjectPropertyObject
} from '../jrql-support';
import { canonicalDouble, expandTerm } from './jsonld';
import { Quad, Quad_Object, Quad_Subject, RdfFactory } from './quads';
import { JRQL, RDF, XS } from '../ns';
import { JrqlMode, ListIndex, listItems, toIndexDataUrl } from './jrql-util';
import { ActiveContext, getContextValue } from 'jsonld/lib/context';
import { isBoolean, isDouble, isNumber, isString } from 'jsonld/lib/types';
import { isArray, lazy } from './util';
import { array } from '../util';

export class SubjectQuads {
  constructor(
    readonly mode: JrqlMode,
    readonly ctx: ActiveContext,
    readonly rdf: RdfFactory,
    readonly vars?: Set<string>) {
  }

  *quads(object: SubjectPropertyObject,
    outer: Quad_Subject | null = null,
    property: string | null = null): Iterable<Quad> {
    // TODO: property is @list in context
    for (let value of array(object))
      if (isArray(value))
        // Nested array is flattened
        yield* this.quads(value, outer, property);
      else if (isSet(value))
        // @set is elided
        yield* this.quads(value['@set'], outer, property);
      else if (typeof value === 'object' && !isValueObject(value))
        // TODO: @json type, nested @context object
        yield* this.subjectQuads(value, outer, property);
      else if (outer != null && property != null)
        // This is an atom, so yield one quad
        yield this.rdf.quad(outer, this.predicate(property),
          this.objectTerm(value, property));
      // TODO: What if the property expands to a keyword in the context?
      else
        throw new Error('Cannot yield quad from top-level value');
  }

  private *subjectQuads(
    object: Subject | Reference, outer: Quad_Subject | null, property: string | null) {
    const subject: Subject = object as Subject;
    // If this is a Reference, we treat it as a Subject
    const sid = this.subjectId(subject);

    if (outer != null && property != null)
      // Yield the outer quad referencing this subject
      yield this.rdf.quad(outer, this.predicate(property), sid);
    else if (this.mode === 'match' && isReference(subject))
      // References at top level => implicit wildcard p-o
      yield this.rdf.quad(sid, this.genVar(), this.genVar());

    // Process predicates and objects
    for (let [property, value] of Object.entries(subject))
      if (isPropertyObject(property, value))
        if (property === '@list')
          yield* this.listQuads(sid, value);
        else
          yield* this.quads(value, sid, property);
  }

  private subjectId(subject: Subject) {
    if (subject['@id'] != null)
      if (subject['@id'].startsWith('_:'))
        return this.rdf.blankNode(subject['@id']);
      else
        return this.expandNode(subject['@id']);
    else if (this.mode === 'match')
      return this.genVar();
    else if (this.mode === 'load' && this.rdf.skolem != null)
      return this.rdf.skolem();
    else
      return this.rdf.blankNode(blank());
  }

  private *listQuads(lid: Quad_Subject, list: SubjectPropertyObject): Iterable<Quad> {
    // Normalise explicit list objects: expand fully to slots
    for (let [index, item] of listItems(list, this.mode))
      yield* this.slotQuads(lid, index, item);
  }

  private *slotQuads(lid: Quad_Subject,
    index: string | ListIndex,
    item: SubjectPropertyObject): Iterable<Quad> {
    const slot = this.asSlot(item);
    let indexKey: string;
    if (typeof index === 'string') {
      // Index is a variable
      index ||= this.genVarName(); // We need the var name now to generate sub-var names
      indexKey = JRQL.subVar(index, 'listKey');
      // Generate the slot id variable if not available
      if (!('@id' in slot))
        slot['@id'] = JRQL.subVar(index, 'slotId');
    } else if (this.mode !== 'match') {
      // Inserting at a numeric index
      indexKey = toIndexDataUrl(index);
    } else {
      // Index is specified numerically in match mode. The value will be matched
      // with the slot index below, and the key index with the slot ID, if present
      const slotVarName = slot['@id'] != null && JRQL.matchVar(slot['@id']);
      indexKey = slotVarName ? JRQL.subVar(slotVarName, 'listKey') : any();
    }
    // Slot index is never asserted, only entailed
    if (this.mode === 'match')
      // Sub-index should never exist for matching
      slot['@index'] = typeof index == 'string' ? `?${index}` : index[0];
    // This will yield the index key as a property, as well as the slot
    yield* this.quads(slot, lid, indexKey);
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
      const varName = JRQL.matchVar(term);
      if (varName != null) {
        if (!varName)
          // Allow anonymous variables as '?'
          return this.genVar();
        this.vars?.add(varName);
        return this.rdf.variable(varName);
      }
    }
  }

  private predicate = lazy(property => {
    switch (property) {
      case '@type': return this.rdf.namedNode(RDF.type);
      case '@index': return this.rdf.namedNode(JRQL.index);
      case '@item': return this.rdf.namedNode(JRQL.item);
      default: return this.expandNode(property, true);
    }
  });

  private expandNode(term: string, vocab = false) {
    return this.matchVar(term) ?? this.rdf.namedNode(expandTerm(term, this.ctx, { vocab }));
  }

  private genVarName() {
    const varName = anyName();
    this.vars?.add(varName);
    return varName;
  }

  private genVar() {
    return this.rdf.variable(this.genVarName());
  }

  objectTerm(value: Atom | Reference, property?: string): Quad_Object {
    if (isString(value)) {
      const variable = this.matchVar(value);
      if (variable != null)
        return variable;
    } else if (isReference(value)) {
      return this.subjectId(value);
    }
    let type: string | null = null, language: string | null = null;
    if (isValueObject(value)) {
      if (value['@type'])
        type = expandTerm(value['@type'], this.ctx);
      language = value['@language'] ?? null;
      value = value['@value'];
    }
    if (type == null && property != null)
      type = getContextValue(this.ctx, property, '@type');

    if (isString(value)) {
      if (property === '@type' || type === '@id' || type === '@vocab')
        return this.expandNode(value, property === '@type' || type === '@vocab');
      if (property != null)
        language = getContextValue(this.ctx, property, '@language');
      if (language != null)
        return this.rdf.literal(value, language);
      if (type === XS.double)
        value = canonicalDouble(parseFloat(value));
    } else if (isBoolean(value)) {
      value = value.toString();
      type ??= XS.boolean;
    } else if (isNumber(value)) {
      if (isDouble(value)) {
        value = canonicalDouble(value);
        type ??= XS.double;
      } else {
        value = value.toFixed(0);
        type ??= XS.integer;
      }
    }

    if (type && type !== '@none')
      return this.rdf.literal(value, this.rdf.namedNode(type));
    else
      return this.rdf.literal(value);
  }
}
