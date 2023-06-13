import { any, anyName, blank, IndirectedData, isSharedDatatype } from '../api';
import {
  Atom, Constraint, Expression, InlineConstraint, isInlineConstraint, isPropertyObject, isReference,
  isSet, isSubjectObject, Reference, Subject, SubjectPropertyObject, Variable
} from '../jrql-support';
import { expandValue, JsonldContext } from './jsonld';
import { asQueryVar, Quad, Quad_Object, Quad_Subject, RdfFactory } from './quads';
import { JRQL, RDF } from '../ns';
import { JrqlMode, ListIndex, listItems, toIndexDataUrl } from './jrql-util';
import { isArray, lazy, mapObject } from './util';
import { array } from '../util';
import { EventEmitter } from 'events';
import { Iri } from '@m-ld/jsonld';

export class SubjectQuads extends EventEmitter {
  constructor(
    readonly rdf: RdfFactory,
    readonly mode: JrqlMode,
    readonly ctx: JsonldContext,
    readonly indirectedData: IndirectedData
  ) {
    super();
  }

  /** Called with with variable names found (sans '?') */
  on(event: 'var', listener: (varName: string) => void): this;
  /** Called with inline filters found, if mode is 'match' */
  on(event: 'filter', listener: (filter: Constraint) => void): this;
  /** Called with inline bindings found, if mode is 'load' */
  on(event: 'bind', listener: (
    returnVar: Variable,
    binding: Expression,
    queryVar: Variable,
    constraint: Constraint
  ) => void): this;
  /** Called with metadata found when processing subjects */
  on(eventName: string, listener: (...args: any[]) => void): this {
    return super.on(eventName, listener);
  }

  toQuads(subjects?: Subject | Subject[]) {
    return subjects ? [...this.process(subjects)] : [];
  }

  private *process(
    object: SubjectPropertyObject,
    outer: Quad_Subject | null = null,
    property: string | null = null
  ): Iterable<Quad> {
    // TODO: property is @list in context
    for (let value of array(object))
      if (isArray(value))
        // Nested array is flattened
        yield *this.process(value, outer, property);
      else if (isSet(value))
        // @set is elided
        yield *this.process(value['@set'], outer, property);
      else if (isSubjectObject(value) || isReference(value))
        // TODO: nested @context object
        yield *this.subjectQuads(value, outer, property);
      else if (outer != null && property != null)
        // This is an atom, so yield one quad
        yield this.quad(outer, property, value);
      // TODO: What if the property expands to a keyword in the context?
      else
        throw new Error(`Cannot yield quad from top-level value: ${value}`);
  }

  private quad(
    subject: Quad_Subject,
    property: string,
    value: Atom | InlineConstraint
  ): Quad {
    const predicate = this.predicate(property);
    if (isInlineConstraint(value)) {
      const { variable, constraint } = this.inlineConstraintDetails(value);
      // The variable is the 1st parameter of the resultant constraint expression.
      const queryVar = asQueryVar(variable);
      const uncurried = mapObject(constraint, (operator, expression) => ({
        [operator]: [queryVar, ...array(expression)]
      }));
      if (this.mode === JrqlMode.match) {
        // A filter, with the variable as the object e.g. ?o > 1
        this.emit('filter', uncurried);
      } else if (this.mode === JrqlMode.load) {
        // A binding, with the return value of the expression e.g. ?o = ?x + 1
        const returnVar = this.newVar();
        this.emit('bind',
          asQueryVar(returnVar), uncurried, queryVar, constraint);
        const quad = this.rdf.quad(subject, predicate, returnVar);
        quad.before = variable;
        return quad;
      }
      // Return variable unless bound to a new one
      return this.rdf.quad(subject, predicate, variable);
    }
    const propContext = predicate.termType === 'NamedNode' ?
      { property, predicate: predicate.value } : undefined;
    return this.rdf.quad(subject, predicate, this.objectTerm(value, propContext));
  }

  private *subjectQuads(
    object: Subject | Reference,
    outer: Quad_Subject | null,
    property: string | null
  ) {
    const subject: Subject = object as Subject;
    // If this is a Reference, we treat it as a Subject
    const sid = this.subjectId(subject);

    if (outer != null && property != null)
      // Yield the outer quad referencing this subject
      yield this.rdf.quad(outer, this.predicate(property), sid);
    else if (this.mode === JrqlMode.match && isReference(subject))
      // References at top level => implicit wildcard p-o
      yield this.rdf.quad(sid, this.newVar(), this.newVar());

    // Process predicates and objects
    for (let [property, value] of Object.entries(subject))
      if (isPropertyObject(property, value))
        if (property === '@list')
          yield *this.listQuads(sid, value);
        else
          yield *this.process(value, sid, property);
  }

  private subjectId(subject: Subject) {
    if (subject['@id'] != null)
      if (subject['@id'].startsWith('_:'))
        return this.rdf.blankNode(subject['@id']);
      else
        return this.expandNode(subject['@id']);
    else if (this.mode === JrqlMode.match)
      return this.newVar();
    else if (this.mode === JrqlMode.load && this.rdf.skolem != null)
      return this.rdf.skolem();
    else
      return this.rdf.blankNode(blank());
  }

  private *listQuads(lid: Quad_Subject, list: SubjectPropertyObject): Iterable<Quad> {
    // Normalise explicit list objects: expand fully to slots
    for (let [index, item] of listItems(list, this.mode))
      yield *this.slotQuads(lid, index, item);
  }

  private *slotQuads(
    lid: Quad_Subject,
    index: string | ListIndex,
    item: SubjectPropertyObject
  ): Iterable<Quad> {
    const slot = this.asSlot(item);
    let indexKey: string;
    if (typeof index === 'string') {
      // Index is a variable
      index ||= this.genVarName(); // We need the var name now to generate sub-var names
      indexKey = JRQL.subVar(index, 'listKey');
      // Generate the slot id variable if not available
      if (!('@id' in slot))
        slot['@id'] = JRQL.subVar(index, 'slotId');
    } else if (this.mode !== JrqlMode.match) {
      // Inserting at a numeric index
      indexKey = toIndexDataUrl(index);
    } else {
      // Index is specified numerically in match mode. The value will be matched
      // with the slot index below, and the key index with the slot ID, if present
      const slotVarName = slot['@id'] != null && JRQL.matchVar(slot['@id']);
      indexKey = slotVarName ? JRQL.subVar(slotVarName, 'listKey') : any();
    }
    // Slot index is never asserted, only entailed
    if (this.mode === JrqlMode.match)
      // Sub-index should never exist for matching
      slot['@index'] = typeof index == 'string' ? `?${index}` : index[0];
    // This will yield the index key as a property, as well as the slot
    yield *this.process(slot, lid, indexKey);
  }

  /** @returns a mutable proto-slot object */
  private asSlot(item: SubjectPropertyObject): Subject {
    if (isArray(item)) {
      // A nested list is a nested list (not flattened or a set)
      return { '@item': { '@list': item } };
    }
    if ((isSubjectObject(item) || isReference(item)) && (
      '@item' in item ||
      this.mode === JrqlMode.graph ||
      this.mode === JrqlMode.serial
    )) {
      // The item is already a slot (with an @item key)
      return { ...item };
    } else {
      return { '@item': item };
    }
  }

  private matchVar = (term: any) => {
    if (
      this.mode !== JrqlMode.graph &&
      this.mode !== JrqlMode.serial &&
      typeof term == 'string'
    ) {
      const varName = JRQL.matchVar(term);
      if (varName != null) {
        if (!varName)
          // Allow anonymous variables as '?'
          return this.newVar();
        this.emit('var', varName);
        return this.rdf.variable(varName);
      }
    }
  };

  private predicate = lazy(property => {
    switch (property) {
      case '@type':
        return this.rdf.namedNode(RDF.type);
      case '@index':
        return this.rdf.namedNode(JRQL.index);
      case '@item':
        return this.rdf.namedNode(JRQL.item);
      default:
        return this.expandNode(property, true);
    }
  });

  private expandNode(term: string, vocab = false) {
    return this.matchVar(term) ??
      this.rdf.namedNode(this.ctx.expandTerm(term, { vocab }));
  }

  private genVarName() {
    const varName = anyName();
    this.emit('var', varName);
    return varName;
  }

  newVar() {
    return this.rdf.variable(this.genVarName());
  }

  objectTerm(
    value: Atom | InlineConstraint,
    context?: { property: string, predicate: Iri }
  ): Quad_Object {
    // Note using indexer access to allow for lazy properties
    const ex = expandValue(context?.property ?? null, value, this.ctx);
    const variable = ex.id == null && this.matchVar(ex.raw);
    if (variable) {
      return variable;
    } else if (ex.type === '@id' || ex.type === '@vocab') {
      return this.rdf.namedNode(ex.canonical);
    } else if (ex.language) {
      return this.rdf.literal(ex.canonical, ex.language);
    } else if (ex.type !== '@none') {
      if (context != null) {
        const datatype = this.indirectedData?.(context.predicate, ex.type);
        const serialising = this.mode === JrqlMode.serial;
        // When serialising, shared datatype without an @id is id-only
        if (datatype != null && (!serialising || !isSharedDatatype(datatype) || ex.id)) {
          const data = serialising ?
            datatype.fromJSON?.(ex.raw) ?? ex.raw : // coming from protocol
            datatype.validate(ex.raw); // coming from the app
          return this.rdf.literal(ex.id || datatype.getDataId(data), datatype, data);
        }
      }
      return this.rdf.literal(ex.canonical, this.rdf.namedNode(ex.type));
    } else {
      throw new RangeError('Cannot construct a literal with no type');
    }
  }

  private inlineConstraintDetails(inlineConstraint: InlineConstraint) {
    if ('@value' in inlineConstraint) {
      const variable = this.matchVar(inlineConstraint['@value']);
      if (variable == null)
        throw new Error(`Invalid variable for inline constraint: ${inlineConstraint}`);
      const { '@value': _, ...constraint } = inlineConstraint;
      return { variable, constraint };
    } else {
      return { variable: this.newVar(), constraint: inlineConstraint };
    }
  }
}
