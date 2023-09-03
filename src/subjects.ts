import {
  Constraint, Expression, isList, isPropertyObject, isSet, isSubjectObject, isTextSplice, List,
  Reference, Slot, Subject, SubjectPropertyObject, TextSplice, Value
} from './jrql-support';
import { isArray, isNaturalNumber, toJSON } from './engine/util';
import {
  clone, compareValues, expandValue, getValues, hasProperty, hasValue, minimiseValue, minimiseValues
} from './engine/jsonld';
import { array } from './util';
import { getPatch } from 'fast-array-diff';
import { IndexSet } from './engine/indices';

export { compareValues, getValues };

/**
 * Subject utilities are generally tolerant of Javascript objects that are not
 * quite Subjects; for example, they may be various kinds of proxies.
 * @category Utility
 */
export type SubjectLike = Record<string, unknown>;

/** @internal */
export abstract class SubjectPropertyValues<S extends SubjectLike = Subject> {
  static for<S extends SubjectLike>(
    subject: S,
    property: string & keyof S,
    deepUpdater?: (values: Iterable<unknown>) => void,
    ignoreUnsupported = true
  ): SubjectPropertyValues<S> {
    if (isPropertyObject(property, subject) && isList(subject) && property === '@list')
      return new SubjectPropertyList(subject, deepUpdater, ignoreUnsupported);
    else
      return new SubjectPropertySet(subject, property, deepUpdater, ignoreUnsupported);
  }

  protected constructor(
    readonly subject: S,
    readonly property: string & keyof S,
    readonly deepUpdater?: (values: Iterable<unknown>, unref?: boolean) => void,
    readonly ignoreUnsupported = true
  ) {
    if (subject[property] != null && !isPropertyObject(this.property, subject[property]))
      throw new RangeError('Invalid property');
  }

  get object(): SubjectPropertyObject {
    return <SubjectPropertyObject>this.subject[this.property];
  }

  abstract values(): Value[];

  minimalSubject(values = this.values()): Subject | undefined {
    if (values.length) {
      const rtn = { [this.property]: minimiseValues(values) };
      if (this.subject['@id'] != null)
        rtn['@id'] = this.subject['@id'];
      return rtn;
    }
  }

  /**
   * Clones only the relevant property from our subject; intended for use in
   * {@link clone}. The return is cheeky, since the type may be restricted.
   */
  protected cloneSubject(): S {
    const rtn: SubjectLike = {};
    if (this.subject['@id'] != null)
      rtn['@id'] = this.subject['@id'];
    if (this.subject[this.property] != null)
      rtn[this.property] = clone(this.subject[this.property]);
    return <S>rtn;
  }

  abstract clone(): SubjectPropertyValues<S>;

  abstract delete(...values: Value[]): this;

  abstract insert(...values: Value[]): this;

  abstract update(
    deletes: Subject | undefined,
    inserts: Subject | undefined,
    updates?: Subject
  ): this;

  abstract exists(value?: unknown): boolean;

  abstract diff(oldValues: unknown[]): { deletes?: Subject; inserts?: Subject };

  toString() {
    return `${this.subject['@id']} ${this.property}: ${this.values()}`;
  }
}

/** @internal */
export class SubjectPropertySet<S extends SubjectLike = Subject> extends SubjectPropertyValues<S> {
  private readonly prior: 'atom' | 'array' | 'set';
  private readonly configurable: boolean;

  constructor(
    subject: S,
    property: string & keyof S,
    deepUpdater?: (values: Iterable<unknown>) => void,
    ignoreUnsupported = true
  ) {
    super(subject, property, deepUpdater, ignoreUnsupported);
    if (isArray(this.object))
      this.prior = 'array';
    else if (isSet(this.object))
      this.prior = 'set';
    else
      this.prior = 'atom';
    this.configurable = Object.getOwnPropertyDescriptor(subject, property)?.configurable ?? false;
  }

  values() {
    return getValues(this.subject, this.property);
  }

  clone() {
    return new SubjectPropertySet(this.cloneSubject(), this.property, this.deepUpdater);
  }

  insert(...values: Value[]) {
    // Favour a subject over an existing reference
    return this._update(values.filter(isSubjectObject), values);
  }

  delete(...values: Value[]) {
    return this._update(values, []);
  }

  update(
    deletes: Subject | undefined,
    inserts: Subject | undefined,
    updates: Subject | undefined
  ) {
    return this._update(
      getValues(deletes ?? {}, this.property),
      getValues(inserts ?? {}, this.property),
      getValues(updates ?? {}, this.property)
    );
  }

  private _update(
    deletes: Value[],
    inserts: Value[],
    updates?: Constraint[]
  ) {
    // Only construct a value set if needed for deletes/inserts, because
    // indexing can be expensive, e.g. large text documents
    let values: Value[];
    let changed = false;
    if (deletes.length || inserts.length) {
      const valueSet = new ValueSet(this.property, this.values());
      // Apply delete/inserts
      for (let value of deletes)
        changed = valueSet.delete(value) || changed;
      for (let value of inserts)
        changed = valueSet.add(value) || changed;
      values = [...valueSet];
    } else {
      values = this.values();
    }
    // Apply supported updates. Note that @update in a MeldUpdate only applies
    // to shared data types, and so reference semantics apply for the value set
    changed = this.applyUpdates(values, updates) || changed;
    // Apply deep updates to the final values
    this.deepUpdater?.(values);
    // Do not call setter if nothing has changed
    if (changed) {
      if (this.prior == 'set') {
        // A JSON-LD Set cannot have unknown other key than @set
        this.setPropertyValue({ '@set': values });
      } else {
        this.setPropertyValue(values.length === 0 ? [] : // See next
          // Properties which were not an array before get collapsed
          values.length === 1 && this.prior == 'atom' ? [...values][0] : values);
        if (values.length === 0 && this.configurable)
          delete this.subject[this.property];
      }
    }
    return this;
  }

  private setPropertyValue(propertyValue: any) {
    // Per contract of updateSubject, this always L-value assigns (no pushing)
    // NOTE Typescript can't tell what the value type should be
    this.subject[this.property] = propertyValue;
  }

  private applyUpdates(values: Value[], updates?: Constraint[]): boolean {
    if (updates == null)
      return false;
    // Partition the updates by operator
    const byOperator: { [operator: string]: Operation | undefined } = {};
    for (let update of updates) {
      for (let [operator, expression] of Object.entries(update))
        (byOperator[operator] ??= this.getOperation(operator))?.addArguments(expression);
    }
    let changed = false;
    for (let operation of Object.values(byOperator))
      changed = operation?.applyTo(values) || changed;
    return changed;
  }

  exists(value?: unknown): boolean {
    if (isArray(value)) {
      return value.every(v => this.exists(v));
    } else if (value != null) {
      const object = this.subject[this.property];
      if (!isPropertyObject(this.property, object))
        return false;
      else if (isSet(object))
        return hasValue(object, '@set', value);
      else
        return hasValue(this.subject, this.property, value);
    } else {
      return hasProperty(this.subject, this.property);
    }
  }

  deletes(oldValues: Value[]) {
    const values = new ValueSet(this.property, oldValues);
    values.deleteAll(this.values());
    return [...values];
  }

  inserts(oldValues: Value[]) {
    const values = new ValueSet(this.property, this.values());
    values.deleteAll(oldValues);
    return [...values];
  }

  diff(oldValues: Value[]) {
    return {
      deletes: this.minimalSubject(this.deletes(oldValues)),
      inserts: this.minimalSubject(this.inserts(oldValues))
    };
  }

  getOperation(operator: string): Operation | undefined {
    switch (operator) {
      case '@plus':
        return new Plus();
      case '@splice':
        return new Splice();
      default:
        if (!this.ignoreUnsupported)
          throw new RangeError(`Unsupported operator ${operator}`);
    }
  }
}

class ValueSet extends IndexSet<Value> {
  constructor(
    readonly property: string,
    ts?: Iterable<Value>
  ) {
    super();
    this.addAll(ts); // Must be after super so property member exists
  }

  construct(ts: Iterable<Value> | undefined): IndexSet<Value> {
    return new ValueSet(this.property, ts);
  }

  getIndex(value: Value): string {
    const { type, canonical } =
      expandValue(this.property, toJSON(minimiseValue(value)));
    return type + canonical;
  }
}

/** @internal */
export class SubjectPropertyList<S extends List> extends SubjectPropertyValues<S> {
  constructor(
    subject: S,
    deepUpdater?: (values: Iterable<unknown>) => void,
    ignoreUnsupported = true
  ) {
    super(subject, '@list', deepUpdater, ignoreUnsupported);
  }

  clone() {
    return new SubjectPropertyList(this.cloneSubject(), this.deepUpdater);
  }

  values(): Value[] {
    return Object.assign([], this.subject['@list']);
  }

  exists(value: Value): boolean {
    return this.values().findIndex(v => compareValues(v, value)) > -1;
  }

  delete(...values: Value[]): this {
    const myValues = this.values();
    const entries: { [key: number]: Value } = {};
    for (let value of values) {
      for (let i = 0; i < myValues.length; i++)
        if (compareValues(value, myValues[i]))
          entries[i] = value;
    }
    return this.update({ '@list': entries }, {});
  }

  insert(index: number, ...values: Value[]): this {
    return this.update({}, { '@list': { [index]: values } });
  }

  update(
    deletes: Subject | undefined,
    inserts: Subject | undefined,
    updates?: Subject
  ): this {
    if (updates != null && !this.ignoreUnsupported)
      throw new RangeError('Unexpected shared data update for a list');
    if (isListUpdate(deletes) || isListUpdate(inserts)) {
      this.updateListIndexes(
        isListUpdate(deletes) ? deletes['@list'] : {},
        isListUpdate(inserts) ? inserts['@list'] : {}
      );
    }
    this.deepUpdater?.(this.values());
    return this;
  }

  diff(oldValues: unknown[]): { deletes?: List; inserts?: List } {
    const deletes: List['@list'] = {}, inserts: List['@list'] = {};
    for (let { type, oldPos, items } of getPatch(oldValues, this.values(), compareValues)) {
      if (type === 'remove') {
        for (let value of items)
          deletes[oldPos++] = minimiseValue(value);
      } else {
        inserts[oldPos] = minimiseValues(items);
      }
    }
    return {
      deletes: this.minimalUpdate(deletes),
      inserts: this.minimalUpdate(inserts)
    };
  }

  private minimalUpdate(update: List['@list']) {
    if (Object.keys(update).length)
      return { '@id': this.subject['@id'], '@list': update };
  }

  private updateListIndexes(deletes: List['@list'], inserts: List['@list']) {
    const list = this.subject['@list'];
    const splice = typeof list.splice == 'function' ? list.splice : (() => {
      // Array splice operation must have a length field to behave
      if (!('length' in list)) {
        const maxIndex = Math.max(...Object.keys(list).map(Number).filter(isNaturalNumber));
        list.length = isFinite(maxIndex) ? maxIndex + 1 : 0;
      }
      return [].splice;
    })();
    if (isArray(inserts)) {
      // Array indicates this is a full read of the list contents
      splice.call(list, 0, list.length, ...inserts);
    } else {
      const splices = new IterableSplices;
      for (let i in deletes)
        splices.ops[i] = { deleteCount: 1 };
      for (let i in inserts) {
        // List updates are always expressed with identified slots, but our list
        // is assumed to contain direct items, not slots
        const slots = array(inserts[i]);
        this.deepUpdater?.(slots, true);
        (splices.ops[Number(i)] ??= { deleteCount: 0 }).items =
          slots.map((slot: Slot) => slot['@item']);
      }
      splices.apply((index, deleteCount, items) =>
        splice.call(list, index, deleteCount, ...items));
    }
  }
}

abstract class Splices<Items> {
  /** A sparse array of concurrent splice operations for every position */
  readonly ops: { deleteCount: number; items?: Items }[] = [];

  /** Create a working set combining given arguments */
  protected abstract working(items1?: Items, items2?: Items): Items;

  apply(splice: (index: number, deleteCount: number, items: Items) => void) {
    let deleteCount = 0, items = this.working();
    for (let i of Object.keys(this.ops).reverse().map(Number)) {
      deleteCount += this.ops[i].deleteCount;
      items = this.working(this.ops[i].items, items);
      if (!(i - 1 in this.ops) || !this.ops[i - 1].deleteCount) {
        splice(i, deleteCount, items);
        deleteCount = 0;
        items = this.working();
      }
    }
  }
}

class IterableSplices extends Splices<Iterable<unknown>> {
  protected working(items1?: Iterable<unknown>, items2?: Iterable<unknown>) {
    return [...items1 ?? [], ...items2 ?? []];
  }
}

class StringSplices extends Splices<string> {
  protected working(items1?: string, items2?: string) {
    return (items1 ?? '') + (items2 ?? '');
  }
}

/** @internal */
function isListUpdate(updatePart?: Subject): updatePart is List {
  return updatePart != null && isList(updatePart);
}

/**
 * Includes the given value in the Subject property, respecting **m-ld** data
 * semantics by expanding the property to an array, if necessary.
 *
 * @param subject the subject to add the value to.
 * @param property the property that relates the value to the subject.
 * @param values the value to add.
 * @category Utility
 */
export function includeValues(
  subject: SubjectLike,
  property: string,
  ...values: Value[]
) {
  SubjectPropertyValues.for(subject, property).insert(...values);
}

/**
 * Determines whether the given set of subject property has the given value.
 * This method accounts for the identity semantics of {@link Reference}s and
 * {@link Subject}s.
 *
 * @param subject the subject to inspect
 * @param property the property to inspect
 * @param value the value or values to find in the set. If `undefined`, then
 * wildcard checks for unknown value at all. If an empty array, always returns `true`
 * @category Utility
 */
export function includesValue(
  subject: SubjectLike,
  property: string,
  value?: Value | Value[]
): boolean {
  return SubjectPropertyValues.for(subject, property).exists(value);
}

/**
 * A deterministic refinement of the greater-than operator used for SPARQL
 * ordering. Assumes no unbound values, blank nodes or simple literals (every
 * literal is typed).
 *
 * @see https://www.w3.org/TR/sparql11-query/#modOrderBy
 * @category Utility
 */
export function sortValues(property: string, values: Value[]) {
  return values.sort((v1, v2) => {
    function rawCompare(r1: Value, r2: Value) {
      return r1 < r2 ? -1 : r1 > r2 ? 1 : 0;
    }
    const { type: t1, raw: r1 } = expandValue(property, v1);
    const { type: t2, raw: r2 } = expandValue(property, v2);
    return t1 === '@id' || t1 === '@vocab' ?
      t2 === '@id' || t2 === '@vocab' ? rawCompare(r1, r2) : 1 :
      t2 === '@id' || t2 === '@vocab' ? -1 : rawCompare(r1, r2);
  });
}

/**
 * Generates the difference between the texts in the form of a splice suitable
 * for use with the `@splice` operator. Will throw if the splice is not
 * contiguous.
 * @category Utility
 */
export function textDiff(text1: string, text2: string): TextSplice | undefined {
  let start = -1, deleteCount = 0, content = '';
  for (let { type, oldPos: index, items } of getPatch([...text1], [...text2])) {
    if (start !== -1) {
      const currentEnd: number = start + deleteCount;
      if (index < currentEnd)
        throw new RangeError('Unexpected patch ordering');
      if (index > currentEnd) {
        // Fill up the gap with a no-op
        deleteCount += index - currentEnd;
        content += text1.substring(currentEnd, index);
      }
    } else {
      start = index;
    }
    if (type === 'add') {
      content += items.join('');
    } else if (type === 'remove')
      deleteCount += items.length;
  }
  if (start !== -1)
    return content ? [start, deleteCount, content] : [start, deleteCount];
}

/** @internal */
interface Operation {
  addArguments(args: Expression | Expression[]): void;
  /** @returns true iff a value reference was substituted */
  applyTo(values: Value[]): boolean;
}

/** @internal */
class Plus implements Operation {
  argArray: number[] = [];
  addArguments(args: Expression | Expression[]) {
    if (isArray(args) && args.length > 1)
      throw new RangeError('@plus operator requires numeric argument');
    const [rhs] = array(args);
    if (typeof rhs != 'number')
      throw new RangeError('@plus operator requires numeric argument');
    if (rhs !== 0)
      this.argArray.push(rhs);
  }
  applyTo(values: Value[]): boolean {
    if (this.argArray.length > 0 && values.length > 0) {
      for (let i = 0; i < values.length; i++) {
        const value = values[i];
        if (typeof value != 'number')
          throw new TypeError('Applying splice to a non-numeric');
        for (let arg of this.argArray)
          values[i] = value + arg;
      }
      return true; // numbers are immutable
    }
    return false;
  }
}

/** @internal */
class Splice extends StringSplices implements Operation {
  addArguments(args: Expression | Expression[]) {
    if (!isTextSplice(args))
      throw new RangeError('@splice operator requires index, deleteCount and content');
    const [index, deleteCount, items] = args;
    if (deleteCount || items)
      this.ops[index] = { deleteCount, items };
  }
  applyTo(values: Value[]): boolean {
    let changed = false;
    for (let i = 0; i < values.length; i++) {
      if (typeof values[i] == 'string') {
        this.apply((index: number, deleteCount: number, content?: string) => {
          const value = <string>values[i]; // It stays a string, TS
          values[i] = value.substring(0, index) +
            (content ?? '') + value.substring(index + deleteCount);
          changed ||= true;
        });
      } else if (typeof values[i] == 'object' && typeof (<any>values[i])?.splice == 'function') {
        // TODO: Fix typing – value is not necessarily a strict `Value`
        this.apply((<any>values[i]).splice.bind(values[i]));
        // Not 'changed' – reference semantics apply
      } else {
        throw new TypeError(`Don't know how to splice ${values[i]}`);
      }
    }
    return changed;
  }
}
