import {
  isList, isPropertyObject, isSet, isSubjectObject, List, Reference, Slot, Subject,
  SubjectPropertyObject, Value
} from './jrql-support';
import { isArray, isNaturalNumber } from './engine/util';
import {
  clone, compareValues, expandValue, getValues, hasProperty, hasValue, minimiseValue, minimiseValues
} from './engine/jsonld';
import { array } from './util';
import { getPatch } from 'fast-array-diff';

export { compareValues, getValues };

/** @internal */
export abstract class SubjectPropertyValues<S extends Subject = Subject> {
  static for<S extends Subject>(
    subject: S,
    property: string & keyof S,
    deepUpdater?: (values: Iterable<any>) => void
  ): SubjectPropertyValues<S> {
    if (isList(subject) && property === '@list')
      return new SubjectPropertyList(subject, deepUpdater);
    else
      return new SubjectPropertySet(subject, property, deepUpdater);
  }

  protected constructor(
    readonly subject: S,
    readonly property: string & keyof S,
    readonly deepUpdater?: (values: any[], unref?: boolean) => void
  ) {
    if (subject[property] != null && !isPropertyObject(this.property, subject[property]))
      throw new RangeError('Invalid property');
  }

  get object(): SubjectPropertyObject {
    return <SubjectPropertyObject>this.subject[this.property];
  }

  abstract get values(): any[];

  minimalSubject(values = this.values): Subject | undefined {
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
    const rtn: Subject = {};
    if (this.subject['@id'] != null)
      rtn['@id'] = this.subject['@id'];
    if (this.subject[this.property] != null)
      rtn[this.property] = clone(this.subject[this.property]);
    return <S>rtn;
  }

  abstract clone(): SubjectPropertyValues<S>;

  abstract delete(...values: any[]): this;

  abstract insert(...values: any[]): this;

  abstract update(deletes: Subject | undefined, inserts: Subject | undefined): this;

  abstract exists(value?: any): boolean;

  abstract diff(oldValues: any[]): { deletes?: Subject; inserts?: Subject };

  toString() {
    return `${this.subject['@id']} ${this.property}: ${this.values}`;
  }
}

/** @internal */
export class SubjectPropertySet<S extends Subject = Subject> extends SubjectPropertyValues<S> {
  private readonly prior: 'atom' | 'array' | 'set';
  private readonly configurable: boolean;

  constructor(
    subject: S,
    property: string & keyof S,
    deepUpdater?: (values: Iterable<any>) => void
  ) {
    super(subject, property, deepUpdater);
    if (isArray(this.object))
      this.prior = 'array';
    else if (isSet(this.object))
      this.prior = 'set';
    else
      this.prior = 'atom';
    this.configurable = Object.getOwnPropertyDescriptor(subject, property)?.configurable ?? false;
  }

  get values() {
    return getValues(this.subject, this.property);
  }

  clone() {
    return new SubjectPropertySet(this.cloneSubject(), this.property, this.deepUpdater);
  }

  insert(...values: any[]) {
    // Favour a subject over an existing reference
    return this._update(values.filter(isSubjectObject), values);
  }

  delete(...values: any[]) {
    return this._update(values, []);
  }

  update(deletes: Subject | undefined, inserts: Subject | undefined) {
    return this._update(
      getValues(deletes ?? {}, this.property),
      getValues(inserts ?? {}, this.property)
    );
  }

  private _update(deletes: any[], inserts: any[]) {
    const oldValues = this.values;
    let values = SubjectPropertySet.minus(oldValues, deletes);
    values = SubjectPropertySet.union(values, inserts);
    // Apply deep updates to the final values
    this.deepUpdater?.(values);
    // Do not call setter if nothing has changed
    if (oldValues !== values) {
      if (this.prior == 'set') {
        // A JSON-LD Set cannot have any other key than @set
        // @ts-ignore Typescript can't tell what the value type should be
        this.subject[this.property] = { '@set': values };
      } else {
        // Per contract of updateSubject, this always L-value assigns (no pushing)
        this.subject[this.property] = values.length === 0 ? [] : // See next
          // Properties which were not an array before get collapsed
          values.length === 1 && this.prior == 'atom' ? values[0] : values;
        if (values.length === 0 && this.configurable)
          delete this.subject[this.property];
      }
    }
    return this;
  }

  exists(value?: any): boolean {
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

  deletes(oldValues: any[]) {
    return SubjectPropertySet.minus(oldValues, this.values);
  }

  inserts(oldValues: any[]) {
    return SubjectPropertySet.minus(this.values, oldValues);
  }

  diff(oldValues: any[]) {
    return {
      deletes: this.minimalSubject(this.deletes(oldValues)),
      inserts: this.minimalSubject(this.inserts(oldValues))
    };
  }

  /** @returns `values` if nothing has changed */
  private static union(values: any[], unionValues: any[]): any[] {
    const newValues = SubjectPropertySet.minus(unionValues, values);
    return newValues.length > 0 ? values.concat(newValues) : values;
  }

  /** @returns `values` if nothing has changed */
  private static minus(values: any[], minusValues: any[]): any[] {
    if (values.length === 0 || minusValues.length === 0)
      return values;
    const filtered = values.filter(value => !minusValues.some(
      minusValue => compareValues(value, minusValue)));
    return filtered.length === values.length ? values : filtered;
  }
}

/** @internal */
export class SubjectPropertyList<S extends List> extends SubjectPropertyValues<S> {
  constructor(subject: S, deepUpdater?: (values: Iterable<any>) => void) {
    super(subject, '@list', deepUpdater);
  }

  clone() {
    return new SubjectPropertyList(this.cloneSubject(), this.deepUpdater);
  }

  get values(): any[] {
    return Object.assign([], this.subject['@list']);
  }

  exists(value: any): boolean {
    return this.values.findIndex(v => compareValues(v, value)) > -1;
  }

  delete(...values: any): this {
    const myValues = this.values;
    const entries: { [key: number]: any } = {};
    for (let value of values) {
      for (let i = 0; i < myValues.length; i++)
        if (compareValues(value, myValues[i]))
          entries[i] = value;
    }
    return this.update({ '@list': entries }, {});
  }

  insert(index: number, ...values: any): this {
    return this.update({}, { '@list': { [index]: values } });
  }

  update(deletes: Subject | undefined, inserts: Subject | undefined): this {
    if (isListUpdate(deletes) || isListUpdate(inserts)) {
      this.updateListIndexes(
        isListUpdate(deletes) ? deletes['@list'] : {},
        isListUpdate(inserts) ? inserts['@list'] : {}
      );
    }
    this.deepUpdater?.(this.values);
    return this;
  }

  diff(oldValues: any[]): { deletes?: List; inserts?: List } {
    const deletes: List['@list'] = {}, inserts: List['@list'] = {};
    for (let { type, oldPos, items } of getPatch(oldValues, this.values, compareValues)) {
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
    const splices: { deleteCount: number, items?: any[] }[] = []; // Sparse
    for (let i in deletes)
      splices[i] = { deleteCount: 1 };
    for (let i in inserts) {
      // List updates are always expressed with identified slots, but our list
      // is assumed to contain direct items, not slots
      const slots = array(inserts[i]);
      this.deepUpdater?.(slots, true);
      (splices[i] ??= { deleteCount: 0 }).items =
        slots.map((slot: Slot) => slot['@item']);
    }
    let deleteCount = 0, items: any[] = [];
    for (let i of Object.keys(splices).reverse().map(Number)) {
      deleteCount += splices[i].deleteCount;
      items.unshift(...splices[i].items ?? []);
      if (!(i - 1 in splices) || !splices[i - 1].deleteCount) {
        splice.call(list, i, deleteCount, ...items);
        deleteCount = 0;
        items = [];
      }
    }
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
  subject: Subject,
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
 * wildcard checks for any value at all. If an empty array, always returns `true`
 * @category Utility
 */
export function includesValue(
  subject: Subject,
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
 */
export function sortValues(property: string, values: Value[]) {
  return values.sort((v1, v2) => {
    function rawCompare(r1: any, r2: any) {
      return r1 < r2 ? -1 : r1 > r2 ? 1 : 0;
    }
    const { type: t1, raw: r1 } = expandValue(property, v1);
    const { type: t2, raw: r2 } = expandValue(property, v2);
    return t1 === '@id' || t1 === '@vocab' ?
      t2 === '@id' || t2 === '@vocab' ? rawCompare(r1, r2) : 1 :
      t2 === '@id' || t2 === '@vocab' ? -1 : rawCompare(r1, r2);
  });
}
