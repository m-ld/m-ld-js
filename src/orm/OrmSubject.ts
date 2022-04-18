import { isPropertyObject, Reference, Subject } from '../jrql-support';
import { GraphSubject } from '../api';
import {
  AtomType, castPropertyValue, ContainerType, normaliseValue, ValueConstructed
} from '../js-support';
import { isArray, isNaturalNumber } from '../engine/util';
import { asValues } from '../engine/jsonld';

/**
 * TODO docs, unit tests
 *
 * @experimental
 * @category Experimental
 */
export abstract class OrmSubject {
  readonly src: Subject & Reference;
  private readonly propertiesInState = new Set<string>();
  updated: Promise<this>;

  protected constructor(src: GraphSubject) {
    this.src = { '@id': src['@id'] };
    this.updated = Promise.resolve(this);
  }

  get deleted() {
    return this.propertiesInState.size === 0;
  }

  /**
   * @param src the initial graph subject – only used for its property value
   * @param property the property IRI in context
   * @param type the expected type of the property. If the type in the graph
   * cannot be cast to this type, {@link updated} will be rejected.
   * @param get called to get the value of the property
   * @param set called to set the value of the property
   * @param initValue an initial Javascript value. If this is non-null, it will
   * be used instead of the `src` property value.
   * @public because the structure of this ORM subject may not necessarily
   * reflect the graph representation – for example, an aggregation member may
   * represent data from its parent graph subject.
   */
  initSrcProperty<T, S>(
    src: GraphSubject,
    property: string,
    type: AtomType<T> | [ContainerType<T>, AtomType<S>],
    get: () => ValueConstructed<T, S>,
    set: (v: ValueConstructed<T, S>) => unknown | Promise<unknown>,
    initValue?: ValueConstructed<T, S>
  ) {
    const [topType, subType] = isArray(type) ? type : [type];
    Object.defineProperty(this.src, property, {
      get,
      set: v => {
        this.propertiesInState[asValues(v).length > 0 ? 'add' : 'delete'](property);
        try {
          const value = castPropertyValue(v, topType, subType, property);
          this.setUpdated(set(value));
        } catch (e) {
          this.setUpdated(Promise.reject(e));
        }
      },
      enumerable: true, // JSON-able
      configurable: false // Cannot delete the property
    });
    if (initValue != null)
      this.setUpdated(set(initValue));
    else
      this.src[property] = src[property]; // Invokes the setter
  }

  /**
   * @param src a List graph subject to initialise the list from
   * @param type JSON-LD type of items in the list
   * @param list an array-like object having:
   * 1. a _mutable_ length, which will be set during list updates
   * 2. numeric indexes, which will only be used to query for existence
   * @param get will be called to get JSON-LD values for items in the list
   * @param set will be called to set items in the list with JSON-LD values
   */
  initList<T>(
    src: GraphSubject,
    type: AtomType<T>,
    list: { length: number, readonly [i: number]: unknown },
    get: (i: number) => ValueConstructed<T>,
    set: (i: number, v: ValueConstructed<T>) => unknown | Promise<unknown>
  ) {
    // Source list is an array-like proxy
    // @ts-ignore
    this.src['@list'] = new Proxy(list, {
      ownKeys: list => Object.keys(list).filter(k => withArrayLikeKey(k,
        () => true, () => true, false)),
      has: (list, p: string | symbol) => withArrayLikeKey(p,
        () => true, () => true, false),
      get: (list, p: string | symbol) => withArrayLikeKey(p,
        () => list.length, i => i in list ?
          normaliseValue(get(i)) : undefined, undefined),
      set: (list, p: string | symbol, value: any) => withArrayLikeKey(p, () => {
        list.length = value;
        this.propertiesInState[list.length > 0 ? 'add' : 'delete']('@list');
        return true;
      }, i => {
        this.setUpdated(set(i, castPropertyValue(value, type)));
        return true;
      }, false)
    });
    if (isPropertyObject('@list', src['@list']))
      [].splice.call(this.src['@list'], 0, list.length, ...asValues(src['@list']));
  }

  /**
   * Called when a property is changing.
   *
   * @param result If any property is asynchronously updated, the final value
   * will only be definitively set when this promise resolves.
   */
  protected setUpdated(result: unknown | Promise<unknown>) {
    this.updated = Promise.all([this.updated, result]).then(() => this);
    this.updated.catch(() => {}); // Prevents unhandled rejection
  }
}

function withArrayLikeKey<T>(
  key: string | symbol,
  withLength: () => T,
  withIndex: (i: number) => T,
  def: T
) {
  if (key === 'length') {
    return withLength();
  } else if (typeof key != 'symbol') {
    const i = Number(key);
    if (isNaturalNumber(i))
      return withIndex(i);
  }
  return def;
}
