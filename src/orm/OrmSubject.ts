import {
  isPropertyObject, List, Reference, Subject, SubjectPropertyObject, Update
} from '../jrql-support';
import { GraphSubject } from '../api';
import {
  AtomType, castPropertyValue, ContainerType, normaliseValue, ValueConstructed
} from '../js-support';
import { isArray, isNaturalNumber } from '../engine/util';
import { asValues, compareValues, minimiseValue } from '../engine/jsonld';
import { SubjectPropertyValues } from '../subjects';
import { isPromise } from 'asynciterator';
import { ConstructOrmSubject, OrmUpdating } from './OrmDomain';

/** @internal */
declare namespace PropertyAccess {
  interface Init<T, S> {
    /**
     * An initial Javascript value. If this is non-null, it will be used instead
     * of the `src` property value.
     */
    init?: ValueConstructed<T, S>;
  }

  interface Orm {
    /** Used to construct a new ORM subject from state */
    construct: ConstructOrmSubject<OrmSubject>;
    /** The current updating ORM state, used to initialise the subject */
    orm: OrmUpdating,
  }

  interface GetSet<T, S> extends Init<T, S> {
    get: () => ValueConstructed<T, S>,
    set: (v: ValueConstructed<T, S>) => unknown | Promise<unknown>
  }

  interface Local<O extends OrmSubject, T, S> extends Init<T, S> {
    /** The local property name */
    local: keyof O;
  }

  /** The graph property is the same as the local property */
  type This<T, S> = Init<T, S> | (Init<T, S> & Orm);

  /** The graph property is different from the local property */
  type Graph<O extends OrmSubject, T, S> = GetSet<T, S> | Local<O, T, S> |
    (Local<O, T, S> & Orm);

  type Any<O extends OrmSubject, T, S> = This<T, S> | Graph<O, T, S>;
}

/**
 * An Object-Resource Mapping (ORM) Subject is a Javascript class used to
 * represent graph nodes in a **m-ld** domain.
 *
 * The constructor of an ORM subject should accept a {@link GraphSubject} from
 * which derive its initial state. In the case of a new subject, which does not
 * yet exist in the domain, this will only contain an `@id` (i.e. it's a {@link
 * Reference}), so the constructor may also accept other values for initialising
 * properties.
 *
 * Local properties properties are then mapped to graph properties (edges) by
 * calling {@link initSrcProperty} in the constructor. This ensures that any
 * local changes to the object property values are tracked, and can be committed
 * to the graph later using {@link commit} (usually called for a batch of
 * subjects from {@link OrmDomain.commit}). E.g.
 *
 * ```
 * class Flintstone extends OrmSubject {
 *   name: string;
 *   height?: number;
 *
 *   constructor(src: GraphSubject) {
 *     super(src);
 *     this.initSrcProperty(src, 'name', String);
 *     this.initSrcProperty(src, 'height', [Optional, Number]);
 *   }
 * }
 * ```
 *
 * Note that an ORM subject constructor should only ever be called by the passed
 * `construct` callback of {@link OrmUpdating.get}, so that the subject is
 * correctly registered with the ORM domain.
 *
 *
 *
 * @see {@link OrmDomain}
 * @experimental
 * @category Experimental
 */
export abstract class OrmSubject {
  readonly src: Subject & Reference;
  private readonly propertiesInState = new Map<string, any[]>();
  private _updated: Promise<this> | this = this;

  protected constructor(src: GraphSubject) {
    this.src = { '@id': src['@id'] };
  }

  /** Resolves when this subject's graph is up-to-date with the domain */
  get updated() {
    return this._updated;
  }

  /** `true` if this subject does not exist in the domain */
  get deleted() {
    return this.propertiesInState.size === 0;
  }

  /**
   * Call from a subclass constructor to initialise the mapping between the
   * graph and this subject's properties.
   *
   * @param src the initial graph subject – only used for its property value
   * @param property the property IRI in context
   * @param type the expected type of the property. If the type in the graph
   * cannot be cast to this type, {@link updated} will be rejected.
   * @param access access to the subclass property.
   * @public because the structure of this ORM subject may not necessarily
   * reflect the graph representation – for example, an aggregation member may
   * represent data from its parent graph subject.
   */
  initSrcProperty<P extends string, T, S>(
    src: GraphSubject,
    property: string,
    type: AtomType<T> | [ContainerType<T>, AtomType<S>],
    access: PropertyAccess.Graph<this, T, S>
  ): void;

  /**
   * Call from a subclass constructor to initialise the mapping between the
   * graph and this subject's properties.
   *
   * @param src the initial graph subject – only used for its property value
   * @param property the property IRI in context
   * @param type the expected type of the property. If the type in the graph
   * cannot be cast to this type, {@link updated} will be rejected.
   * @param access access to the subclass property.
   * @public because the structure of this ORM subject may not necessarily
   * reflect the graph representation – for example, an aggregation member may
   * represent data from its parent graph subject.
   */
  initSrcProperty<T, S>(
    src: GraphSubject,
    property: string & keyof this,
    type: AtomType<T> | [ContainerType<T>, AtomType<S>],
    access?: PropertyAccess.This<T, S>
  ): void;

  initSrcProperty<T, S>(
    src: GraphSubject,
    property: string,
    type: AtomType<T> | [ContainerType<T>, AtomType<S>],
    access: PropertyAccess.Any<this, T, S> = {}
  ) {
    const [topType, subType] = isArray(type) ? type : [type];
    const { get, set } = this.resolveAccess(property, topType, access);
    Object.defineProperty(this.src, property, {
      get,
      set: v => {
        const srcValues = asValues(v);
        if (srcValues.length > 0)
          this.propertiesInState.set(property, srcValues);
        else
          this.propertiesInState.delete(property);
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
    if (access.init == null || property in src) {
      this.src[property] = src[property]; // Invokes the setter
    } else {
      this.setUpdated(set(access.init));
    }
  }

  /**
   * Call from a subclass constructor to initialise the mapping between the
   * graph and this subject's list items.
   *
   * @param src a List graph subject to initialise the list from
   * @param type JSON-LD type of items in the list
   * @param list an array-like object having:
   * 1. a _mutable_ length, which will be set during list updates
   * 2. numeric indexes, which will only be used to query for existence
   * @param access will be called to get & set JSON-LD values for items in the list
   */
  initList<T>(
    src: GraphSubject,
    type: AtomType<T>,
    list: { length: number, readonly [i: number]: unknown },
    access: {
      get: (i: number) => ValueConstructed<T>,
      set: (i: number, v: ValueConstructed<T>) => unknown | Promise<unknown>
    }
  ) {
    const listPropertyInState = () => {
      let values = this.propertiesInState.get('@list');
      if (values == null)
        this.propertiesInState.set('@list', values = Array(list.length));
      return values;
    };
    // @ts-ignore
    // Source list is an ArrayLike proxy
    this.src['@list'] = new Proxy(list, {
      ownKeys: list => Object.keys(list).filter(k => withArrayLikeKey(k,
        () => true, () => true, false)),
      has: (list, p: string | symbol) => withArrayLikeKey(p,
        () => true, () => true, false),
      get: (list, p: string | symbol) => withArrayLikeKey(p,
        () => list.length, i => i in list ?
          normaliseValue(access.get(i)) : undefined, undefined),
      set: (list, p: string | symbol, value: any) => withArrayLikeKey(p, () => {
        list.length = value;
        if (list.length > 0)
          listPropertyInState();
        else
          this.propertiesInState.delete('@list');
        return true;
      }, i => {
        listPropertyInState()[i] = value;
        this.setUpdated(access.set(i, castPropertyValue(value, type)));
        return true;
      }, false)
    });
    if (isPropertyObject('@list', src['@list']))
      [].splice.call(this.src['@list'], 0, list.length, ...asValues(src['@list']));
  }

  /**
   * Gathers all changes in the subject since the last update from state,
   * returns them as a m-ld update for applying to the domain, and reverts them
   * in anticipation of the resultant echoed update (which may differ due to the
   * application of constraints).
   */
  commit(): Update {
    const update: { '@delete'?: Subject, '@insert'?: Subject } = {};
    const mod = (s: '@insert' | '@delete') => update[s] ??= { '@id': this.src['@id'] };
    // TODO: deleted
    for (let property of new Set([
      ...Object.keys(this.src).filter(p => isPropertyObject(p, this.src[p])),
      ...this.propertiesInState.keys()
    ])) {
      const olds = this.propertiesInState.get(property) ?? [];
      if (property !== '@list') {
        const { deletes, inserts } = new SubjectPropertyValues(this.src, property).diff(olds);
        if (deletes.length > 0)
          mod('@delete')[property] = deletes.map(minimiseValue);
        if (inserts.length > 0)
          mod('@insert')[property] = inserts.map(minimiseValue);
        if (deletes.length > 0 || inserts.length > 0)
          this.src[property] = olds;
      } else {
        // TODO: Distinguish a move from a delete/insert, requires a List class
        const news = this.src['@list'] as ArrayLike<any>;
        const listMod = (s: '@insert' | '@delete') =>
          (((mod(s) as List)['@list'] ??= {}) as { [key: number]: SubjectPropertyObject });
        diffArrays(olds, news,
          i => listMod('@delete')[i] = minimiseValue(olds[i]),
          (i, items) => listMod('@insert')[i] = items.map(minimiseValue));
        if (update['@delete']?.['@list'] != null || update['@insert']?.['@list'] != null)
          [].splice.call(news, 0, news.length, ...olds);
      }
    }
    return update;
  }

  /**
   * Called when a graph subject (src) property is changing.
   *
   * @param result If any property is asynchronously updated, the final value
   * will only be definitively set when this promise resolves.
   */
  protected setUpdated(result: unknown | Promise<unknown>) {
    // Don't make unnecessary promises
    if (this._updated !== this || isPromise(result)) {
      this._updated = Promise.all([this._updated, result]).then(() => this);
      this._updated.catch(() => {}); // Prevents unhandled rejection
    }
  }

  private resolveAccess<T, S>(
    property: string,
    type: AtomType<T> | ContainerType<T>,
    access: PropertyAccess.Any<this, T, S>
  ): PropertyAccess.GetSet<T, S> {
    if ('get' in access) {
      return access;
    } else {
      const local = 'local' in access ? access.local : <keyof this>property;
      // TODO: type-assert that this[local] is ValueConstructed<T, S>
      if ('construct' in access) {
        if (type !== Subject && type !== Array)
          throw new TypeError('ORM subjects must be constructed from Subjects');
        // Subjects and arrays of subjects can be automatically get/set
        return {
          set: async (v: any) => this[local] =
            <any>(await access.orm.get(v, access.construct)),
          get: type === Subject ?
            () => (<any>this[local]).src :
            () => (<any>this[local])?.map((s: GraphSubject) => s.src)
        };
      } else {
        return {
          get: () => <any>this[local],
          set: (v: any) => this[local] = <any>v
        };
      }
    }
  }
}

/** @internal */
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

/**
 * Tries to construct the minimal operation to convert the olds to the news.
 * Note, something like fast-array-diff generates ordered patches, which don't
 * fit the m-ld requirement that deletes and inserts are unordered.
 * TODO: awful complexity for a prepend
 * @internal
 */
function diffArrays(
  olds: any[],
  news: ArrayLike<any>,
  deleted: (i: number) => void,
  inserted: (i: number, items: any[]) => void
) {
  for (let oldI = 0, newI = 0; ;) {
    if (newI < news.length && oldI < olds.length) {
      // remove anything that differs at this old position
      do {
        if (compareValues(olds[oldI], news[newI])) {
          oldI++;
          newI++; // new item accounted for in old list
          break;
        } else {
          deleted(oldI++);
        }
      } while (oldI < olds.length);
    } else {
      if (newI < news.length && oldI >= olds.length) {
        // Insert remaining news at the end
        inserted(oldI, [].slice.call(news, newI));
      } else if (newI >= news.length) {
        // Delete remaining olds
        for (; oldI < olds.length; oldI++)
          deleted(oldI);
      }
      break;
    }
  }
}