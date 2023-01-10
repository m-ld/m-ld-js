import {
  isPropertyObject, List, Reference, Subject, SubjectProperty, SubjectPropertyObject, Update
} from '../jrql-support';
import { GraphSubject } from '../api';
import {
  AtomType, castPropertyValue, JsProperty, JsType, normaliseValue, ValueConstructed
} from '../js-support';
import { isNaturalNumber } from '../engine/util';
import { asValues, compareValues, minimiseValue } from '../engine/jsonld';
import { SubjectPropertyValues } from '../subjects';
import async from '../engine/async';
import { ConstructOrmSubject, OrmScope, OrmUpdating } from './OrmDomain';
import { Iri } from '@m-ld/jsonld';
import 'reflect-metadata';

/**
 * Runtime property access metadata for ORM subjects
 * @see {@link OrmSubject}
 */
declare namespace PropertyAccess {
  interface Init<T, S> {
    /**
     * An initial Javascript value. If this is non-null, it will be used instead
     * of the `src` property value.
     */
    init?: ValueConstructed<T, S>;
  }

  interface Orm<T, S> extends Init<T, S> {
    /** Used to construct a new ORM subject from state */
    construct: ConstructOrmSubject<OrmSubject>;
    /** The current updating ORM state, used to initialise the subject */
    orm: OrmUpdating,
    /** The ORM scope to use when initialising subjects */
    scope?: OrmScope
  }

  interface GetSet<T, S> extends Init<T, S> {
    /** Gets the property value from the ORM subject as a JSON-LD value */
    get: () => ValueConstructed<T, S>,
    /** Sets the property value in the ORM subject from a JSON-LD value */
    set: (v: ValueConstructed<T, S>) => unknown | Promise<unknown>
  }

  type Any<T = unknown, S = unknown> = Init<T, S> | Orm<T, S> | GetSet<T, S>;
}

/** @internal */
const propertyMetadataKey = Symbol('__property');
/** @internal */
const propertiesMetadataKey = Symbol('__properties');

/**
 * Shorthand annotation to declare an ORM subject field to be mapped to a
 * JSON-LD graph property (an edge).
 * @param type the JSON-LD type of the graph subject property
 * @param name the JSON-LD property. If undefined, the field name will be used
 */
export function property(type: JsType<any>, name?: Iri): PropertyDecorator {
  return (target, property) => {
    const local = property.toString();
    Reflect.defineMetadata(
      propertyMetadataKey,
      new JsProperty(name ?? local, type),
      target, property);
    Reflect.defineMetadata(
      propertiesMetadataKey,
      [...Reflect.getMetadata(propertiesMetadataKey, target) ?? [], local],
      target);
  };
}

/** @internal NOTE do not inline: breaks typedoc */
type ObjKey<O> = [obj: O, key: string & keyof O];

/**
 * An {@link OrmDomain Object-Resource Mapping (ORM)} Subject is a Javascript
 * class used to reflect graph nodes in a **m-ld** domain.
 *
 * The constructor of an ORM subject should accept a {@link GraphSubject} from
 * which derive its initial state. In the case of a new subject, which does not
 * yet exist in the domain, this will only contain an `@id` (i.e. it's a {@link
 * Reference}), so the constructor may also accept other values for initialising
 * properties.
 *
 * Local properties properties are then mapped to graph properties (edges) by
 * calling {@link initSrcProperties} in the constructor. This ensures that any
 * local changes to the object property values are tracked, and can be committed
 * to the graph later using {@link commit} (usually called for a batch of
 * subjects from {@link OrmDomain.commit}). E.g.
 *
 * ```typescript
 * class Flintstone extends OrmSubject {
 *   @property(JsType.for(String))
 *   name: string;
 *   @property(JsType.for(Optional, Number))
 *   height?: number;
 *
 *   constructor(src: GraphSubject) {
 *     super(src);
 *     this.initSrcProperties(src);
 *   }
 * }
 * ```
 *
 * Note that an ORM subject constructor should only ever be called by the passed
 * `construct` callback of {@link OrmUpdating.get}, so that the subject is
 * correctly registered with the ORM domain.
 *
 * @see {@link OrmDomain}
 * @experimental
 * @category Experimental
 */
export abstract class OrmSubject {
  readonly src: Subject & Reference;
  private readonly propertiesInState = new Map<string, any[]>();
  private _updated: Promise<this> | this = this;

  /** Utility property definition for the JSON-LD `@type` key */
  static '@type' = new JsProperty('@type', JsType.for(Array, String));

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
   * graph and this subject's properties, which have been declared with the
   * {@link property} annotation in Typescript. Additional runtime property
   * access metadata can be provided in the `access` parameter.
   *
   * @param src the initial graph subject – used for its property values
   * @param access custom property access information
   */
  initSrcProperties(
    src: GraphSubject,
    access: { [local: string]: PropertyAccess.Any } = {}
  ) {
    for (let local of Reflect.getMetadata(propertiesMetadataKey, this)) {
      const property: JsProperty<any> =
        Reflect.getMetadata(propertyMetadataKey, this, local);
      if (property != null && !this.propertyInitialised(property))
        this.initSrcProperty(src, local, property, access[local]);
    }
  }

  /**
   * Call from a subclass constructor to initialise the mapping between the
   * graph and this subject's property. This method is for use in Javascript,
   * and in scenarios where the structure of this ORM subject does not reflect
   * the graph representation – for example, an aggregation member reflecting
   * data from its parent graph subject.
   *
   * @param src the initial graph subject – only used for its property value
   * @param local the local property name. If the type in the graph
   * cannot be cast to the property type, {@link updated} will be rejected.
   * @param property the property definition
   * @param access custom property access information
   * @throws {RangeError} if the property has already been initialised
   */
  initSrcProperty<T, S, O = this>(
    src: GraphSubject,
    local: (string & keyof this) | ObjKey<O>,
    property: JsProperty<T, S>,
    access?: PropertyAccess.Any<T, S>
  ) {
    if (this.propertyInitialised(property))
      throw new RangeError('Property already initialised');
    const { get, set } = typeof local == 'string' ?
      resolveAccess(this, local, property, access) :
      resolveAccess(...local, property, access);
    Object.defineProperty(this.src, property.name, {
      get,
      set: v => {
        const srcValues = asValues(v);
        if (srcValues.length > 0)
          this.propertiesInState.set(property.name, srcValues);
        else
          this.propertiesInState.delete(property.name);
        try {
          this.onPropertyUpdated(property.name, set(property.type.cast(v)));
        } catch (e) {
          this.onPropertyUpdated(property.name, Promise.reject(e));
        }
      },
      enumerable: true, // JSON-able
      configurable: false // Cannot delete the property
    });
    if (access?.init == null || property.name in src) {
      this.src[property.name] = src[property.name]; // Invokes the setter
    } else {
      this.onPropertyUpdated(property.name, set(access.init));
    }
  }

  private propertyInitialised(property: JsProperty<any, any>) {
    return property.name in this.src;
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
  initSrcList<T>(
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
        this.onPropertyUpdated(['@list', i],
          access.set(i, castPropertyValue(value, type)));
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
        const { deletes, inserts } =
          new SubjectPropertyValues(this.src, property).diff(olds);
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
   * Called when a graph subject (src) property is being updated.
   *
   * @param property The property that is being updated.
   * @param result If any property is asynchronously updated, the final value
   * will only be definitively set when this promise resolves.
   */
  protected onPropertyUpdated(property: SubjectProperty, result: unknown | Promise<unknown>) {
    // Don't make unnecessary promises
    if (this._updated !== this || async.isPromise(result)) {
      this._updated = Promise.all([this._updated, result]).then(() => this);
      this._updated.catch(() => {}); // Prevents unhandled rejection
    }
  }

  /**
   * Called by the ORM domain when the subject's properties have been fully
   * updated. The {@link deleted} flag will be set. Any reference properties or
   * other asynchronous dependencies may not yet be loaded (may be 'faults').
   */
  onPropertiesUpdated(orm: OrmUpdating) {}
}

/** @internal */
function resolveAccess<T, S, O>(
  obj: O,
  key: string & keyof O,
  property: JsProperty<T, S>,
  access?: PropertyAccess.Any<T, S>
): PropertyAccess.GetSet<T, S> {
  if (access != null && 'get' in access) {
    return access;
  } else {
    // TODO: type-assert that obj[key] is ValueConstructed<T, S>
    if (access != null && 'construct' in access) {
      const [type] = property.type.type;
      if (type !== Subject && type !== Array)
        throw new TypeError('ORM subjects must be constructed from Subjects');
      // Subjects and arrays of subjects can be automatically get/set
      return {
        set: async (v: any) => obj[key] =
          <any>(await access.orm.get(v, access.construct, access.scope)),
        get: type === Subject ?
          () => (<any>obj[key]).src :
          () => (<any>obj[key])?.map((s: GraphSubject) => s.src)
      };
    } else {
      return {
        get: () => <any>obj[key],
        set: (v: any) => obj[key] = <any>v
      };
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