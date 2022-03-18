import { isReference, Reference, Subject } from './jrql-support';
import { GraphSubject, GraphUpdate, MeldReadState, StateProc } from './api';
import { isArray } from './engine/util';
import { ReadLatchable } from './engine/index';
import { SharedPromise } from './engine/locks';
import { Iri } from 'jsonld/jsonld-spec';
import { SubjectUpdater } from './updates';
import {
  AtomType, castPropertyValue, ContainerType, normaliseValue, ValueConstructed
} from './js-support';

export type ConstructOrmSubject<T extends OrmSubject> =
  (src: GraphSubject, orm: OrmState) => T;

/**
 * TODO docs
 *
 * @experimental
 * @category Experimental
 */
export interface OrmState extends ReadLatchable {
  get<T extends OrmSubject>(src: GraphSubject, construct: ConstructOrmSubject<T>): Promise<T>;
  get<T extends OrmSubject>(src: GraphSubject[], construct: ConstructOrmSubject<T>): Promise<T[]>;
  update(update?: GraphUpdate): Promise<void>;
}

/**
 * TODO docs, unit tests
 *
 * @experimental
 * @category Experimental
 */
export class OrmDomain {
  private readonly orm = new class implements OrmState {
    private _active?: { state: MeldReadState, latch: SharedPromise<unknown> };
    private _cache = new Map<Iri, OrmSubject>();

    async get<T extends OrmSubject>(
      src: GraphSubject | GraphSubject[],
      construct: ConstructOrmSubject<T>
    ): Promise<T | T[]> {
      if (isArray(src)) {
        return Promise.all(src.map(src => (<OrmState>this).get(src, construct)));
      } else {
        return <T>this._cache.get(src['@id']) ?? await this.latch(async state => {
          if (isReference(src)) {
            const got = await state.get(src['@id']);
            if (got == null)
              return this.delete(construct(src, this));
            else
              return this.put(construct(got, this));
          } else {
            return this.put(construct(src, this));
          }
        });
      }
    }

    put<T extends OrmSubject>(subject: T): T {
      this._cache.set(subject.src['@id'], subject);
      return subject;
    }

    delete<T extends OrmSubject>(subject: T): T {
      subject.deleted = true;
      this._cache.delete(subject.src['@id']);
      return subject;
    }

    async update(update?: GraphUpdate) {
      if (update != null) {
        const updater = new SubjectUpdater(update);
        for (let subject of this._cache.values()) {
          updater.update(subject.src);
          if (isReference(subject.src))
            this.delete(subject);
        }
      }
      // In the course of updating, the cache may mutate. Rely on Map order
      for (let subject of this._cache.values())
        await subject.updated.catch();
    }

    async latch<T>(proc: StateProc<MeldReadState, T>): Promise<T> {
      const result = Promise.resolve(proc(this.active.state));
      return this.active.latch.extend('latch', result);
    }

    async activate<T>(
      state: MeldReadState,
      proc: (env: ReadLatchable) => T | Promise<T>
    ): Promise<T> {
      const latch = new SharedPromise('activate', () => proc(this));
      this._active = { state, latch };
      await latch.run(); // Wait for all child procs to complete
      return latch.result.finally(() => delete this._active);
    }

    private get active() {
      if (this._active == null)
        throw new RangeError('No state active');
      else
        return this._active;
    }
  };

  withActiveState<T>(
    state: MeldReadState,
    proc: (orm: OrmState) => T | Promise<T>
  ): Promise<T> {
    return this.orm.activate(state, proc);
  }
}

/**
 * TODO docs, unit tests
 *
 * @experimental
 * @category Experimental
 */
export abstract class OrmSubject {
  readonly src: Subject & Reference;
  deleted = false;
  updated: Promise<this>;

  constructor(src: GraphSubject) {
    this.src = { '@id': src['@id'] };
    this.updated = Promise.resolve(this);
  }

  protected initSrcProperty<T, S>(
    src: GraphSubject,
    property: string,
    type: AtomType<T> | [ContainerType<T>, AtomType<S>],
    get: () => ValueConstructed<T, S>,
    set: (v: ValueConstructed<T, S>) => unknown | Promise<unknown>,
    value?: ValueConstructed<T, S>
  ) {
    const [topType, subType] = isArray(type) ? type : [type];
    const update = (value: ValueConstructed<T, S>) => {
      this.updated = Promise.all([this.updated, set(value)]).then(() => this);
    }
    Object.defineProperty(this.src, property, {
      get: () => normaliseValue(get()),
      set: v => update(castPropertyValue(v, topType, subType, property)),
      enumerable: true, // JSON-able
      configurable: false // Cannot delete the property
    });
    if (value != null)
      update(value);
    else
      this.src[property] = src[property]; // Invokes the setter
  }
}
