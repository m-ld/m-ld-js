import { GraphSubject, GraphUpdate, MeldReadState, StateProc } from '../api';
import { SharedPromise } from '../engine/locks';
import { Iri } from '@m-ld/jsonld';
import { OrmSubject } from './OrmSubject';
import { isArray, settled } from '../engine/util';
import { isReference } from '../jrql-support';
import { SubjectUpdater } from '../updates';
import { ReadLatchable } from '../engine/index';
import { BehaviorSubject, firstValueFrom } from 'rxjs';
import { filter } from 'rxjs/operators';

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
  update(
    update?: GraphUpdate,
    deleted?: (subject: OrmSubject) => void,
    cacheMiss?: (ref: GraphSubject) => void | Promise<unknown>
  ): Promise<void>;
}

/**
 * TODO docs, unit tests
 *
 * @experimental
 * @category Experimental
 */
export class OrmDomain {
  /**
   * Used to delay all attempts to access subjects until after any update.
   * Note that we're "up to date" with no state before the first update.
   */
  private readonly _upToDate = new BehaviorSubject(true);

  private readonly orm = new class implements OrmState {
    private _cache = new Map<Iri, OrmSubject>();
    private _active?: { state: MeldReadState, latch: SharedPromise<unknown> };

    /**
     * @param src can be a Reference, used to load the Subject
     * @param construct called as necessary to create a new ORM Subject
     */
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
      this._cache.delete(subject.src['@id']);
      return subject;
    }

    async update(
      update?: GraphUpdate,
      deleted?: (subject: OrmSubject) => void,
      cacheMiss?: (ref: GraphSubject) => void | Promise<unknown>
    ) {
      if (update != null) {
        const updater = new SubjectUpdater(update);
        for (let subject of this._cache.values()) {
          updater.update(subject.src);
          if (subject.deleted) {
            this.delete(subject);
            deleted?.(subject);
          }
        }
        if (cacheMiss != null) {
          // Anything new to the ORM domain which is 'got' by the cacheMiss
          // function must be entered into the cache before iterating the
          // updated-ness, below
          await Promise.all(update['@insert']
            .filter(inserted => !this._cache.has(inserted['@id']))
            .map(cacheMiss));
        }
      }
      // In the course of updating, the cache may mutate. Rely on Map order
      for (let subject of this._cache.values())
        await settled(subject.updated);
    }

    async latch<T>(proc: StateProc<MeldReadState, T>): Promise<T> {
      const result = Promise.resolve(proc(this.active.state));
      return this.active.latch.extend('latch', result);
    }

    async activate<T>(
      state: MeldReadState,
      proc: (env: ReadLatchable) => T | Promise<T>
    ): Promise<T> {
      // FIXME: what about re-entry?!
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

  updating<T>(
    state: MeldReadState,
    proc: (orm: OrmState) => T | Promise<T>
  ): Promise<T> {
    this._upToDate.next(false);
    return this.orm.activate(state, proc).finally(() => this._upToDate.next(true));
  }

  upToDate() {
    return firstValueFrom(this._upToDate.pipe(filter(ready => ready)));
  }
}