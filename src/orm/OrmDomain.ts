import { GraphSubject, GraphUpdate, MeldContext, MeldReadState, StateProc } from '../api';
import { SharedPromise } from '../engine/locks';
import { Iri } from '@m-ld/jsonld';
import { OrmSubject } from './OrmSubject';
import { isArray, settled } from '../engine/util';
import { isReference, Subject, Update } from '../jrql-support';
import { SubjectUpdater } from '../updates';
import { ReadLatchable } from '../engine';
import { BehaviorSubject, firstValueFrom } from 'rxjs';
import { filter } from 'rxjs/operators';
import async from '../engine/async';
import { array } from '../util';
import { MeldApp, MeldAppContext, MeldConfig } from '../config';
import { EventEmitter } from 'events';

/**
 * Standardised constructor type for ORM subjects
 * @see {@link OrmDomain}
 * @experimental
 * @category Experimental
 */
export type ConstructOrmSubject<T extends OrmSubject> =
  (src: GraphSubject, orm: OrmUpdating) => T | Promise<T>;

/**
 * Event handler for all new ORM subjects, in case they are of interest to the
 * domain – use {@link get} to translate the raw graph subject to an ORM
 * subject.
 */
export type CacheMissListener =
  (ref: GraphSubject, orm: OrmUpdating) => void | Promise<unknown>;

/**
 * Wraps a {@link MeldReadState read state}, in which some elements of an ORM
 * domain may be loading.
 *
 * @see {@link OrmDomain}
 * @experimental
 * @category Experimental
 */
export interface OrmUpdating extends ReadLatchable {
  /**
   * The attached ORM domain. The value of this property can be safely stored
   * if required to later obtain a new updating state, or to commit changes.
   */
  readonly domain: OrmDomain;
  /**
   * Get a subject in the domain. This method and its array-based overload are
   * the ONLY way to obtain subject instances, including new ones – a subject
   * constructor should only ever be called by the passed `construct` callback.
   * This ensures that the subject is attached to the domain. For new subjects,
   * it also allows that the subject may have been concurrently created remotely
   * in another clone.
   *
   * @param src can be a plain IRI or Reference, used to identify the Subject
   * @param construct called as necessary to create a new ORM Subject
   * @param scope
   */
  get<T extends OrmSubject>(
    src: GraphSubject | string,
    construct: ConstructOrmSubject<T>,
    scope?: OrmScope
  ): Promise<T>;
  /**
   * Get an array of subjects in the domain. This method and its overload are
   * the ONLY way to obtain subject instances, including new ones – a subject
   * constructor should only ever be called by the passed `construct` callback.
   * This ensures that the subject is attached to the domain. For new subjects,
   * it also allows that the subject may have been concurrently created remotely
   * in another clone.
   *
   * @param src can be a plain IRI or Reference, used to identify the Subject
   * @param construct called as necessary to create a new ORM Subject
   * @param scope
   */
  get<T extends OrmSubject>(
    src: GraphSubject[] | string[],
    construct: ConstructOrmSubject<T>,
    scope?: OrmScope
  ): Promise<T[]>;
  /**
   * Updates the ORM domain from the given **m-ld** core API update. Usually
   * called from a core API `read` or `follow` callback. Once the returned
   * promise has resolved, all subject-graph changes will have been applied to
   * domain subjects.
   *
   * @param update the **m-ld** core update
   */
  updated(update?: GraphUpdate): Promise<void>;
}

export interface OrmScope extends EventEmitter {
  /** The domain to which this scope belongs */
  readonly domain: OrmDomain;
  /** Destroys this scope including all cached subjects and event listeners */
  invalidate(): void;
  /** emitted when an ORM subject has been removed from the domain */
  on(eventName: 'deleted', listener: (subject: OrmSubject) => void): this;
  /**
   * emitted for all new ORM subjects, in case they are of
   * interest to the domain – use {@link get} to translate the raw graph subject
   * to an ORM subject.
   */
  on(eventName: 'cacheMiss', listener: CacheMissListener): this;
}

/**
 * The experimental Object-Resource Mapping (ORM) layer is a set of constructs
 * to support idiomatic use of object-oriented Javascript with the core **m-ld**
 * API. Instead of programming with reads and writes to the domain graph, you
 * manipulate Javascript objects that reflect the domain.
 *
 * > 🧪 ORM is currently used internally in extensions and has not yet been
 * tested in an application. If it sounds just like what you're looking to do
 * in your application, please [let us know](https://m-ld.org/hello)!
 *
 * An ORM domain object is a top-level container for a mutable graph of
 * {@link OrmSubject "subjects"} in-memory, where subjects are instances of
 * Javascript classes, and graph edges are straightforward class properties.
 *
 * Since read operations like traversal of graph edges are asynchronous in
 * **m-ld**, an ORM domain can be in an "updating" state, in which some edge may
 * not be fully loaded (sometimes called a "fault"). This mode is entered using
 * the {@link updating} method. It gives access to an {@link OrmUpdating} state,
 * from which graph content can be loaded. An updating state is required to
 * create new subject instances attached to the domain object (also since their
 * properties may need loading).
 *
 * Mutations of the graph by the app MUST NOT be done in an updating state (a
 * subject property that is not yet loaded could have an unexpected value of
 * `undefined` or `empty`; and the app mutation may be overridden by an
 * asynchronous load). An exception to this is setting the initial properties
 * of a new subject. Changes to a subject, or to the whole ORM domain, can be
 * {@link commit committed} to a writeable **m-ld** state.
 *
 * @see {@link OrmSubject}
 * @experimental
 * @category Experimental
 */
export class OrmDomain implements MeldAppContext {
  /**
   * Used to delay all attempts to access subjects until after any update.
   * Note that we're "up to date" with no state before the first update.
   */
  private readonly _updating = new BehaviorSubject<{
    state: MeldReadState, latch: SharedPromise<unknown>
  } | null>(null);

  private _scopes = new Set<_OrmScope>();

  private readonly orm = new class implements OrmUpdating {
    constructor(readonly domain: OrmDomain) {}

    async get<T extends OrmSubject>(
      src: GraphSubject | GraphSubject[] | string | string[],
      construct: ConstructOrmSubject<T>,
      scope = this.domain.scope
    ): Promise<T | T[]> {
      if (isArray(src)) {
        return Promise.all(src.map(src => (<OrmUpdating>this).get(src, construct)));
      } else {
        if (!(scope instanceof _OrmScope) || !this.domain._scopes.has(scope))
          throw new RangeError('Scope must be obtained from domain');
        const id = typeof src == 'string' ? src : src['@id'];
        if (scope._cache.has(id)) {
          // noinspection ES6MissingAwait - getting a promise
          return <T | Promise<T>>scope._cache.get(id);
        } else {
          const subject = this.latch(async state => {
            if (typeof src == 'string' || isReference(src)) {
              const got = await state.get(id);
              if (got != null)
                return construct(got, this);
              else
                // The given constructor might populate some properties or not,
                // but in any case we're returning the ref, so we need to keep it
                // at least until it's been deleted for certain
                return construct({ '@id': id }, this);
            } else {
              return construct(src, this);
            }
          });
          scope._cache.set(id, subject);
          return subject;
        }
      }
    }

    async updated(update?: GraphUpdate) {
      if (update != null) {
        const updater = new SubjectUpdater(update);
        for (let scope of this.domain._scopes) {
          for (let subject of scope._cache.values()) {
            if (async.isPromise(subject))
              await subject
                .then(subject => this.updateSubject(updater, subject, scope))
                .catch(); // Error will have been reported by get()
            else
              this.updateSubject(updater, subject, scope);
          }
          if (scope.hasCacheMissListeners) {
            // Anything new to the ORM domain which is 'got' by a cacheMiss
            // listener must be entered into the cache before iterating the
            // updated-ness, below
            await Promise.all(update['@insert']
              .filter(inserted => !scope._cache.has(inserted['@id']))
              .map(ref => scope.emitCacheMiss(ref, this)));
          }
        }
      }
      // In the course of an update, the cache may mutate. Rely on Map order to
      // ensure any new subjects are captured and updated.
      for (let scope of this.domain._scopes) {
        for (let [id, subject] of scope._cache.entries()) {
          if (async.isPromise(subject))
            scope._cache.set(id, subject = await subject);
          if (async.isPromise(subject.updated))
            await settled(subject.updated);
        }
      }
    }

    private updateSubject(
      updater: SubjectUpdater,
      subject: OrmSubject,
      scope: _OrmScope
    ) {
      updater.update(subject.src);
      subject.onPropertiesUpdated(this);
      if (subject.deleted) {
        // Here we make the assumption that a subject which is (still)
        // deleted after an update is no longer of interest to the app
        scope._cache.delete(subject.src['@id']);
        scope.emitDeleted(subject);
      }
    }

    async latch<T>(proc: StateProc<MeldReadState, T>): Promise<T> {
      const result = Promise.resolve(proc(this.active.state));
      return this.active.latch.extend('latch', result);
    }

    private get active() {
      if (this.domain._updating.value == null)
        throw new RangeError('No state active');
      else
        return this.domain._updating.value;
    }
  }(this);

  readonly config: MeldConfig;
  readonly app: MeldApp;
  readonly context: MeldContext;
  /** Global scope for the domain */
  readonly scope: OrmScope;

  constructor({ config, app, context }: MeldAppContext) {
    this.app = app;
    this.config = config;
    this.context = context;
    this.scope = this.createScope();
  }

  /**
   * A scope allows a code module such as an extension to avoid seeing some
   * other module's ORM classes when asking for a subject instance. This can be
   * important if the module's subject scope intersects with another module's.
   * Note that in such circumstances it's probably a good idea to use
   * module-specific properties, to avoid interference.
   */
  createScope(): OrmScope {
    const scope = new _OrmScope(this);
    this._scopes.add(scope);
    scope.on('invalidated', () => {
      if (scope !== this.scope)
        // Cheeky to mutate the domain's Set
        this._scopes.delete(scope);
    });
    return scope;
  }

  /**
   * Call to obtain an {@link OrmUpdating updating} state of the domain, to be
   * used to create or load ORM subjects.
   *
   * @param state a readable **m-ld** core API state
   * @param proc a procedure that uses the updating state – the given updating
   * state is guaranteed to remain valid until the returned promise settles.
   */
  async updating<T>(
    state: MeldReadState,
    proc: (orm: OrmUpdating) => T | Promise<T>
  ): Promise<T> {
    if (this._updating.value == null) {
      const latch = new SharedPromise('activate', () => proc(this.orm));
      this._updating.next({ state, latch });
      try {
        await latch.run(); // Wait for all child procs to complete
        return latch.result;
      } finally {
        await this.orm.updated();
        this._updating.next(null);
      }
    } else if (state === this._updating.value.state) {
      // Re-entry with the same state
      const result = await proc(this.orm);
      await this.orm.updated();
      return result;
    } else {
      throw new RangeError('Multiple active states!');
    }
  }

  /**
   * Allows the caller to wait for any active {@link OrmUpdating updating} state
   * to go out of scope, so that the app is free to mutate ORM subjects without
   * risk of faults. This method can be used to protect any code that is called
   * asynchronously, such as an event handler in a user interface.
   */
  upToDate(): Promise<unknown> {
    return firstValueFrom(this._updating.pipe(filter(updating => updating == null)));
  }

  /**
   * Obtain an update that captures every mutation made by the app to the domain
   * since the domain was last {@link updating updated}. The update should be
   * immediately written to the **m-ld** engine. This method also REVERTS the
   * mutations made in the app code. This is because the updates will be very
   * quickly re-applied, having been processed by the **m-ld** engine, in the
   * next update cycle.
   */
  commit(): Update {
    const update = { '@delete': [] as Subject[], '@insert': [] as Subject[] };
    for (let scope of this._scopes) {
      for (let subject of scope._cache.values()) {
        if (async.isPromise(subject))
          throw new TypeError('ORM domain has not finished updating');
        const subjectUpdate = subject.commit();
        update['@delete'].push(...array<Subject>(subjectUpdate['@delete']));
        update['@insert'].push(...array<Subject>(subjectUpdate['@insert']));
      }
    }
    return update;
  }
}

/**
 * Private friend class of OrmDomain for created scopes
 * @internal
 */
class _OrmScope extends EventEmitter implements OrmScope {
  _cache = new Map<Iri, OrmSubject | Promise<OrmSubject>>();

  constructor(
    readonly domain: OrmDomain
  ) {
    super();
  }

  invalidate(): void {
    this.removeAllListeners();
    this._cache.clear();
    this.emit('invalidated');
  }

  /** emitted when an ORM subject has been removed from the domain */
  on(eventName: 'deleted', listener: (subject: OrmSubject) => void): this;
  /**
   * emitted for all new ORM subjects, in case they are of
   * interest to the domain – use {@link get} to translate the raw graph subject
   * to an ORM subject.
   */
  on(eventName: 'cacheMiss', listener: CacheMissListener): this;
  /** Internal event for removing scope from domain */
  on(eventName: 'invalidated', listener: () => void): this;
  /** @override */
  on(eventName: string, listener: (...args: any[]) => void): this {
    return super.on(eventName, listener);
  }

  emitDeleted = (subject: OrmSubject) =>
    this.emit('deleted', subject);

  get hasCacheMissListeners() {
    return this.listenerCount('cacheMiss') > 0;
  }

  // Manual emission to return promise
  emitCacheMiss = (ref: GraphSubject, orm: OrmUpdating) =>
    Promise.all(this.listeners('cacheMiss')
      .map((listener: CacheMissListener) => listener(ref, orm)));
}
