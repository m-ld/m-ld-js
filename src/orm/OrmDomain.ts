import { GraphSubject, GraphUpdate, MeldReadState, StateProc } from '../api';
import { SharedPromise } from '../engine/locks';
import { Iri } from '@m-ld/jsonld';
import { OrmSubject } from './OrmSubject';
import { isArray, settled } from '../engine/util';
import { isReference, Subject, Update } from '../jrql-support';
import { SubjectUpdater } from '../updates';
import { ReadLatchable } from '../engine/index';
import { BehaviorSubject, firstValueFrom } from 'rxjs';
import { filter } from 'rxjs/operators';
import async from '../engine/async';
import { array } from '../util';
import { MeldApp, MeldConfig } from '../config';

/**
 * Standardised constructor type for ORM subjects
 * @see {@link OrmDomain}
 * @experimental
 * @category Experimental
 */
export type ConstructOrmSubject<T extends OrmSubject> =
  (src: GraphSubject, orm: OrmUpdating) => T | Promise<T>;

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
   */
  get<T extends OrmSubject>(
    src: GraphSubject | string,
    construct: ConstructOrmSubject<T>
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
   */
  get<T extends OrmSubject>(
    src: GraphSubject[] | string[],
    construct: ConstructOrmSubject<T>
  ): Promise<T[]>;
  /**
   * Updates the ORM domain from the given **m-ld** core API update. Usually
   * called from a core API `read` or `follow` callback. Once the returned
   * promise has resolved, all subject-graph changes will have been applied to
   * domain subjects.
   *
   * @param update the **m-ld** core update
   * @param deleted called when an ORM subject has been removed from the domain
   * @param cacheMiss called for all new ORM subjects, in case they are of
   * interest to the domain – use {@link get} to translate the raw graph subject
   * to an ORM subject.
   */
  updated(
    update?: GraphUpdate,
    deleted?: (subject: OrmSubject) => void,
    cacheMiss?: (ref: GraphSubject) => void | Promise<unknown>
  ): Promise<void>;
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
export class OrmDomain {
  /**
   * Used to delay all attempts to access subjects until after any update.
   * Note that we're "up to date" with no state before the first update.
   */
  private readonly _updating = new BehaviorSubject<{
    state: MeldReadState, latch: SharedPromise<unknown>
  } | null>(null);

  private _cache = new Map<Iri, OrmSubject | Promise<OrmSubject>>();

  private readonly orm = new class implements OrmUpdating {
    constructor(readonly domain: OrmDomain) {}

    async get<T extends OrmSubject>(
      src: GraphSubject | GraphSubject[] | string | string[],
      construct: ConstructOrmSubject<T>
    ): Promise<T | T[]> {
      if (isArray(src)) {
        return Promise.all(src.map(src => (<OrmUpdating>this).get(src, construct)));
      } else {
        const id = typeof src == 'string' ? src : src['@id'];
        if (this.domain._cache.has(id)) {
          // noinspection ES6MissingAwait - getting a promise
          return <Promise<T>>this.domain._cache.get(id);
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
          this.domain._cache.set(id, subject);
          return subject;
        }
      }
    }

    async updated(
      update?: GraphUpdate,
      deleted?: (subject: OrmSubject) => void,
      cacheMiss?: (ref: GraphSubject) => void | Promise<unknown>
    ) {
      if (update != null) {
        const updater = new SubjectUpdater(update);
        for (let subject of this.domain._cache.values()) {
          if (async.isPromise(subject))
            await subject
              .then(subject => this.updateSubject(updater, subject, deleted))
              .catch(); // Error will have been reported by get()
          else
            this.updateSubject(updater, subject, deleted);
        }
        if (cacheMiss != null) {
          // Anything new to the ORM domain which is 'got' by the cacheMiss
          // function must be entered into the cache before iterating the
          // updated-ness, below
          await Promise.all(update['@insert']
            .filter(inserted => !this.domain._cache.has(inserted['@id']))
            .map(cacheMiss));
        }
      }
      // In the course of an update, the cache may mutate. Rely on Map order to
      // ensure any new subjects are captured and updated.
      for (let [id, subject] of this.domain._cache.entries()) {
        if (async.isPromise(subject))
          this.domain._cache.set(id, subject = await subject);
        if (async.isPromise(subject.updated))
          await settled(subject.updated);
      }
    }

    private updateSubject(
      updater: SubjectUpdater,
      subject: OrmSubject,
      deleted?: (subject: OrmSubject) => void
    ) {
      updater.update(subject.src);
      if (subject.deleted) {
        // Here we make the assumption that a subject which is (still)
        // deleted after an update is no longer of interest to the app
        this.domain._cache.delete(subject.src['@id']);
        deleted?.(subject);
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

  constructor(
    readonly config: MeldConfig,
    readonly app: MeldApp
  ) {}

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
    for (let subject of this._cache.values()) {
      if (async.isPromise(subject))
        throw new TypeError('ORM domain has not finished updating');
      const subjectUpdate = subject.commit();
      update['@delete'].push(...array<Subject>(subjectUpdate['@delete']));
      update['@insert'].push(...array<Subject>(subjectUpdate['@insert']));
    }
    return update;
  }
}