import type { Context } from './jrql-support';
import type { ConstraintConfig } from './constraints';
import type { LogLevelDesc } from 'loglevel';
import type { AppPrincipal, MeldExtensions } from './api';
import type { EventEmitter } from 'events';
import { Observable } from 'rxjs';

/**
 * **m-ld** clone configuration, used to initialise a {@link clone} for use.
 *
 * @category Configuration
 */
export interface MeldConfig {
  /**
   * The local identity of the m-ld clone session, used for message bus identity
   * and logging. This identity does not need to be the same across re-starts of
   * a clone with persistent data. It must be unique among the clones for the
   * domain. For convenience, you can use the {@link uuid} function.
   */
  '@id': string;
  /**
   * A URI domain name, which defines the universal identity of the dataset
   * being manipulated by a set of clones (for example, on the configured
   * message bus). For a clone with persistent data from a prior session, this
   * *must* be the same as the previous session.
   */
  '@domain': string;
  /**
   * An optional JSON-LD context for the domain data. If not specified:
   * * `@base` defaults to `http://{domain}`
   * * `@vocab` defaults to the resolution of `/#` against the base
   */
  '@context'?: Context;
  /**
   * Semantic constraints to apply to the domain data.
   * @deprecated see {@link MeldExtensions}
   */
  constraints?: ConstraintConfig[];
  /**
   * Journaling configuration
   */
  journal?: JournalConfig;
  /**
   * Set to `true` to indicate that this clone will be 'genesis'; that is, the
   * first new clone on a new domain. This flag will be ignored if the clone is
   * not new. If `false`, and this clone is new, successful clone initialisation
   * is dependent on the availability of another clone. If set to true, and
   * subsequently another non-new clone appears on the same domain, either or
   * both clones will immediately close to preserve their data integrity.
   */
  genesis: boolean;
  /**
   * An sane upper bound on how long any to wait for a response over the
   * network, in milliseconds. Used for message send timeouts and to trigger
   * fallback behaviours. Default is five seconds.
   */
  networkTimeout?: number;
  /**
   * An upper bound on operation message size, in bytes. Usually imposed by the
   * message publishing implementation. Default is infinity. Exceeding this
   * limit will cause a transaction to fail, to prevent a clone from being
   * unable to transmit the update to its peers.
   */
  maxOperationSize?: number;
  /**
   * Log level for the clone
   * @see https://github.com/pimterry/loglevel#documentation
   */
  logLevel?: LogLevelDesc;
}

/**
 * **m-ld** clone journal configuration.
 * @category Experimental
 * @experimental
 */
export interface JournalConfig {
  /**
   * Time, in milliseconds, to delay expensive automatic journal administration
   * tasks such as truncation and compaction, while the clone is highly active.
   * Default is one second. Set to `0` to disable automatic journal
   * administration (administration may still be prompted by the app).
   *
   * @default 1000
   */
  adminDebounce?: number;
  /**
   * A threshold of approximate entry size, in bytes, beyond which a fused entry will be
   * committed rather than further extended. The entry storage size may be less than this if it
   * compresses well, and can also be greater if the last (or only) individual transaction was
   * itself large. Default is 10K.
   * @default 10000
   */
  maxEntryFootprint?: number;
}

/**
 * Types of prompts for journal administration functions.
 * @internal
 */
export enum JournalCheckPoint {
  /**
   * General administration: the journal implementation responds by performing
   * any outstanding (potentially compute intensive) tasks.
   *
   * Note that if {@link JournalConfig.adminDebounce} is non-zero (or omitted),
   * admin checkpoints will occur automatically, debounced after clone activity.
   */
  ADMIN,
  /**
   * A savepoint instructs the journal implementation to retain the most recent
   * entry so that subsequent entries can be precisely revoked.
   */
  SAVEPOINT,
  /**
   * TODO: TRUNCATE, entries before will be dropped
   */
}

/**
 * Hooks for the app to take control of clone journal administration
 * @internal
 */
export type JournalAdmin = {
  /**
   * A stream of prompts for the journal to perform administrative activity.
   *
   * @see {@link JournalCheckPoint}
   */
  checkpoints?: Observable<JournalCheckPoint>,
  /**
   * The schedule delays journal administrative activity to some suitable time
   * after a checkpoint. By default, in the browser this will wait for an idle
   * period if `requestIdleCallback` is available; otherwise, it will wait for
   * the next event loop tick.
   *
   * @see https://developer.mozilla.org/en-US/docs/Web/API/Window/requestIdleCallback
   */
  schedule?: Observable<unknown>
};

/**
 * The runtime embedding environment for the **m-ld** clone. The clone calls
 * back the app for specific behaviours; see the members of this class.
 *
 * @category Configuration
 */
export interface MeldApp {
  /**
   * This object must be provided if the domain declares an access control
   * extension requiring an identified security principal (user or machine).
   */
  principal?: AppPrincipal;
  /**
   * An event emitter for receiving low-level backend transaction events. Use to
   * debug or trigger offline save. Received events are:
   *
   * - `commit(id: string)`: a transaction batch with the given ID has committed
   *   normally
   * - `error(err: any)`: an error has occurred in the store (most such errors
   *   will also manifest in the operation performed)
   * - `clear()`: the store has been cleared, as when applying a new snapshot
   * - `timing(entry: PerformanceEntry)`: stopwatch timings for debugging
   */
  backendEvents?: EventEmitter;
  /**
   * Journal administration hooks
   * @internal
   */
  journalAdmin?: JournalAdmin;
}

/**
 * Initial definition of a **m-ld** app. Extensions provided will be used for
 * bootstrapping, prior to the clone joining the domain. After that, different
 * extensions may come into effect if so declared in the data.
 *
 * @category Configuration
 */
export type InitialApp = MeldApp & MeldExtensions;
