import { QuadStoreDataset } from './engine/dataset';
import { DatasetClone } from './engine/dataset/DatasetClone';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { MeldApi } from './MeldApi';
import { Context, Subject, Pattern, Read, Update } from './jrql-support';
import { LogLevelDesc } from 'loglevel';
import { generate } from 'short-uuid';
import { ConstraintConfig, constraintFromConfig } from './constraints';
import * as spec from '@m-ld/m-ld-spec';
import { DomainContext } from './engine/MeldJson';
import { Observable } from 'rxjs';

export * from './MeldApi';
export { shortId } from './engine/util';
export {
  Pattern, Reference, Context, Variable, Value, Describe,
  Group, Query, Read, Result, Select, Subject, Update
} from './jrql-support';

/**
 * **m-ld** clone configuration, used to initialise a {@link clone} for use.
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
   * *must not* be different to the previous session.
   */
  '@domain': string;
  /**
   * An optional JSON-LD context for the domain data. If not specified:
   * * `@base` defaults to `http://{domain}`
   * * `@vocab` defaults to the resolution of `/#` against the base
   */
  '@context'?: Context;
  /**
   * A constraint to apply to the domain data, usually a composite.
   */
  constraint?: ConstraintConfig;
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
   * Options for the LevelDB instance to be opened for the clone data
   */
  ldbOpts?: AbstractOpenOptions;
  /**
   * An sane upper bound on how long any to wait for a response over the
   * network, in milliseconds. Used for message send timeouts and to trigger
   * fallback behaviours. Default is five seconds.
   */
  networkTimeout?: number;
  /**
   * Log level for the clone
   */
  logLevel?: LogLevelDesc;
}

/** 
 * Utility to generate a unique UUID for use in a MeldConfig
 */
export function uuid() {
  // This is indirected for documentation (do not just re-export generate)
  return generate();
}

/**
 * Create or initialise a local clone, depending on whether the given LevelDB
 * database already exists. This function returns as soon as it is safe to begin
 * transactions against the clone; this may be before the clone has received all
 * updates from the domain. To await latest updates, await a call to the
 * {@link MeldApi} `latest` method.
 * @param ldb an instance of a leveldb backend
 * @param remotes a driver for connecting to remote m-ld clones on the domain.
 * This can be a configured object (e.g. `new MqttRemotes(config)`) or just the
 * class (`MqttRemotes`).
 * @param config the clone configuration
 * @param constraint a constraint implementation, overrides config constraint.
 * Experimental: use with caution.
 */
export async function clone(
  ldb: AbstractLevelDOWN,
  remotes: dist.mqtt.MqttRemotes | dist.ably.AblyRemotes,
  config: MeldConfig,
  constraint?: MeldConstraint): Promise<MeldApi> {

  const context = new DomainContext(config['@domain'], config['@context']);
  const dataset = new QuadStoreDataset(ldb, config.ldbOpts);

  if (typeof remotes == 'function')
    remotes = new remotes(config);

  if (constraint == null && config.constraint != null)
    constraint = await constraintFromConfig(config.constraint, context);

  const clone = new DatasetClone({ dataset, remotes, constraint, config });
  await clone.initialise();
  return new MeldApi(context, clone);
}

// The m-ld remotes API is not yet public, so here we just declare the available
// modules for documentation purposes.
declare module dist {
  module mqtt { export type MqttRemotes = any; }
  module ably { export type AblyRemotes = any; }
}

// Unchanged from m-ld-spec
export type LiveStatus = spec.LiveStatus;
export type MeldStatus = spec.MeldStatus;

export interface MeldUpdate extends spec.MeldUpdate {
  '@delete': Subject[];
  '@insert': Subject[];
}

/**
 * A means to access the local clock tick of a transaction response.
 */
export interface HasExecTick {
  /**
   * The promise will be resolved with the local clone clock tick of the
   * transaction. For a read, this will be the tick from which the results were
   * obtained. For a write, this is the tick of the transaction completion. In
   * both cases, the promise resolution is a microtask in the event loop
   * iteration corresponding to the given clone clock tick. Therefore, `follow`
   * can be immediately called and the result subscribed, to be notified of
   * strictly subsequent updates.
   *
   * Note that this means the actual tick value is typically redundant, for this
   * engine. It's provided for consistency and in case of future use.
   */
  readonly tick: Promise<number>;
}

/**
 * A **m-ld** clone represents domain data to an app.
 *
 * The Javascript clone engine uses a database engine, for which in-memory,
 * on-disk and in-browser persistence options are available (see
 * [Getting&nbsp;Started](/#getting-started)).
 *
 * The raw API methods of this class are augmented with convenience methods for
 * use by an app in the [`MeldApi`](/classes/meldapi.html) class.
 *
 * @see https://spec.m-ld.org/interfaces/meldclone.html
 */
export interface MeldClone {
  /**
   * Actively writes data to, or reads data from, the domain.
   *
   * The transaction executes once, asynchronously, and the results are notified
   * to subscribers of the returned stream.
   *
   * For write requests, the query executes as soon as possible. The result is
   * only completion or error of the returned observable stream – no Subjects
   * are signalled.
   *
   * For read requests, the query executes in response to the first subscription
   * to the returned stream, and subsequent subscribers will share the same
   * results stream.
   *
   * @param request the declarative transaction description
   * @returns an observable stream of subjects. For a write transaction, this is
   * empty, but indicates final completion or error of the transaction.
   */
  transact(request: Pattern): Observable<Subject> & HasExecTick;
  /**
   * Follow updates from the domain. All data changes are signalled through the
   * returned stream, strictly ordered according to the clone's logical clock.
   * The updates can therefore be correctly used to maintain some other view of
   * data, for example in a user interface or separate database.
   *
   * In this engine, the returned stream will signal all updates after the event
   * loop tick on which the stream is subscribed. To ensure that all updates are
   * observed, call this method immediately on receipt of the clone object, and
   * synchronously subscribe.
   *
   * This method will include the notification of 'rev-up' updates after a
   * connect to the domain. To change this behaviour, also subscribe to `status`
   * changes and ignore updates while the status is marked as `outdated`.
   *
   * @returns an observable stream of updates from the domain.
   */
  follow(): Observable<MeldUpdate>;
  /**
   * The current and future status of a clone. This stream is hot and
   * continuous, terminating when the clone closes (and can therefore be used to
   * detect closure).
   */
  readonly status: Observable<MeldStatus> & LiveStatus;
  /**
   * Closes this clone engine gracefully. Using this method ensures that data
   * has been fully flushed to storage and all transactions have been notified
   * to the domain (if this clone is online).
   * @param err used to notify a reason for the closure, for example an
   * application failure, for problem diagnosis.
   */
  close(err?: any): Promise<unknown>;
}

/**
 * Interface provided to a {@link MeldConstraint} to read data during checking
 * and application of a constraint.
 * @param request the Read request, e.g. a Select or Describe
 * @returns an observable stream of found Subjects
 */
export type MeldReader = <R extends Read>(request: R) => Observable<Subject>;

/**
 * A constraint asserts an invariant for data in a clone. When making
 * transactions against the clone, the constraint is 'checked', and violating
 * transactions fail.
 *
 * Constraints are also 'applied' for incoming updates from other clones. This
 * is because a constraint may be violated as a result of data changes in either
 * clone. In this case, the constraint must resolve the violation by application
 * of some rule.
 *
 * > 🚧 *Data constraints are currently an experimental feature. Please
 * > [contact&nbsp;us](mailto:info@m-ld.io) to discuss constraints required for
 * > your use-case.*
 *
 * In this clone engine, constraints are checked and applied for updates prior
 * to their application to the data (the updates are 'provisional'). If the
 * constraint requires to know the final state, it must infer it from the given
 * reader and the update.
 *
 * @see http://m-ld/org/doc/#concurrency
 */
export interface MeldConstraint {
  /**
   * Check the given update does not violate this constraint.
   * @param update the provisional update, prior to application to the data
   * @param read a way to read data from the clone at the time of the update
   * @returns a rejection if the constraint is violated (or fails)
   */
  check(update: MeldUpdate, read: MeldReader): Promise<unknown>;
  /**
   * Applies the constraint to an update being applied to the data. If the
   * update would cause a violation, this method must provide an Update which
   * resolves the violation.
   * @param update the provisional update, prior to application to the data
   * @param read a way to read data from the clone at the time of the update
   * @returns `null` if no violation is found. Otherwise, an Update the resolves
   * the violation so that the constraint invariant is upheld.
   */
  apply(update: MeldUpdate, read: MeldReader): Promise<Update | null>;
}