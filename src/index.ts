import { QuadStoreDataset } from './engine/dataset';
import { DatasetEngine } from './engine/dataset/DatasetEngine';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { ApiStateMachine } from "./engine/MeldState";
import { LogLevelDesc } from 'loglevel';
import { ConstraintConfig, constraintFromConfig } from './constraints';
import { DomainContext } from './engine/MeldEncoding';
import { Context } from './jrql-support';
import { MeldClone, MeldConstraint } from './api';

export {
  Pattern, Reference, Context, Variable, Value, Describe,
  Group, Query, Read, Result, Select, Subject, Update,
  isRead, isWrite
} from './jrql-support';

export * from './util';
export * from './api';

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
   * An upper bound on delta message size, in bytes. Usually imposed by the
   * message publishing implementation. Default is infinity. Exceeding this
   * limit will cause a transaction to fail, to prevent a clone from being
   * unable to transmit the update to its peers.
   */
  maxDeltaSize?: number;
  /**
   * Log level for the clone
   */
  logLevel?: LogLevelDesc;
}

/**
 * Create or initialise a local clone, depending on whether the given LevelDB
 * database already exists. This function returns as soon as it is safe to begin
 * transactions against the clone; this may be before the clone has received all
 * updates from the domain. To await latest updates [TODO]
 * 
 * @param ldb an instance of a leveldb backend
 * @param remotes a driver for connecting to remote m-ld clones on the domain.
 * This can be a configured object (e.g. `new MqttRemotes(config)`) or just the
 * class (`MqttRemotes`).
 * @param config the clone configuration
 * @param handler Handle updates from the domain. All data changes are signalled
 * through this handler, strictly ordered according to the clone's logical
 * clock. The updates can therefore be correctly used to maintain some other
 * view of data, for example in a user interface or separate database. This will
 * include the notification of 'rev-up' updates after a connect to the domain.
 * To change this behaviour, subscribe to `status` changes and ignore updates
 * while the status is marked as `outdated`.
 * @param constraint a constraint implementation, overrides config constraint.
 * 🚧 Experimental: use with caution.
 */
export async function clone(
  ldb: AbstractLevelDOWN,
  remotes: dist.mqtt.MqttRemotes | dist.ably.AblyRemotes,
  config: MeldConfig,
  constraint?: MeldConstraint): Promise<MeldClone> {

  const context = new DomainContext(config['@domain'], config['@context']);
  const dataset = new QuadStoreDataset(ldb, config.ldbOpts);

  if (typeof remotes == 'function')
    remotes = new remotes(config);

  if (constraint == null && config.constraint != null)
    constraint = await constraintFromConfig(config.constraint, context);

  const engine = new DatasetEngine({ dataset, remotes, config, constraint });
  await engine.initialise();
  const api = new ApiStateMachine(context, engine);
  return {
    get: api.get,
    read: api.read,
    write: api.write,
    delete: api.delete,
    status: engine.status,
    close: engine.close
  };
}

// The m-ld remotes API is not yet public, so here we just declare the available
// modules for documentation purposes.
declare module dist {
  module mqtt {
    /**
     * MQTT remotes implementation, see [MQTT Remotes](/#mqtt-remotes)
     */
    export type MqttRemotes = any;
  }
  module ably {
    /**
     * Ably remotes implementation, see [Ably Remotes](/#ably-remotes)
     */
    export type AblyRemotes = any;
  }
}