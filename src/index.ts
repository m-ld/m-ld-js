import { QuadStoreDataset } from './dataset';
import { DatasetClone } from './dataset/DatasetClone';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { MeldApi } from './m-ld/MeldApi';
import { Context } from './dataset/jrql-support';
import { MeldRemotes } from './m-ld';
import { LogLevelDesc } from 'loglevel';

export * from './m-ld/MeldApi';
export { shortId } from './util';
export {
  Pattern, Reference, Context, Variable, Value, Describe,
  Group, Query, Read, Result, Select, Subject, Update
} from './dataset/jrql-support';

/**
 * m-ld clone configuration
 */
export interface MeldConfig {
  /**
   * The local identity of the m-ld clone, used for message bus identity and
   * logging. Must be unique among the clones for the domain. For convenience,
   * you can use the {@link uuid} function.
   */
  '@id': string;
  /**
   * A URI domain name, which defines the universal identity of the dataset
   * being manipulated by a set of clones (for example, on the configured
   * message bus).
   */
  '@domain': string;
  /**
   * An optional JSON-LD context for the domain data. If not specified:
   * * `@base` defaults to `http://{domain}`
   * * `@vocab` defaults to the resolution of `/#` against the base
   */
  '@context'?: Context;
  /**
   * Set to `true` to indicate that this clone will be 'genesis'; that is, the
   * first new clone on a new domain. This flag will be ignored if the clone is
   * not new. If `false`, and this clone is new, successful clone initialisation
   * is dependent on the availability of another clone. If set to true, and
   * subsequently another non-new clone appears on the same domain, either or
   * both clones will become permanently 'siloed', that is, unable to
   * participate further in the domain.
   */
  genesis: boolean;
  /**
   * Options for the LevelDB instance to be opened for the clone data
   */
  ldbOpts?: AbstractOpenOptions;
  /**
   * An sane upper bound on how long any to wait for a response over the
   * network. Used for message send timeouts and to trigger fallback behaviours.
   * Default is five seconds.
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
export { generate as uuid } from 'short-uuid';

/**
 * Create or initialise a local clone, depending on whether the given LevelDB
 * database already exists. This function returns as soon as it is safe to
 * begin transactions against the clone; this may be before the clone has
 * received all updates from the domain. To await latest updates, await a call
 * to the {@link MeldApi.latest} method.
 * @param ldb an instance of a leveldb backend
 * @param remotes a driver for connecting to remote m-ld clones on the domain
 * @param config the clone configuration
 */
export async function clone(ldb: AbstractLevelDOWN, remotes: MeldRemotes, config: MeldConfig): Promise<MeldApi> {
  const dataset = new QuadStoreDataset(ldb, config.ldbOpts);
  const clone = new DatasetClone(dataset, remotes, config);
  await clone.initialise();
  return new MeldApi(config['@domain'], config['@context'] ?? null, clone);
}