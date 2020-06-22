import { QuadStoreDataset } from './dataset';
import { DatasetClone } from './dataset/DatasetClone';
import { generate } from 'short-uuid';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { MeldApi } from './m-ld/MeldApi';
import { Context, Reference } from './dataset/jrql-support';
import { MqttRemotes, MeldMqttOpts } from './mqtt/MqttRemotes';
import { MeldRemotes, MeldStore } from './m-ld';
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
   * logging. Must be unique among the clones for the domain. If not provided,
   * an identity will be automatically assigned.
   */
  '@id'?: string;
  /**
   * The URI domain name, which defines the universal identity of the dataset
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
   * Options for the LevelDB instance to be opened for the clone data
   */
  ldbOpts?: AbstractOpenOptions;
  /**
   * Options for the MQTT message bus
   */
  // TODO: Refactor to make MQTT an optional specialisation
  mqttOpts: MeldMqttOpts;
  /**
   * An upper bound on how long any to wait for a response over the network.
   * Used for message send timeouts and to trigger fallback behaviours.
   */
  networkTimeout?: number;
  /**
   * Log level for the clone
   */
  logLevel?: LogLevelDesc;
}

/**
 * Create or initialise a local clone, depending on whether the given LevelDB
 * database already exists. This function returns as soon as it is safe to
 * begin transactions against the clone; this may be before the clone has
 * received all updates from the domain. To await latest updates, await a call
 * to the {@link MeldApi.latest} method.
 * @param ldb an instance of a leveldb backend
 * @param config the clone configuration
 */
export async function clone(ldb: AbstractLevelDOWN, config: MeldConfig): Promise<MeldApi> {
  const theConfig = { ...config, '@id': config['@id'] ?? generate() };
  const clone = await initLocal(ldb, theConfig, initRemotes(theConfig));
  return new MeldApi(config['@domain'], theConfig['@context'] || null, clone);
}

async function initLocal(ldb: AbstractLevelDOWN,
  config: Reference & MeldConfig, remotes: MeldRemotes): Promise<MeldStore> {
  const dataset = new QuadStoreDataset(ldb, config.ldbOpts);
  const clone = new DatasetClone(config['@id'], dataset, remotes, config);
  await clone.initialise();
  return clone;
}

function initRemotes(config: Reference & MeldConfig): MeldRemotes {
  return new MqttRemotes(config['@domain'], config['@id'], {
    sendTimeout: config.networkTimeout,
    logLevel: config.logLevel,
    ...config.mqttOpts
  });
}

