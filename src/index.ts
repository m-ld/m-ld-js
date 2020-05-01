import { QuadStoreDataset } from './dataset';
import { DatasetClone } from './dataset/DatasetClone';
import { generate } from 'short-uuid';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { MeldApi } from './m-ld/MeldApi';
import { Context, Resource } from './m-ld/jsonrql';
import { MqttRemotes, MeldMqttOpts } from './mqtt/MqttRemotes';
import { MeldRemotes, MeldStore } from './m-ld';
import { LogLevelDesc } from 'loglevel';

export { MeldApi };

// TODO: Refactor to make MQTT an optional specialisation
export interface MeldConfig {
  '@id'?: string;
  '@domain': string;
  '@context'?: Context;
  ldbOpts?: AbstractOpenOptions;
  mqttOpts: MeldMqttOpts;
  logLevel?: LogLevelDesc;
  reconnectDelayMillis?: number;
}

export async function clone(ldb: AbstractLevelDOWN, config: MeldConfig): Promise<MeldApi> {
  const theConfig = { ...config, '@id': config['@id'] ?? generate() };
  const remotes = await initRemotes(theConfig);
  const clone = await initLocal(ldb, theConfig, remotes);
  return new MeldApi(config['@domain'], theConfig['@context'] || null, clone);
}

async function initLocal(ldb: AbstractLevelDOWN,
  config: Resource<MeldConfig>, remotes: MeldRemotes): Promise<MeldStore> {
  const dataset = new QuadStoreDataset(ldb, { ...config.ldbOpts, id: config['@id'] });
  const clone = new DatasetClone(dataset, remotes, config);
  await clone.initialise();
  return clone;
}

async function initRemotes(config: Resource<MeldConfig>): Promise<MeldRemotes> {
  const remotes = new MqttRemotes(config['@domain'], config['@id'], {
    ...config.mqttOpts, logLevel: config.mqttOpts.logLevel ?? config.logLevel
  });
  await remotes.initialise();
  return remotes;
}

