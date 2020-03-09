import { QuadStoreDataset } from './dataset';
import { DatasetClone } from './dataset/DatasetClone';
import { generate } from 'short-uuid';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { MeldApi } from './m-ld/MeldApi';
import { Context } from './m-ld/jsonrql';
import { MqttRemotes } from './mqtt/MqttRemotes';
import { IClientOptions, connect } from 'async-mqtt';
import { MeldRemotes, MeldStore } from './m-ld';

export { MeldApi };

type MeldMqttOpts = Omit<IClientOptions, 'clientId' | 'will'> &
  ({ hostname: string } | { host: string, port: number })

export interface MeldConfig {
  id?: string;
  domain: string;
  ldbOpts?: AbstractOpenOptions;
  mqttOpts: MeldMqttOpts;
  context?: Context;
}

export async function clone(ldb: AbstractLevelDOWN, config: MeldConfig): Promise<MeldApi> {
  const theConfig = { ...config, id: config.id || generate() };
  const remotes = await initRemotes(theConfig);
  const clone = await initLocal(ldb, theConfig, remotes);
  return new MeldApi(config.domain, theConfig.context || null, clone);
}

async function initLocal(ldb: AbstractLevelDOWN,
  config: MeldConfig & { id: string }, remotes: MeldRemotes): Promise<MeldStore> {
  const clone = new DatasetClone(new QuadStoreDataset(
    ldb, { ...config.ldbOpts, id: config.id }), remotes);
  await clone.initialise();
  return clone;
}

async function initRemotes(config: MeldConfig): Promise<MeldRemotes> {
  const mqttOpts = { ...config.mqttOpts, clientId: config.id };
  const mqtt = connect(mqttOpts);
  mqtt.options = mqttOpts; // Bug in async-mqtt
  const remotes = new MqttRemotes(config.domain, mqtt);
  await remotes.initialise();
  return remotes;
}

