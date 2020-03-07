import { QuadStoreDataset } from './dataset';
import { DatasetClone } from './dataset/DatasetClone';
import { generate } from 'short-uuid';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { MeldApi } from './m-ld/MeldApi';
import { Context } from './m-ld/jsonrql';
import { MqttRemotes } from './mqtt/MqttRemotes';
import { IClientOptions, connect } from 'async-mqtt';

export { MeldApi };
  
type MeldMqttOpts = Omit<IClientOptions, 'clientId' | 'will'> &
  ({ hostname: string } | { host: string, port: number })

export interface MeldConfig {
  domain: string;
  id?: string;
  genesis?: boolean;
  ldbOpts?: AbstractOpenOptions;
  context?: Context;
  mqttOpts: MeldMqttOpts;
}

export async function clone(ldb: AbstractLevelDOWN, config: MeldConfig) {
  const cloneId = config.id || generate();
  const mqttOpts = { ...config.mqttOpts, clientId: cloneId };
  const mqtt = connect(mqttOpts);
  mqtt.options = mqttOpts; // Bug in async-mqtt
  const remotes = new MqttRemotes(config.domain, mqtt);
  await remotes.initialise();

  const clone = new DatasetClone(new QuadStoreDataset(ldb, {
    ...config.ldbOpts,
    id: cloneId
  }), remotes);
  clone.genesis = !!config.genesis;
  await clone.initialise();

  return new MeldApi(config.domain, config.context || null, clone);
}
