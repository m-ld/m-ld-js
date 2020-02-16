import { QuadStoreDataset } from './dataset';
import { DatasetClone } from './dataset/DatasetClone';
import { generate } from 'short-uuid';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { MeldApi } from './m-ld/MeldApi';
import { Context } from './m-ld/jsonrql';
import { MqttRemotes } from './mqtt/MqttRemotes';
import { MqttClient } from 'mqtt';
import { AsyncClient } from 'async-mqtt';

export { MeldApi };

export interface MeldConfig {
  domain: string;
  id?: string;
  genesis?: boolean;
  ldbOpts?: AbstractOpenOptions;
  context?: Context;
}

export async function clone(ldb: AbstractLevelDOWN, mqtt: MqttClient, config: MeldConfig) {
  const remotes = new MqttRemotes(config.domain, new AsyncClient(mqtt));
  await remotes.initialise();

  const clone = new DatasetClone(new QuadStoreDataset(ldb, {
    ...config.ldbOpts,
    id: config.id || generate()
  }), remotes);
  clone.genesis = !!config.genesis;
  await clone.initialise();
  
  return new MeldApi(config.domain, config.context || null, clone);
}
