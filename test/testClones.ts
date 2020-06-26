import { DatasetClone } from '../src/dataset/DatasetClone';
import { MeldRemotes, DeltaMessage, MeldLocal } from '../src/m-ld';
import { mock, MockProxy } from 'jest-mock-extended';
import { Observable, of, NEVER, BehaviorSubject, from, asapScheduler } from 'rxjs';
import { Dataset, QuadStoreDataset } from '../src/dataset';
import MemDown from 'memdown';
import { TreeClock } from '../src/clocks';
import { AsyncMqttClient, IPublishPacket } from 'async-mqtt';
import { EventEmitter } from 'events';
import { observeOn } from 'rxjs/operators';

export async function genesisClone(remotes?: MeldRemotes, genesis = true) {
  const clone = new DatasetClone(memStore(), remotes ?? mockRemotes(), {
    '@id': 'test', '@domain': 'test.m-ld.org', genesis
  });
  await clone.initialise();
  return clone;
}

export function mockRemotes(
  updates: Observable<DeltaMessage> = NEVER,
  onlines: Array<boolean | null> = [true],
  newClock: TreeClock = TreeClock.GENESIS): MeldRemotes {
  return {
    ...mock<MeldRemotes>(),
    setLocal: () => { },
    updates, online: hotOnline(onlines),
    newClock: () => Promise.resolve(newClock)
  };
}

function hotOnline(onlines: Array<boolean | null>) {
  const online = new BehaviorSubject(onlines[0]);
  from(onlines.slice(1)).pipe(observeOn(asapScheduler)).forEach(v => online.next(v));
  return online;
}

export function memStore(): Dataset {
  return new QuadStoreDataset(new MemDown);
}

export function mockLocal(
  updates: Observable<DeltaMessage> = NEVER,
  onlines: Array<boolean | null> = [true]): MeldLocal {
  // This weirdness is due to jest-mock-extended trying to mock arrays
  return { ...mock<MeldLocal>(), updates, online: hotOnline(onlines) };
}

export interface MockMqtt extends AsyncMqttClient {
  mockConnect(): void;
  mockClose(): void;
  mockPublish(topic: string, json: any): Promise<void>;
  mockSubscribe(subscriber: (topic: string, json: any) => void): void;
  lastPublish(): Promise<IPublishPacket>;
}

export function mockMqtt(): MockMqtt & MockProxy<AsyncMqttClient> {
  let mqtt = new EventEmitter() as MockMqtt & MockProxy<AsyncMqttClient>;
  mqtt.mockConnect = () => {
    mqtt.connected = true;
    mqtt.emit('connect');
  };
  mqtt.mockClose = () => {
    mqtt.connected = false;
    mqtt.emit('close');
  };
  mqtt.mockPublish = (topic: string, json: any) => {
    return new Promise<void>((resolve) => setImmediate(mqtt => {
      mqtt.emit('message', topic,
        Buffer.from(typeof json === 'string' ? json : JSON.stringify(json)));
      resolve();
    }, mqtt)); // Pass current mqtt in case of sync test
  };
  mqtt.mockSubscribe = (subscriber: (topic: string, json: any) => void) => {
    mqtt.on('message', (topic, payload) => {
      try {
        subscriber(topic, JSON.parse(payload.toString()));
      } catch (err) {
        subscriber(topic, payload.toString());
      }
    });
  };
  mqtt.lastPublish = () => mqtt.publish.mock.results.slice(-1)[0].value;
  mqtt = mock<MockMqtt>(mqtt);
  // jest-mock-extended typing is confused by the AsyncMqttClient overloads, hence <any>
  mqtt.subscribe.mockReturnValue(<any>Promise.resolve([]));
  mqtt.publish.mockImplementation((topic, msg) => <any>mqtt.mockPublish(topic, <string>msg));
  return mqtt;
}