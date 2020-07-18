import { MeldRemotes, DeltaMessage, MeldLocal, LiveValue } from '../src/m-ld';
import { mock, MockProxy } from 'jest-mock-extended';
import { Observable, NEVER, BehaviorSubject, from, asapScheduler } from 'rxjs';
import { Dataset, QuadStoreDataset } from '../src/dataset';
import MemDown from 'memdown';
import { TreeClock } from '../src/clocks';
import { AsyncMqttClient, IPublishPacket } from 'async-mqtt';
import { EventEmitter } from 'events';
import { observeOn } from 'rxjs/operators';
import { MeldConfig } from '../src';
import { AbstractLevelDOWN } from 'abstract-leveldown';

export function testConfig(config?: Partial<MeldConfig>): MeldConfig {
  return { '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, ...config };
}

export function mockRemotes(
  updates: Observable<DeltaMessage> = NEVER,
  lives: Array<boolean | null> | LiveValue<boolean | null> = [true],
  newClock: TreeClock = TreeClock.GENESIS): MeldRemotes {
  // This weirdness is due to jest-mock-extended trying to mock arrays
  return {
    ...mock<MeldRemotes>(),
    setLocal: () => { },
    updates,
    live: Array.isArray(lives) ? hotLive(lives) : lives,
    newClock: () => Promise.resolve(newClock)
  };
}

export function hotLive(lives: Array<boolean | null>): BehaviorSubject<boolean | null> {
  const live = new BehaviorSubject(lives[0]);
  from(lives.slice(1)).pipe(observeOn(asapScheduler)).forEach(v => live.next(v));
  return live;
}

export async function memStore(
  leveldown: AbstractLevelDOWN = new MemDown): Promise<Dataset> {
  return new QuadStoreDataset(leveldown);
}

export function mockLocal(
  impl?: Partial<MeldLocal>, lives: Array<boolean | null> = [true]): MeldLocal {
  // This weirdness is due to jest-mock-extended trying to mock arrays
  return { ...mock<MeldLocal>(), updates: NEVER, live: hotLive(lives), ...impl };
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