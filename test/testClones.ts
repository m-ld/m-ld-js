import { EncodedOperation, MeldLocal, MeldRemotes, OperationMessage } from '../src/engine';
import { mock, MockProxy } from 'jest-mock-extended';
import { asapScheduler, BehaviorSubject, from, NEVER, Observable, Observer } from 'rxjs';
import { Dataset, QuadStoreDataset } from '../src/engine/dataset';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { AsyncMqttClient, IPublishPacket } from 'async-mqtt';
import { EventEmitter } from 'events';
import { observeOn } from 'rxjs/operators';
import { MeldConfig } from '../src';
import { AbstractLevelDOWN } from 'abstract-leveldown';
import { LiveValue } from '../src/engine/LiveValue';
import { Context } from 'jsonld/jsonld-spec';
import { MeldMemDown } from '../src/memdown';

export function testConfig(config?: Partial<MeldConfig>): MeldConfig {
  return { '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, ...config };
}

export function mockRemotes(
  updates: Observable<OperationMessage> = NEVER,
  lives: Array<boolean | null> | LiveValue<boolean | null> = [false],
  newClock: TreeClock = TreeClock.GENESIS): MeldRemotes {
  // This weirdness is due to jest-mock-extended trying to mock arrays
  return {
    ...mock<MeldRemotes>(),
    setLocal: () => {},
    operations: updates,
    live: Array.isArray(lives) ? hotLive(lives) : lives,
    newClock: () => Promise.resolve(newClock)
  };
}

export function hotLive(lives: Array<boolean | null>): BehaviorSubject<boolean | null> {
  const live = new BehaviorSubject(lives[0]);
  from(lives.slice(1)).pipe(observeOn(asapScheduler)).forEach(v => live.next(v));
  return live;
}

export async function memStore(opts?: {
  backend?: AbstractLevelDOWN,
  context?: Context
}): Promise<Dataset> {
  return new QuadStoreDataset(
    opts?.backend ?? new MeldMemDown,
    opts?.context).initialise();
}

export function mockLocal(
  impl?: Partial<MeldLocal>, lives: Array<boolean | null> = [true]):
  MeldLocal & { liveSource: Observer<boolean | null> } {
  const live = hotLive(lives);
  // This weirdness is due to jest-mock-extended trying to mock arrays
  return { ...mock<MeldLocal>(), operations: NEVER, live, liveSource: live, ...impl };
}

/**
 * Wraps a clock and provides mock MessageService-like test mutations
 */
export class MockProcess {
  gwc: GlobalClock;

  constructor(
    public time: TreeClock,
    public prev: number = time.ticks) {
    this.gwc = GlobalClock.GENESIS.update(time);
  }

  tick(internal = false) {
    if (!internal)
      this.prev = this.time.ticks;
    this.time = this.time.ticked();
    this.gwc = this.gwc.update(this.time);
    return this;
  }

  join(clock: TreeClock) {
    this.time = this.time.update(clock);
    this.gwc = this.gwc.update(this.time);
    return this;
  }

  fork() {
    const { left, right } = this.time.forked();
    this.time = left;
    return new MockProcess(right);
  }

  sentOperation(deletes: string, inserts: string) {
    // Do not inline: this sets prev
    const op = this.operated(deletes, inserts);
    return new OperationMessage(this.prev, op);
  }

  operated(deletes: string, inserts: string): EncodedOperation {
    this.tick();
    return [2, this.time.ticks, this.time.toJSON(), deletes, inserts];
  }
}

export interface MockMqtt extends AsyncMqttClient {
  mockConnect(): void;
  mockClose(): void;
  mockPublish(topic: string, payload: Buffer | string): Promise<void>;
  mockSubscribe(subscriber: (topic: string, payload: Buffer) => void): void;
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
  mqtt.mockPublish = (topic: string, payload: Buffer | string) => {
    return new Promise<void>((resolve) => setImmediate(mqtt => {
      mqtt.emit('message', topic, payload);
      resolve();
    }, mqtt)); // Pass current mqtt in case of sync test
  };
  mqtt.mockSubscribe = (subscriber: (topic: string, payload: Buffer) => void) => {
    mqtt.on('message', (topic, payload) => subscriber(topic, payload));
  };
  mqtt.lastPublish = () => mqtt.publish.mock.results.slice(-1)[0].value;
  mqtt = mock<MockMqtt>(mqtt);
  // jest-mock-extended typing is confused by the AsyncMqttClient overloads, hence <any>
  mqtt.subscribe.mockReturnValue(<any>Promise.resolve([]));
  mqtt.unsubscribe.mockReturnValue(<any>Promise.resolve());
  mqtt.publish.mockImplementation(
    (topic, payload: Buffer | string) => <any>mqtt.mockPublish(topic, payload));
  return mqtt;
}