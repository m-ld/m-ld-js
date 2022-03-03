import {
  BufferEncoding, EncodedOperation, MeldLocal, MeldRemotes, OperationMessage, Snapshot
} from '../src/engine';
import { mock, MockProxy } from 'jest-mock-extended';
import { asapScheduler, BehaviorSubject, from, NEVER, Observable, Observer } from 'rxjs';
import { Dataset, Patch, PatchQuads, QuadStoreDataset } from '../src/engine/dataset';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { AsyncMqttClient, IPublishPacket } from 'async-mqtt';
import { EventEmitter } from 'events';
import { observeOn } from 'rxjs/operators';
import { MeldConfig, MeldConstraint, MeldReadState, MeldUpdateBid, StateProc } from '../src';
import { AbstractLevelDOWN } from 'abstract-leveldown';
import { LiveValue } from '../src/engine/LiveValue';
import { MeldMemDown } from '../src/memdown';
import { Future, MsgPack } from '../src/engine/util';
import { DatasetSnapshot } from '../src/engine/dataset/SuSetDataset';
import { ClockHolder } from '../src/engine/messages';
import { DomainContext } from '../src/engine/MeldEncoding';
import { JrqlGraph } from '../src/engine/dataset/JrqlGraph';
import { ActiveContext } from 'jsonld/lib/context';
import { activeCtx } from '../src/engine/jsonld';
import { Context, Write } from '../src/jrql-support';
import { InterimUpdatePatch } from '../src/engine/dataset/InterimUpdatePatch';

export function testConfig(config?: Partial<MeldConfig>): MeldConfig {
  return { '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, ...config };
}

export const testContext = new DomainContext('test.m-ld.org');

export function mockRemotes(
  updates: Observable<OperationMessage> = NEVER,
  lives: Array<boolean | null> | LiveValue<boolean | null> = [false],
  newClock: TreeClock = TreeClock.GENESIS
): MeldRemotes {
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

export class MockState {
  static async create({ dataset, context }: { dataset?: Dataset, context?: Context } = {}) {
    dataset ??= await memStore({ context });
    return new MockState(dataset,
      await dataset.lock.acquire('state', 'test', 'share'));
  }

  protected constructor(
    readonly dataset: Dataset,
    readonly close: () => void
  ) {}

  write(txn: () => Promise<Patch>) {
    return this.dataset.transact({
      prepare: async () => ({ patch: await txn() })
    });
  }
}

export class MockGraphState {
  static async create({ dataset, context }: { dataset?: Dataset, context?: Context } = {}) {
    return new MockGraphState(
      await MockState.create({ dataset, context }),
      await activeCtx(context ?? {}));
  }

  readonly jrqlGraph: JrqlGraph;

  protected constructor(
    readonly state: MockState,
    readonly ctx: ActiveContext
  ) {
    this.jrqlGraph = new JrqlGraph(state.dataset.graph());
  }

  async write(request: Write, constraint?: MeldConstraint): Promise<MeldUpdateBid> {
    const update = new Future<MeldUpdateBid>();
    await this.state.write(async () => {
      const patch = await this.jrqlGraph.write(request, this.ctx);
      const interim = new InterimUpdatePatch(
        this.jrqlGraph, this.ctx, patch, null, null, { mutable: true });
      if (constraint != null)
        await constraint.check(this.jrqlGraph.asReadState, interim);
      const txn = await interim.finalise();
      update.resolve(txn.internalUpdate);
      return new PatchQuads(txn.assertions).append(txn.entailments);
    });
    return Promise.resolve(update);
  }

  close() {
    this.state.close();
  }
}

export function mockLocal(
  impl?: Partial<MeldLocal>, lives: Array<boolean | null> = [true]):
  MeldLocal & { liveSource: Observer<boolean | null> } {
  const live = hotLive(lives);
  // This weirdness is due to jest-mock-extended trying to mock arrays
  return {
    ...mock<MeldLocal>(),
    operations: NEVER,
    live,
    liveSource: live,
    withLocalState: async <T>(procedure: StateProc<MeldReadState, T>) => procedure(mock()),
    ...impl
  };
}

export function testOp(
  time: TreeClock,
  deletes: object = {},
  inserts: object = {},
  { from, principalId, agreed }: {
    from?: number, principalId?: string, agreed?: [number, any]
  } = {}
): EncodedOperation {
  return [
    4,
    from ?? time.ticks,
    time.toJSON(),
    MsgPack.encode([deletes, inserts]),
    [BufferEncoding.MSGPACK],
    principalId ?? null,
    agreed ?? null
  ];
}

/**
 * Wraps a clock and provides mock MessageService-like test mutations
 */
export class MockProcess implements ClockHolder<TreeClock> {
  agreed = TreeClock.GENESIS;

  constructor(
    public time: TreeClock,
    public prev: number = time.ticks,
    public gwc = GlobalClock.GENESIS.set(time)
  ) {
  }

  event(): TreeClock {
    // CAUTION: not necessarily internal
    this.tick(true);
    return this.time;
  }

  peek(): TreeClock {
    return this.time;
  }

  push(time: TreeClock) {
    this.time = time;
    return this;
  }

  tick(internal = false) {
    if (!internal)
      this.prev = this.time.ticks;
    this.time = this.time.ticked();
    if (!internal)
      this.gwc = this.gwc.set(this.time);
    return this;
  }

  join(clock: TreeClock) {
    this.time = this.time.update(clock);
    this.gwc = this.gwc.set(clock);
    return this;
  }

  fork() {
    const { left, right } = this.time.forked();
    this.time = left;
    return new MockProcess(right, this.prev, this.gwc);
  }

  sentOperation(deletes: object, inserts: object, agree?: true) {
    // Do not inline: this sets prev
    const op = this.operated(deletes, inserts, agree);
    return OperationMessage.fromOperation(this.prev, op, null);
  }

  operated(deletes: object, inserts: object, agree?: any): EncodedOperation {
    this.tick();
    let agreed: [number, any] | undefined;
    if (agree) {
      this.agreed = this.time;
      agreed = [this.time.ticks, agree];
    }
    return testOp(this.time, deletes, inserts, { agreed });
  }

  snapshot(data: Snapshot.Datum[]): DatasetSnapshot {
    return { gwc: this.gwc, agreed: this.agreed, data: from(data) };
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
    return new Promise<void>(resolve => setImmediate(mqtt => {
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
  mqtt.end.mockImplementation(() => <any>mqtt.mockClose());
  return mqtt;
}
