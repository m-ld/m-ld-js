import {
  BufferEncoding, EncodedOperation, MeldLocal, MeldRemotes, Revup, Snapshot
} from '../src/engine';
import { mock, MockProxy } from 'jest-mock-extended';
import { asapScheduler, BehaviorSubject, from, NEVER, Observable, Observer } from 'rxjs';
import { Dataset, Patch, PatchQuads, QuadStoreDataset } from '../src/engine/dataset';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { AsyncMqttClient, IPublishPacket } from 'async-mqtt';
import { EventEmitter } from 'events';
import { observeOn } from 'rxjs/operators';
import {
  Attribution, Context, InterimUpdate, MeldConfig, MeldConstraint, MeldExtensions, MeldPreUpdate,
  MeldReadState, StateManaged, StateProc, Write
} from '../src';
import { AbstractLevelDOWN } from 'abstract-leveldown';
import { LiveValue } from '../src/engine/api-support';
import { MeldMemDown } from '../src/memdown';
import { Future, MsgPack } from '../src/engine/util';
import { DatasetSnapshot } from '../src/engine/dataset/SuSetDataset';
import { ClockHolder } from '../src/engine/messages';
import { DomainContext } from '../src/engine/MeldEncoding';
import { JrqlGraph } from '../src/engine/dataset/JrqlGraph';
import { ActiveContext, activeCtx } from '../src/engine/jsonld';
import { InterimUpdatePatch } from '../src/engine/dataset/InterimUpdatePatch';
import { MeldOperationMessage } from '../src/engine/MeldOperationMessage';

export function testConfig(config?: Partial<MeldConfig>): MeldConfig {
  return { '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, ...config };
}

export const testContext = new DomainContext('test.m-ld.org');

export const testExtensions = (ext?: MeldExtensions): StateManaged<MeldExtensions> => ({
  ready: () => Promise.resolve(ext ?? {})
});

export function mockRemotes(
  updates: Observable<MeldOperationMessage> = NEVER,
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

export class MockRemotes implements MeldRemotes {
  live: LiveValue<boolean>;
  operations: Observable<MeldOperationMessage>;
  newClock: () => Promise<TreeClock>;
  revupFrom: (time: TreeClock, state: MeldReadState) => Promise<Revup | undefined>;
  snapshot: (state: MeldReadState) => Promise<Snapshot>;
  setLocal: (clone: MeldLocal | null) => void;

  constructor() {
    Object.assign(this, mockRemotes());
  }
}

export function hotLive(lives: Array<boolean | null>): BehaviorSubject<boolean | null> {
  const live = new BehaviorSubject(lives[0]);
  from(lives.slice(1)).pipe(observeOn(asapScheduler)).subscribe(v => live.next(v));
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

type GraphStateWriteOpts = { updateType?: 'user' | 'internal', constraint?: MeldConstraint };

export class MockGraphState {
  static async create({ dataset, context }: { dataset?: Dataset, context?: Context } = {}) {
    context ??= testContext;
    return new MockGraphState(
      await MockState.create({ dataset, context }),
      await activeCtx(context ?? {}));
  }

  readonly graph: JrqlGraph;

  protected constructor(
    readonly state: MockState,
    readonly ctx: ActiveContext
  ) {
    this.graph = new JrqlGraph(state.dataset.graph());
  }

  async write(
    request: Write,
    opts?: MeldConstraint | GraphStateWriteOpts
  ): Promise<MeldPreUpdate> {
    const { constraint, updateType }: GraphStateWriteOpts =
      opts != null ? ('check' in opts ? { constraint: opts } : opts) : {};
    const update = new Future<MeldPreUpdate>();
    await this.state.write(async () => {
      const patch = await this.graph.write(request, this.ctx);
      const interim = new InterimUpdatePatch(
        this.graph, this.ctx, patch, null, null, { mutable: true });
      await constraint?.check(this.graph.asReadState, interim);
      const txn = await interim.finalise();
      update.resolve(updateType === 'user' ? txn.userUpdate : txn.internalUpdate);
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
    latch: async <T>(procedure: StateProc<MeldReadState, T>) => procedure(mock()),
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

  sentOperation(
    deletes: object,
    inserts: object,
    { agree, attr }: { agree?: true, attr?: Attribution } = {}
  ) {
    // Do not inline: this sets prev
    const op = this.operated(deletes, inserts, agree);
    return MeldOperationMessage.fromOperation(this.prev, op, attr ?? null);
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
  mockPublish(topic: string, payload: any): Promise<void>;
  mockSubscribe(subscriber: (topic: string, payload: any) => void): void;
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
  mqtt.mockPublish = (topic: string, payload: any) => {
    return new Promise<void>(resolve => setImmediate(mqtt => {
      mqtt.emit('message', topic, payload);
      resolve();
    }, mqtt)); // Pass current mqtt in case of sync test
  };
  mqtt.mockSubscribe = (subscriber: (topic: string, payload: any) => void) => {
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

export function mockInterim(update: MeldPreUpdate) {
  // Passing an implementation into the mock adds unwanted properties
  return Object.assign(mock<InterimUpdate>(), { update: Promise.resolve(update) });
}