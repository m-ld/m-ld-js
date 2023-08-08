import {
  BufferEncoding, EncodedOperation, MeldLocal, MeldRemotes, OperationMessage, Revup, Snapshot
} from '../src/engine';
import { mock, mockFn, MockProxy } from 'jest-mock-extended';
import {
  asapScheduler, BehaviorSubject, EMPTY, from, NEVER, Observable, ObservableInput, Observer
} from 'rxjs';
import { Dataset, Patch, PatchQuads, QuadStoreDataset, TxnContext } from '../src/engine/dataset';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { AsyncMqttClient, IPublishPacket } from 'async-mqtt';
import { EventEmitter } from 'events';
import { observeOn } from 'rxjs/operators';
import {
  Attribution, Context, GraphSubject, GraphSubjects, IndirectedData, InterimUpdate, MeldConfig,
  MeldConstraint, MeldPreUpdate, MeldReadState, StateProc, SubjectsUpdate, Write
} from '../src';
import { AbstractLevel } from 'abstract-level';
import { LiveValue } from '../src/engine/api-support';
import { MemoryLevel } from 'memory-level';
import * as MsgPack from '../src/engine/msgPack';
import { ClockHolder } from '../src/engine/messages';
import { DomainContext, MeldEncoder } from '../src/engine/MeldEncoding';
import { JrqlGraph } from '../src/engine/dataset/JrqlGraph';
import { InterimUpdatePatch } from '../src/engine/dataset/InterimUpdatePatch';
import { MeldOperationMessage } from '../src/engine/MeldOperationMessage';
import { Future } from '../src/engine/Future';
import { SubjectGraph } from '../src/engine/SubjectGraph';
import { TidsStore } from '../src/engine/dataset/TidsStore';
import { JsonldContext } from '../src/engine/jsonld';
import { JrqlQuads } from '../src/engine/dataset/JrqlQuads';
import { Stopwatch } from '../src/engine/Stopwatch';
import { CacheFactory } from '../src/engine/cache';
import { SHARED } from '../src/engine/locks';

export const testDomain = 'test.m-ld.org';
export const testDomainContext = new DomainContext(testDomain);
export const testContext = JsonldContext.active(testDomainContext);
export function testConfig(config?: Partial<MeldConfig>): MeldConfig {
  return {
    '@id': 'test',
    '@domain': testDomain,
    genesis: true,
    networkTimeout: 50,
    ...config
  };
}

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
    newClock: mockFn().mockResolvedValue(newClock)
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
  backend?: AbstractLevel<any>,
  domain?: string
}): Promise<Dataset> {
  return new QuadStoreDataset(
    opts?.domain ?? testDomain,
    opts?.backend ?? new MemoryLevel()
  ).initialise();
}

export class MockState {
  static async create({ dataset, domain }: { dataset?: Dataset, domain?: string } = {}) {
    dataset ??= await memStore({ domain });
    return new MockState(
      dataset,
      await dataset.lock.acquire('state', 'test', SHARED)
    );
  }

  txnId = 0;

  protected constructor(
    readonly dataset: Dataset,
    readonly close: () => void
  ) {}

  write(txn: () => Promise<Patch>) {
    return this.dataset.transact({
      prepare: async () => ({ patch: await txn() })
    });
  }

  newTxnContext() {
    const id = `${++this.txnId}`;
    const txc = new class extends EventEmitter implements TxnContext {
      sw = new Stopwatch('txn', id);
      id = id;
    }();
    return txc;
  }
}

type GraphStateWriteOpts = {
  updateType?: 'user' | 'internal',
  constraint?: MeldConstraint
};

export class MockGraphState {
  static async create({ dataset, context, domain, indirectedData, cacheFactory }: {
    dataset?: Dataset,
    context?: Context,
    domain?: string,
    indirectedData?: IndirectedData,
    cacheFactory?: CacheFactory
  } = {}) {
    return new MockGraphState(
      await MockState.create({ dataset, domain }),
      await (context ? JsonldContext.active(context) : testContext),
      indirectedData ?? (() => undefined),
      // Turn caching off by default
      cacheFactory ?? new CacheFactory({ max: 0 })
    );
  }

  readonly graph: JrqlGraph;
  tidsStore: TidsStore;

  protected constructor(
    readonly state: MockState,
    readonly ctx: JsonldContext,
    readonly indirectedData: IndirectedData,
    cacheFactory: CacheFactory
  ) {
    const graph = state.dataset.graph();
    const quads = new JrqlQuads(graph, indirectedData, cacheFactory);
    this.graph = new JrqlGraph(graph, quads);
    this.tidsStore = mock();
  }

  async write(
    request: Write,
    opts?: MeldConstraint | GraphStateWriteOpts
  ): Promise<MeldPreUpdate> {
    const { constraint, updateType }: GraphStateWriteOpts =
      opts != null ? ('check' in opts ? { constraint: opts } : opts) : {};
    const update = new Future<MeldPreUpdate>();
    await this.state.write(async () => {
      const txc = this.state.newTxnContext();
      const patch = await this.graph.write(request, this.ctx, txc);
      const interim = new InterimUpdatePatch(
        patch,
        this.graph,
        this.tidsStore,
        this.ctx,
        null,
        null,
        txc,
        { mutable: true }
      );
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
  opts: {
    from?: number,
    principalId?: string,
    agreed?: [number, any],
    updates?: object
  } = {}
): EncodedOperation {
  const update = [deletes, inserts];
  if (opts.updates)
    update.push(opts.updates);
  return [
    5,
    opts.from ?? time.ticks,
    time.toJSON(),
    MsgPack.encode(update),
    [BufferEncoding.MSGPACK],
    opts.principalId ?? null,
    opts.agreed ?? null
  ];
}

export function decodeOpUpdate(op: OperationMessage): [{}, {}] {
  return MeldEncoder.jsonFromBuffer(
    op.data[EncodedOperation.Key.update],
    op.data[EncodedOperation.Key.encoding]);
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
    { agree, attr, updates }: { agree?: true, attr?: Attribution, updates?: object } = {}
  ): MeldOperationMessage {
    // Do not inline: this sets prev
    const op = this.operated(deletes, inserts, { agree, pid: attr?.pid, updates });
    return MeldOperationMessage.fromOperation(this.prev, op, attr ?? null);
  }

  operated(
    deletes: object,
    inserts: object,
    opts: { agree?: any, pid?: string, updates?: object } = {}
  ): EncodedOperation {
    this.tick();
    let agreed: [number, any] | undefined;
    if (opts.agree) {
      this.agreed = this.time;
      agreed = [this.time.ticks, opts.agree];
    }
    return testOp(this.time, deletes, inserts, {
      agreed, principalId: opts.pid, updates: opts.updates
    });
  }

  revup(updates: Observable<OperationMessage> = EMPTY): Revup {
    return {
      gwc: this.gwc,
      updates,
      cancel: mockFn()
    };
  }

  snapshot(
    data: ObservableInput<Snapshot.Datum> = [],
    updates: Observable<OperationMessage> = EMPTY
  ): Snapshot {
    return {
      gwc: this.gwc,
      agreed: this.agreed,
      data: from(data),
      cancel: mockFn(),
      updates
    };
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

export function mockUpdate(update?: SubjectsUpdate) {
  return {
    '@delete': new SubjectGraph(update?.['@delete'] ?? []),
    '@insert': new SubjectGraph(update?.['@insert'] ?? []),
    '@update': new SubjectGraph(update?.['@update'] ?? [])
  };
}

export function mockInterim(
  // Allow undefined or plain array @delete & @insert, for readability
  update: Partial<{
    [key in keyof MeldPreUpdate]: MeldPreUpdate[key] extends GraphSubjects ?
      Array<GraphSubject> : MeldPreUpdate[key]
  }>
) {
  // Passing an implementation into the mock adds unwanted properties
  return Object.assign(mock<InterimUpdate>(), {
    update: Promise.resolve({ ...update, ...mockUpdate(update) }),
    hidden: mockFn().mockReturnValue(EMPTY)
  });
}

