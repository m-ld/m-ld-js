import { toPrefixedId } from './SuSetGraph';
import { EncodedOperation, OperationMessage } from '..';
import { GlobalClock, GlobalClockJson, TreeClock, TreeClockJson } from '../clocks';
import { MsgPack } from '../util';
import { Kvps, KvpStore } from '.';
import { MeldEncoder, MeldOperation } from '../MeldEncoding';
import { CausalOperation, CausalOperator } from '../ops';
import { from, Observable } from 'rxjs';
import { filter, mergeMap } from 'rxjs/operators';
import { Triple } from '../quads';

/** There is only one journal with a fixed key. */
const JOURNAL_KEY = '_qs:journal';

/**
 * Journal entries are indexed by end-tick as
 * `_qs:tick:${zeroPad(tick.toString(36), 8)}`. This gives a maximum tick of
 * 36^8, about 3 trillion, about 90 years in milliseconds.
 */
type EntryKey = ReturnType<typeof tickKey>;
const TICK_KEY_PRE = '_qs:tick';
const TICK_KEY_LEN = 8;
const TICK_KEY_RADIX = 36;
const TICK_KEY_PAD = new Array(TICK_KEY_LEN).fill('0').join('');
const TICK_KEY_MAX = toPrefixedId(TICK_KEY_PRE, '~'); // > 'z'
function tickKey(tick: number) {
  return toPrefixedId(TICK_KEY_PRE,
    `${TICK_KEY_PAD}${tick.toString(TICK_KEY_RADIX)}`.slice(-TICK_KEY_LEN));
}

/** Entries are also indexed by time hash (TID) */
const TID_KEY_PRE = '_qs:tid';

function tidKey(tid: string) {
  return toPrefixedId(TID_KEY_PRE, tid);
}

/** Operations indexed by time hash (TID) */
const TID_OP_KEY_PRE = '_qs:op';

function tidOpKey(tid: string) {
  return toPrefixedId(TID_OP_KEY_PRE, tid);
}

interface JournalJson {
  /**
   * First known tick of the local clock. Journal entries will exist for
   * subsequent ticks. Operations may be retained for transactions prior to
   * this.
   */
  start: number;
  /**
   * Current local clock time, including internal ticks.
   */
  time: TreeClockJson;
  /**
   * JSON-encoded public clock time ('global wall clock' or 'Great Westminster
   * Clock'). This has latest public ticks seen for all processes (not internal
   * ticks), unlike an entry time, which may be causally related to older
   * messages from third parties, and the journal time, which has internal ticks
   * for the local clone identity.
   */
  gwc: GlobalClockJson;
}

type JournalEntryJson = [
  /** Previous tick for this entry's clock (may be remote) */
  prev: number,
  /** Operation transaction ID */
  tid: string
];

export interface EntryBuilder {
  next(operation: MeldOperation, localTime: TreeClock): EntryBuilder;
  commit: Kvps;
}

/** Encapsulates pattern used for Journal contents */
abstract class JournalObject<J> {
  protected constructor(
    /** KVP dataset */
    protected readonly data: SuSetJournalData) {
  }
  /** Gets raw data encoded into KVP value */
  abstract get json(): J;
  /** Pushes current data into KVP dataset */
  abstract commit: Kvps;
}

/** Immutable (partial) expansion of EncodedOperation */
export class SuSetJournalOperation extends JournalObject<EncodedOperation> {
  static fromJson(data: SuSetJournalData, json: EncodedOperation, tid?: string) {
    // Destructuring fields for convenience
    const [, from, timeJson] = json;
    const time = TreeClock.fromJson(timeJson);
    tid ??= time.hash();
    return new SuSetJournalOperation(data, tid, from, time, json);
  }

  static fromOperation(data: SuSetJournalData, operation: MeldOperation) {
    return new SuSetJournalOperation(data,
      operation.time.hash(),
      operation.from,
      operation.time,
      operation.encoded)
  }

  constructor(
    data: SuSetJournalData,
    readonly tid: string,
    readonly from: number,
    readonly time: TreeClock,
    readonly operation: EncodedOperation) {
    super(data);
  }

  get json() {
    return this.operation;
  }

  commit: Kvps = batch => {
    batch.put(tidOpKey(this.tid), MsgPack.encode(this.operation));
  };

  get tick() {
    return this.time.ticks;
  }

  asMeldOperation() {
    return this.data.decode(this.operation);
  }

  /**
   * @param createOperator creates a causal reduction operator from the given first operation
   * @param minFrom the least required value of the range of the operation with
   * the returned identity
   * @returns found operations, up to and including this one
   */
  async causalReduce(
    createOperator: (first: MeldOperation) => CausalOperator<Triple, TreeClock>,
    minFrom = 1): Promise<MeldOperation> {
    // TODO: Lock the journal against garbage compaction while processing!
    type OpId = { tick: number, tid: string };
    const seekToFrom = async (start: OpId): Promise<OpId> => {
      const prev = await this.data.entryPrev(start.tid);
      if (prev < minFrom || prev < start.tick - 1)
        return start; // This is as far back as we have
      else
        // Get previous tick for given tick (or our tick)
        return seekToFrom({ tick: prev, tid: this.time.ticked(prev).hash() });
    };
    let { tid, tick } = await seekToFrom(this);
    const operator = createOperator(await this.data.meldOperation(tid));
    while (tid !== this.tid) {
      tid = this.time.ticked(++tick).hash();
      operator.next(tid === this.tid ?
        this.asMeldOperation() : await this.data.meldOperation(tid));
    }
    return this.data.toMeldOperation(operator.commit());
  }

  /**
   * Gets the causally-contiguous history of this operation, fused into a single
   * operation.
   * @see CausalTimeRange.contiguous
   */
  async fusedPast(): Promise<EncodedOperation> {
    return (await this.causalReduce(first => first.fusion())).encoded;
  }

  async cutSeen(op: MeldOperation): Promise<MeldOperation> {
    return this.causalReduce(prev => op.cutting().next(prev), op.from);
  }
}

/** Immutable expansion of JournalEntryJson */
export class SuSetJournalEntry extends JournalObject<JournalEntryJson> {
  static async fromJson(data: SuSetJournalData, key: EntryKey, json: JournalEntryJson) {
    // Destructuring fields for convenience
    const [prev, tid] = json;
    const operation = await data.operation(tid);
    if (operation != null)
      return new SuSetJournalEntry(data, key, prev, operation);
    else
      throw missingOperationError(tid);
  }

  static fromOperation(data: SuSetJournalData,
    operation: MeldOperation, localTime: TreeClock, gwc: GlobalClock) {
    return new SuSetJournalEntry(data,
      tickKey(localTime.ticks),
      gwc.getTicks(operation.time),
      SuSetJournalOperation.fromOperation(data, operation))
  }

  private constructor(
    data: SuSetJournalData,
    readonly key: EntryKey,
    readonly prev: number,
    readonly operation: SuSetJournalOperation) {
    super(data);
  }

  get json(): JournalEntryJson {
    return [this.prev, this.operation.tid];
  }

  commit: Kvps = batch => {
    const encoded = MsgPack.encode(this.json);
    batch.put(this.key, encoded);
    batch.put(tidKey(this.operation.tid), encoded);
    this.operation.commit(batch);
  };

  async next(): Promise<SuSetJournalEntry | undefined> {
    return this.data.entryAfter(this.key);
  }

  asMessage(): OperationMessage {
    return new OperationMessage(this.prev, this.operation.json, this.operation.time);
  }
}

/** Immutable expansion of JournalJson */
export class SuSetJournal extends JournalObject<JournalJson> {
  static fromJson(data: SuSetJournalData, json: JournalJson) {
    const time = TreeClock.fromJson(json.time);
    const gwc = GlobalClock.fromJson(json.gwc);
    return new SuSetJournal(data, json.start, time, gwc);
  }

  constructor(
    data: SuSetJournalData,
    readonly start: number,
    readonly time: TreeClock,
    readonly gwc: GlobalClock) {
    super(data);
  }

  get json(): JournalJson {
    return { start: this.start, time: this.time.toJson(), gwc: this.gwc.toJson() };
  }

  commit: Kvps = batch => {
    batch.put(JOURNAL_KEY, MsgPack.encode(this.json));
    this.data._journal = this;
  };

  withTime(localTime: TreeClock, gwc?: GlobalClock): SuSetJournal {
    return new SuSetJournal(this.data, this.start, localTime, gwc ?? this.gwc);
  }

  builder(): EntryBuilder {
    const journal = this;
    return new class {
      private localTime = journal.time;
      private gwc = journal.gwc;
      private entries: SuSetJournalEntry[] = [];

      next(operation: MeldOperation, localTime: TreeClock) {
        const entry = SuSetJournalEntry.fromOperation(journal.data,
          operation, localTime, this.gwc);
        this.entries.push(entry);
        this.gwc = this.gwc.update(operation.time);
        this.localTime = localTime;
        return this;
      }

      /** Commits the built journal entries to the journal */
      commit: Kvps = async batch => {
        for (let entry of this.entries)
          entry.commit(batch);

        journal.withTime(this.localTime, this.gwc).commit(batch);
      };
    };
  }

  latestOperations(): Observable<EncodedOperation> {
    return from(this.gwc.tids()).pipe(
      // From each op, emit a fusion of all contiguous ops up to the op
      mergeMap(tid => this.data.operation(tid)),
      filter<SuSetJournalOperation>(op => op != null),
      mergeMap(op => op.fusedPast()));
  }
}

export class SuSetJournalData {
  /** Journal state cache */
  _journal: SuSetJournal | null = null;

  constructor(
    private readonly kvps: KvpStore,
    readonly encoder: MeldEncoder) {
  }

  async initialised() {
    // Create the Journal if not exists
    return (await this.kvps.get(JOURNAL_KEY)) != null;
  }

  decode(op: EncodedOperation) {
    return MeldOperation.fromEncoded(this.encoder, op);
  }

  toMeldOperation(op: CausalOperation<Triple, TreeClock>) {
    return MeldOperation.fromOperation(this.encoder, op);
  }

  reset(localTime: TreeClock, gwc: GlobalClock): Kvps {
    return new SuSetJournal(this, localTime.ticks, localTime, gwc).commit;
  }

  async journal() {
    if (this._journal == null) {
      const value = await this.kvps.get(JOURNAL_KEY);
      if (value == null)
        throw new Error('Missing journal');
      this._journal = SuSetJournal.fromJson(this, MsgPack.decode(value));
    }
    return this._journal;
  }

  async entryPrev(tid: string) {
    const value = await this.kvps.get(tidKey(tid));
    if (value == null)
      throw missingOperationError(tid);
    const [prev] = MsgPack.decode(value) as JournalEntryJson;
    return prev;
  }

  async entryAfter(key: number | EntryKey) {
    if (typeof key == 'number')
      key = tickKey(key);
    const kvp = await this.kvps.read({ gt: key, lt: TICK_KEY_MAX, limit: 1 }).toPromise();
    if (kvp != null) {
      const [key, value] = kvp;
      return SuSetJournalEntry.fromJson(this, key, MsgPack.decode(value));
    }
  }

  async operation(tid: string): Promise<SuSetJournalOperation | undefined> {
    const value = await this.kvps.get(tidOpKey(tid));
    if (value != null)
      return SuSetJournalOperation.fromJson(this, MsgPack.decode(value), tid);
  }

  async meldOperation(tid: string) {
    const first = await this.operation(tid);
    if (first == null)
      throw missingOperationError(tid);
    return this.decode(first.json);
  }

  insertOperation(operation: EncodedOperation): Kvps {
    return SuSetJournalOperation.fromJson(this, operation).commit;
  }
}

function missingOperationError(tid: string) {
  return new Error(`Journal corrupted: operation ${tid} is missing`);
}

