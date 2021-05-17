import { toPrefixedId } from './SuSetGraph';
import { EncodedOperation } from '..';
import { TreeClock, TreeClockJson } from '../clocks';
import { MsgPack } from '../util';
import { Kvps, KvpStore } from '.';
import { MeldEncoder, MeldOperation } from '../MeldEncoding';
import { CausalTimeRange } from '../ops';

/** There is only one journal with a fixed key. */
const JOURNAL_KEY = '_qs:journal';

/**
 * Journal entries are indexed by end-tick as
 * `_qs:entry:${zeroPad(tick.toString(36), 8)}`. This gives a maximum tick of
 * 36^8, about 3 trillion, about 90 years in milliseconds.
 */
type EntryKey = ReturnType<typeof entryKey>;
const ENTRY_KEY_PRE = '_qs:entry';
const ENTRY_KEY_LEN = 8;
const ENTRY_KEY_PAD = new Array(ENTRY_KEY_LEN).fill('0').join('');
function entryKey(tick: number) {
  // Dummy head has tick -1
  if (tick === -1)
    return toPrefixedId(ENTRY_KEY_PRE, 'head');
  else if (tick >= 0)
    return toPrefixedId(ENTRY_KEY_PRE,
      `${ENTRY_KEY_PAD}${tick.toString(36)}`.slice(-ENTRY_KEY_LEN));
  else
    throw 'Invalid entry tick ' + tick;
}

/** Causally-fused operation from a clone, indexed by time hash (TID) */
const OPERATION_KEY_PRE = '_qs:op';
function operationKey(tid: string) {
  return toPrefixedId(OPERATION_KEY_PRE, tid);
}

interface JournalJson {
  tail: EntryKey;
  /** local clock time, including internal ticks */
  time: TreeClockJson;
  /**
   * JSON-encoded public clock time ('global wall clock' or 'Great Westminster
   * Clock'). This has latest public ticks seen for all processes (not internal
   * ticks), unlike an entry time, which may be causally related to older
   * messages from third parties, and the journal time, which has internal ticks
   * for the local clone identity. This clock has no identity.
   */
  gwc: TreeClockJson;
}

type JournalEntryJson = [
  /** Previous tick for this entry's clock (may be remote) */
  prev: number,
  /** Start of _local_ tick range, inclusive. The entry key is the encoded end. */
  start: number,
  /** Raw operation - may contain Buffers */
  encoded: EncodedOperation,
  /** Next entry key */
  next: EntryKey | undefined
];

/**
 * Utility class for key entry details
 */
interface JournalEntry {
  key: string,
  prev: number,
  start: number,
  operation: MeldOperation
}

/** Immutable expansion of JournalEntryJson */
export class SuSetJournalEntry implements JournalEntry {
  static fromJson(data: SuSetJournalData, key: EntryKey, json: JournalEntryJson) {
    // Destructuring fields for convenience
    const [prev, start, encoded, next] = json;
    const [, from, timeJson] = encoded;
    const time = TreeClock.fromJson(timeJson) as TreeClock;
    return new SuSetJournalEntry(data, key, prev, start, encoded, from, time, next);
  }

  static fromEntry(data: SuSetJournalData, entry: JournalEntry, next: EntryKey | undefined) {
    const { key, prev, start, operation } = entry;
    const { from, time } = operation;
    return new SuSetJournalEntry(data, key, prev, start, operation, from, time, next);
  }

  /** Cache of operation if available */
  private _operation?: MeldOperation;
  encoded: EncodedOperation;

  private constructor(
    private readonly data: SuSetJournalData,
    readonly key: EntryKey,
    readonly prev: number,
    readonly start: number,
    op: EncodedOperation | MeldOperation,
    readonly from: number,
    readonly time: TreeClock,
    readonly nextKey: EntryKey | undefined) {
    if (op instanceof MeldOperation) {
      this._operation = op;
      this.encoded = op.encoded;
    } else {
      this.encoded = op;
    }
  }

  get operation() {
    if (this._operation == null)
      this._operation = MeldOperation.fromEncoded(this.data.encoder, this.encoded);
    return this._operation;
  }

  private get json(): JournalEntryJson {
    return [this.prev, this.start, this.encoded, this.nextKey];
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    if (this.nextKey != null)
      return this.data.entry(this.nextKey);
  }

  static head(localTime?: TreeClock): [EntryKey, Partial<JournalEntryJson>] {
    const tick = localTime?.ticks ?? -1;
    return [entryKey(tick), [
      // Dummy operation for head
      -1, tick, [2, tick, (localTime ?? TreeClock.GENESIS).toJson(), '[]', '[]']
    ]];
  }

  builder(journal: SuSetJournal) {
    // The head represents this entry, made ready for appending new entries
    let head = new EntryBuilder(this.data, this, journal.time, journal.gwc), tail = head;
    const builder = {
      next: (operation: MeldOperation, localTime: TreeClock) => {
        tail = tail.next(operation, localTime);
        return builder;
      },
      /** Commits the built journal entries to the journal */
      commit: <Kvps>(async batch => {
        const entries = [...head.build()];
        if (entries[0].key !== this.key)
          batch.del(this.key);
        await Promise.all(entries.map(async entry => {
          batch.put(entry.key, MsgPack.encode(entry.json));
          await this.data.updateLatestOperation(entry)(batch);
        }));
        journal.commit(entries.slice(-1)[0], tail.localTime, tail.gwc)(batch);

      })
    };
    return builder;
  }
}

class EntryBuilder {
  private nextBuilder?: EntryBuilder

  constructor(
    private readonly data: SuSetJournalData,
    private entry: JournalEntry,
    public localTime: TreeClock,
    public gwc: TreeClock) {
  }

  next(operation: MeldOperation, localTime: TreeClock) {
    if (CausalTimeRange.contiguous(this.entry.operation, operation))
      return this.fuseNext(operation, localTime);
    else
      return this.makeNext(operation, localTime);
  }

  *build(): Iterable<SuSetJournalEntry> {
    yield SuSetJournalEntry.fromEntry(
      this.data, this.entry, this.nextBuilder?.entry.key);
    if (this.nextBuilder != null)
      yield* this.nextBuilder.build();
  }

  private makeNext(operation: MeldOperation, localTime: TreeClock) {
    return this.nextBuilder = new EntryBuilder(this.data, {
      key: entryKey(localTime.ticks),
      prev: this.gwc.getTicks(operation.time),
      start: localTime.ticks,
      operation
    }, localTime, this.nextGwc(operation));
  }

  private fuseNext(operation: MeldOperation, localTime: TreeClock) {
    const thisOp = this.entry.operation;
    const fused = MeldOperation.fromOperation(this.data.encoder, thisOp.fuse(operation));
    this.entry = {
      key: entryKey(localTime.ticks),
      prev: this.entry.prev,
      start: this.entry.start,
      operation: fused
    };
    this.localTime = localTime;
    this.gwc = this.nextGwc(operation);
    return this;
  }

  private nextGwc(operation: MeldOperation): TreeClock {
    return this.gwc.update(operation.time);
  }
}

/** Immutable expansion of JournalJson */
export class SuSetJournal {
  /** Tail state cache */
  _tail: SuSetJournalEntry | null = null;

  static fromJson(data: SuSetJournalData, json: JournalJson) {
    const time = TreeClock.fromJson(json.time) as TreeClock;
    const gwc = TreeClock.fromJson(json.gwc) as TreeClock;
    return new SuSetJournal(data, json.tail, time, gwc);
  }

  private constructor(
    private readonly data: SuSetJournalData,
    readonly tailKey: EntryKey,
    readonly time: TreeClock,
    readonly gwc: TreeClock) {
  }

  async tail(): Promise<SuSetJournalEntry> {
    if (this._tail == null) {
      if (this.tailKey == null)
        throw new Error('Journal has no tail yet');
      this._tail = await this.data.entry(this.tailKey) ?? null;
      if (this._tail == null)
        throw new Error('Journal tail is missing');
    }
    return this._tail;
  }

  setLocalTime(localTime: TreeClock, newClone?: boolean): Kvps {
    return async batch => {
      let tailKey = this.tailKey;
      if (newClone) {
        // For a new clone, the journal's temp tail has a bogus timestamp
        batch.del(this.tailKey);
        const [headKey, headJson] = SuSetJournalEntry.head(localTime);
        batch.put(headKey, MsgPack.encode(headJson));
        tailKey = headKey;
      }
      // A genesis clone has an initial head without a GWC. Other clones will
      // have their journal reset with a snapshot. So, it's safe to use the
      // local time as the gwc, which is needed for subsequent entries.
      const gwc = this.gwc ?? localTime.scrubId();
      const json: JournalJson = { tail: tailKey, time: localTime.toJson(), gwc: gwc.toJson() };
      batch.put(JOURNAL_KEY, MsgPack.encode(json));
      // Not updating caches for rare time update
      this.data._journal = null;
      this._tail = null;
    };
  }

  static initJson(headKey: EntryKey, localTime?: TreeClock, gwc?: TreeClock): Partial<JournalJson> {
    return { tail: headKey, time: localTime?.toJson(), gwc: gwc?.toJson() };
  }

  /**
   * Commits a new tail and time, with updates to the journal and tail cache
   */
  commit(tail: SuSetJournalEntry, localTime: TreeClock, gwc: TreeClock): Kvps {
    return batch => {
      const json: JournalJson = { tail: tail.key, time: localTime.toJson(), gwc: gwc.toJson() };
      batch.put(JOURNAL_KEY, MsgPack.encode(json));
      this.data._journal = new SuSetJournal(this.data, tail.key, localTime, gwc);
      this.data._journal._tail = tail;
    }
  }
}

export class SuSetJournalData {
  /** Journal state cache */
  _journal: SuSetJournal | null = null;

  constructor(
    private readonly kvps: KvpStore,
    readonly encoder: MeldEncoder) {
  }

  async initialise(): Promise<Kvps | undefined> {
    // Create the Journal if not exists
    const journal = await this.kvps.get(JOURNAL_KEY);
    if (journal == null)
      return this.reset();
  }

  reset(localTime?: TreeClock, gwc?: TreeClock): Kvps {
    const [headKey, headJson] = SuSetJournalEntry.head(localTime);
    const journalJson = SuSetJournal.initJson(headKey, localTime, gwc);
    return batch => {
      batch.put(JOURNAL_KEY, MsgPack.encode(journalJson));
      batch.put(headKey, MsgPack.encode(headJson));
      this._journal = null; // Not caching one-time change
    };
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

  async entry(key: EntryKey) {
    const value = await this.kvps.get(key);
    if (value != null)
      return SuSetJournalEntry.fromJson(this, key, MsgPack.decode(value));
  }

  async entryFor(tick: number) {
    const kvp = await this.kvps.gte(entryKey(tick));
    if (kvp != null) {
      const [key, value] = kvp;
      if (key.startsWith(ENTRY_KEY_PRE)) {
        const firstAfter = SuSetJournalEntry.fromJson(this, key, MsgPack.decode(value));
        // Check that the entry's tick range covers the request
        if (firstAfter.start <= tick)
          return firstAfter;
      }
    }
  }

  async operation(tid: string): Promise<EncodedOperation | undefined> {
    const value = await this.kvps.get(operationKey(tid));
    if (value != null)
      return MsgPack.decode(value);
  }

  updateLatestOperation(entry: SuSetJournalEntry): Kvps {
    return async batch => {
      // Do we have an operation for the entry's prev tick?
      if (entry.prev >= 0) {
        const prevTime = entry.time.ticked(entry.prev);
        const prevTid = prevTime.hash();
        const prevOp = await this.operation(prevTid);
        let newLatest = entry.encoded;
        if (prevOp != null) {
          const [, from] = prevOp;
          if (CausalTimeRange.contiguous({ from, time: prevTime }, entry)) {
            const fused = MeldOperation.fromEncoded(this.encoder, prevOp).fuse(entry.operation);
            newLatest = MeldOperation.fromOperation(this.encoder, fused).encoded;
          }
        }
        // Always delete the old latest
        // TODO: This is not perfect garbage collection, see fused-updates spec
        batch.del(operationKey(prevTid));
        batch.put(operationKey(entry.time.hash()), MsgPack.encode(newLatest));
      }
    }
  }
}
