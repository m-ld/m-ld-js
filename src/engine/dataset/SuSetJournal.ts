import { toPrefixedId } from './SuSetGraph';
import { EncodedOperation } from '..';
import { GlobalClock, GlobalClockJson, TreeClock, TreeClockJson } from '../clocks';
import { MsgPack } from '../util';
import { Kvps, KvpStore } from '.';
import { MeldEncoder, MeldOperation } from '../MeldEncoding';
import { CausalOperation } from '../ops';
import { EMPTY, from, Observable, of } from 'rxjs';
import { expand, mergeMap, takeWhile, toArray } from 'rxjs/operators';
import { Triple } from '../quads';

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
const ENTRY_KEY_RADIX = 36;
const ENTRY_KEY_PAD = new Array(ENTRY_KEY_LEN).fill('0').join('');
const ENTRY_KEY_MAX = toPrefixedId(ENTRY_KEY_PRE, '~'); // > 'z'
function entryKey(tick: number) {
  return toPrefixedId(ENTRY_KEY_PRE,
    `${ENTRY_KEY_PAD}${tick.toString(ENTRY_KEY_RADIX)}`.slice(-ENTRY_KEY_LEN));
}

/** Operations indexed by time hash (TID) */
const OPERATION_KEY_PRE = '_qs:op';
function operationKey(tid: string) {
  return toPrefixedId(OPERATION_KEY_PRE, tid);
}

interface JournalJson {
  /** First known tick of the local clock – for which an entry may not exist */
  start: number;
  /** Current local clock time, including internal ticks */
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

/** Immutable expansion of JournalEntryJson */
export class SuSetJournalEntry {
  static async fromJson(data: SuSetJournalData, key: EntryKey, json: JournalEntryJson) {
    // Destructuring fields for convenience
    const [prev, tid] = json;
    const operation = await data.operation(tid);
    if (operation != null) {
      const [, from, timeJson] = operation;
      const time = TreeClock.fromJson(timeJson);
      return new SuSetJournalEntry(data, key, prev, tid, operation, from, time);
    } else {
      throw new Error(`Journal corrupted: operation ${tid} is missing`);
    }
  }

  static fromOperation(data: SuSetJournalData,
    operation: MeldOperation, localTime: TreeClock, gwc: GlobalClock) {
    return new SuSetJournalEntry(data,
      entryKey(localTime.ticks),
      gwc.getTicks(operation.time),
      operation.time.hash(),
      operation.encoded,
      operation.from,
      operation.time)
  }

  private constructor(
    private readonly data: SuSetJournalData,
    readonly key: EntryKey,
    readonly prev: number,
    readonly tid: string,
    readonly operation: EncodedOperation,
    readonly from: number,
    readonly time: TreeClock) {
  }

  get json(): JournalEntryJson {
    return [this.prev, this.tid];
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    return this.data.entryAfter(this.key);
  }

  commit: Kvps = batch => {
    batch.put(this.key, MsgPack.encode(this.json));
    batch.put(operationKey(this.tid), MsgPack.encode(this.operation));
  };
}

/** Immutable expansion of JournalJson */
export class SuSetJournal {
  static fromJson(data: SuSetJournalData, json: JournalJson) {
    const time = TreeClock.fromJson(json.time);
    const gwc = GlobalClock.fromJson(json.gwc);
    return new SuSetJournal(data, json.start, time, gwc);
  }

  constructor(
    private readonly data: SuSetJournalData,
    readonly start: number,
    readonly time: TreeClock,
    readonly gwc: GlobalClock) {
  }

  withTime(localTime: TreeClock, gwc?: GlobalClock): SuSetJournal {
    return new SuSetJournal(this.data, this.start, localTime, gwc ?? this.gwc);
  }

  get json(): JournalJson {
    return { start: this.start, time: this.time.toJson(), gwc: this.gwc.toJson() };
  }

  commit: Kvps = batch => {
    batch.put(JOURNAL_KEY, MsgPack.encode(this.json));
    this.data._journal = this;
  };

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
      mergeMap(tid => {
        // From each op, expand backwards with causal ticks
        return from(this.data.operation(tid)).pipe(
          expand(op => {
            if (op == null)
              return EMPTY;
            const [, from, timeJson] = op, time = TreeClock.fromJson(timeJson);
            return this.data.operation(time.ticked(from - 1).hash());
          }),
          takeWhile<EncodedOperation>(op => op != null),
          // TODO: Make fusions work in reverse, so don't have to load all
          toArray(),
          mergeMap(ops => {
            if (ops.length <= 1) {
              return from(ops);
            } else {
              ops.reverse();
              const fused = ops.slice(1).reduce(
                (fusion, op) => fusion.next(this.data.decode(op)),
                this.data.decode(ops[0]).fusion()).commit();
              return of(this.data.encode(fused));
            }
          }));
      }));
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

  encode(op: CausalOperation<Triple, TreeClock>) {
    return MeldOperation.fromOperation(this.encoder, op).encoded;
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

  async entryAfter(key: number | EntryKey) {
    if (typeof key == 'number')
      key = entryKey(key);
    const kvp = await this.kvps.read({ gt: key, lt: ENTRY_KEY_MAX, limit: 1 }).toPromise();
    if (kvp != null) {
      const [key, value] = kvp;
      return SuSetJournalEntry.fromJson(this, key, MsgPack.decode(value));
    }
  }

  async operation(tid: string): Promise<EncodedOperation | undefined> {
    const key = operationKey(tid);
    const value = await this.kvps.get(key);
    if (value != null)
      return MsgPack.decode(value);
  }

  insertOperation(operation: EncodedOperation): Kvps {
    return async batch => {
      const [, , timeJson] = operation;
      const time = TreeClock.fromJson(timeJson);
      batch.put(operationKey(time.hash()), MsgPack.encode(operation));
    };
  }
}
