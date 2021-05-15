import { toPrefixedId } from './SuSetGraph';
import { EncodedOperation } from '..';
import { TreeClock, TreeClockJson } from '../clocks';
import { MsgPack } from '../util';
import { Dataset, Kvps } from '.';
import { MeldEncoding, MeldOperation } from '../MeldEncoding';
import { CausalTimeRange } from '../ops';
import { MeldError } from '../MeldError';

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
  operation: EncodedOperation,
  /** Next entry key */
  next: EntryKey | undefined
];

/** Utility interface for entry details; de-structures causal time range */
interface JournalEntry extends CausalTimeRange<TreeClock> {
  key: string,
  prev: number,
  start: number,
  operation: EncodedOperation
}

/** Immutable expansion of JournalEntryJson */
export class SuSetJournalEntry implements JournalEntry {
  constructor(
    private readonly dataset: SuSetJournalDataset,
    readonly key: EntryKey,
    private readonly json: JournalEntryJson,
    // De-structure some content for convenience, unless provided
    readonly prev = json[0],
    readonly start = json[1],
    readonly operation = json[2],
    readonly from = operation[1],
    readonly time = TreeClock.fromJson(operation[2]) as TreeClock,
    readonly nextKey = json[3]) {
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    if (this.nextKey != null)
      return this.dataset.entry(this.nextKey);
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
    let head = new EntryBuilder(this.dataset, this, journal.time, journal.gwc), tail = head;
    const builder = {
      next: (operation: MeldOperation, localTime: TreeClock) => {
        tail = tail.next(operation, localTime);
        return builder;
      },
      /** Commits the built journal entries to the journal */
      commit: <Kvps>(batch => {
        let entry: SuSetJournalEntry | null = null;
        for (entry of head.build())
          batch.put(entry.key, MsgPack.encode(entry.json));
        if (entry == null)
          throw new TypeError; // Head always defined
        journal.commit(entry, tail.localTime, tail.gwc)(batch);
      })
    };
    return builder;
  }
}

class EntryBuilder {
  private nextBuilder?: EntryBuilder

  constructor(
    private readonly dataset: SuSetJournalDataset,
    private entry: JournalEntry,
    public localTime: TreeClock,
    public gwc: TreeClock) {
  }

  next(operation: MeldOperation, localTime: TreeClock) {
    // if (CausalTimeRange.contiguous(this.entry, operation))
    //   return this.fuseNext(operation, localTime);
    // else
    return this.makeNext(operation, localTime);
  }

  *build(): Iterable<SuSetJournalEntry> {
    yield new SuSetJournalEntry(this.dataset,
      this.entry.key,
      this.json,
      this.entry.prev,
      this.entry.start,
      this.entry.operation,
      this.entry.from,
      this.entry.time,
      this.nextBuilder?.entry.key);
    if (this.nextBuilder != null)
      yield* this.nextBuilder.build();
  }

  private makeNext(operation: MeldOperation, localTime: TreeClock) {
    return this.nextBuilder = new EntryBuilder(this.dataset, {
      key: entryKey(localTime.ticks),
      prev: this.gwc.getTicks(operation.time),
      start: localTime.ticks,
      operation: operation.encoded,
      from: operation.from,
      time: operation.time
    }, localTime, this.nextGwc(operation));
  }

  private fuseNext(operation: MeldOperation, localTime: TreeClock) {
    const thisOp = MeldOperation.fromEncoded(this.dataset.encoding, this.entry.operation);
    const fused = MeldOperation.fromOperation(this.dataset.encoding, thisOp.fuse(operation));
    this.entry = {
      key: entryKey(localTime.ticks),
      prev: this.entry.prev,
      start: this.entry.start,
      operation: fused.encoded,
      from: fused.from,
      time: fused.time
    };
    this.localTime = localTime;
    this.gwc = this.nextGwc(operation);
    return this;
  }

  private nextGwc(operation: MeldOperation): TreeClock {
    return this.gwc.update(operation.time);
  }

  private get json(): JournalEntryJson {
    return [
      this.entry.prev,
      this.entry.start,
      this.entry.operation,
      this.nextBuilder?.entry.key
    ];
  }
}

/** Immutable expansion of JournalJson */
export class SuSetJournal {
  /** Tail state cache */
  _tail: SuSetJournalEntry | null = null;

  constructor(
    private readonly dataset: SuSetJournalDataset,
    json: JournalJson,
    // De-structure some content for convenience, unless provided
    readonly tailKey = json.tail,
    readonly time = TreeClock.fromJson(json.time) as TreeClock,
    readonly gwc = TreeClock.fromJson(json.gwc) as TreeClock) {
  }

  async tail(): Promise<SuSetJournalEntry> {
    if (this._tail == null) {
      if (this.tailKey == null)
        throw new Error('Journal has no tail yet');
      this._tail = await this.dataset.entry(this.tailKey) ?? null;
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
      this.dataset._journal = null;
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
      this.dataset._journal = new SuSetJournal(this.dataset, json, tail.key, localTime, gwc);
      this.dataset._journal._tail = tail;
    }
  }
}

export class SuSetJournalDataset {
  /** Journal state cache */
  _journal: SuSetJournal | null = null;

  constructor(
    private readonly ds: Dataset,
    readonly encoding: MeldEncoding) {
  }

  async initialise(): Promise<Kvps | undefined> {
    // Create the Journal if not exists
    const journal = await this.ds.get(JOURNAL_KEY);
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
      const value = await this.ds.get(JOURNAL_KEY);
      if (value == null)
        throw new Error('Missing journal');
      this._journal = new SuSetJournal(this, MsgPack.decode(value));
    }
    return this._journal;
  }

  async entry(key: EntryKey) {
    if (this.ds.closed)
      throw new MeldError('Clone has closed');
    const value = await this.ds.get(key);
    if (value != null)
      return new SuSetJournalEntry(this, key, MsgPack.decode(value));
  }

  async entryFor(tick: number) {
    const kvp = await this.ds.gte(entryKey(tick));
    if (kvp != null) {
      const [key, value] = kvp;
      if (key.startsWith(ENTRY_KEY_PRE)) {
        const firstAfter = new SuSetJournalEntry(this, key, MsgPack.decode(value));
        // Check that the entry's tick range covers the request
        if (firstAfter.start <= tick)
          return firstAfter;
      }
    }
  }
}
