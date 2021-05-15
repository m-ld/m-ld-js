import { toPrefixedId } from './SuSetGraph';
import { EncodedOperation } from '..';
import { TreeClock, TreeClockJson } from '../clocks';
import { MsgPack } from '../util';
import { Dataset, Kvps } from '.';
import { MeldEncoding, MeldOperation } from '../MeldEncoding';
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

interface JournalJson {
  tail: EntryKey;
  time: TreeClockJson; // the local clock time
}

type JournalEntryJson = [
  /** Start of local tick range, inclusive. The entry key is the encoded end. */
  start: number,
  /** Raw operation - may contain Buffers */
  operation: EncodedOperation,
  /**
   * JSON-encoded public clock time ('global wall clock' or 'Great Westminster
   * Clock'). This has latest public ticks seen for all processes (not internal
   * ticks), unlike the operation time, which may be causally related to older
   * messages from third parties, and the journal time, which has internal ticks
   * for the local clone identity. This clock has no identity.
   */
  gwc: TreeClockJson,
  /** Next entry key */
  next: EntryKey | undefined
];

/** Utility interface for entry details; de-structures causal time range */
interface JournalEntry extends CausalTimeRange<TreeClock> {
  key: string,
  start: number,
  operation: EncodedOperation,
  gwc: TreeClock
}

/** Immutable expansion of JournalEntryJson */
export class SuSetJournalEntry implements JournalEntry {
  constructor(
    private readonly dataset: SuSetJournalDataset,
    readonly key: EntryKey,
    private readonly json: JournalEntryJson,
    // De-structure some content for convenience, unless provided
    readonly start = json[0],
    readonly operation = json[1],
    readonly from = operation[1],
    readonly time = TreeClock.fromJson(operation[2]) as TreeClock,
    readonly gwc = TreeClock.fromJson(json[2]) as TreeClock,
    readonly nextKey = json[3]) {
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    if (this.nextKey != null)
      return this.dataset.entry(this.nextKey);
  }

  static head(localTime?: TreeClock, gwc?: TreeClock): [EntryKey, Partial<JournalEntryJson>] {
    const tick = localTime?.ticks ?? -1;
    return [entryKey(tick), [
      // Dummy operation for head
      tick, [2, tick, (localTime ?? TreeClock.GENESIS).toJson(), '[]', '[]'], gwc?.toJson()
    ]];
  }

  builder(journal: SuSetJournal) {
    // The head represents this entry, made ready for appending new entries
    let head = new EntryBuilder(this.dataset, this, journal.time), tail = head;
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
        journal.commit(entry, tail.localTime)(batch);
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
    public localTime: TreeClock) {
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
      this.entry.start,
      this.entry.operation,
      this.entry.from,
      this.entry.time,
      this.entry.gwc,
      this.nextBuilder?.entry.key);
    if (this.nextBuilder != null)
      yield* this.nextBuilder.build();
  }

  private makeNext(operation: MeldOperation, localTime: TreeClock) {
    return this.nextBuilder = new EntryBuilder(this.dataset, {
      key: entryKey(localTime.ticks),
      start: localTime.ticks,
      operation: operation.encoded,
      from: operation.from,
      time: operation.time,
      gwc: this.nextGwc(operation)
    }, localTime);
  }

  private fuseNext(operation: MeldOperation, localTime: TreeClock) {
    const thisOp = MeldOperation.fromEncoded(this.dataset.encoding, this.entry.operation);
    const fused = MeldOperation.fromOperation(this.dataset.encoding, thisOp.fuse(operation));
    this.entry = {
      key: entryKey(localTime.ticks),
      start: this.entry.start,
      operation: fused.encoded,
      from: fused.from,
      time: fused.time,
      gwc: this.nextGwc(operation)
    };
    this.localTime = localTime;
    return this;
  }

  private nextGwc(operation: MeldOperation): TreeClock {
    return this.entry.gwc.update(operation.time);
  }

  private get json(): JournalEntryJson {
    return [
      this.entry.start,
      this.entry.operation,
      this.entry.gwc.toJson(),
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
    readonly time = TreeClock.fromJson(json.time) as TreeClock) {
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
        // The dummy head time will only ever be used for a genesis clone, as all
        // other clones will have their journal reset with a snapshot. So, it's safe
        // to use the local time as the gwc, which is needed for subsequent entries.
        const [headKey, headJson] = SuSetJournalEntry.head(localTime, localTime.scrubId());
        batch.put(headKey, MsgPack.encode(headJson));
        tailKey = headKey;
      }
      const json: JournalJson = { tail: tailKey, time: localTime.toJson() };
      batch.put(JOURNAL_KEY, MsgPack.encode(json));
      // Not updating caches for rare time update
      this.dataset._journal = null;
      this._tail = null;
    };
  }

  static initJson(headKey: EntryKey, localTime?: TreeClock): Partial<JournalJson> {
    return { tail: headKey, time: localTime?.toJson() };
  }

  /**
   * Commits a new tail and time, with updates to the journal and tail cache
   */
  commit(tail: SuSetJournalEntry, localTime: TreeClock): Kvps {
    return batch => {
      const json: JournalJson = { tail: tail.key, time: localTime.toJson() };
      batch.put(JOURNAL_KEY, MsgPack.encode(json));
      this.dataset._journal = new SuSetJournal(this.dataset, json, tail.key, localTime);
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

  reset(gwc?: TreeClock, localTime?: TreeClock): Kvps {
    const [headKey, headJson] = SuSetJournalEntry.head(localTime, gwc);
    const journalJson = SuSetJournal.initJson(headKey, localTime);
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
