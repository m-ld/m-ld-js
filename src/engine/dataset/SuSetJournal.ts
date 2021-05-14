import { toPrefixedId } from './SuSetGraph';
import { EncodedOperation } from '..';
import { TreeClock, TreeClockJson } from '../clocks';
import { MsgPack } from '../util';
import { Dataset, Kvps } from '.';
import { MeldOperation } from '../MeldEncoding';

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
  number,
  /** Raw operation - may contain Buffers */
  EncodedOperation,
  /**
   * JSON-encoded public clock time ('global wall clock' or 'Great Westminster
   * Clock'). This has latest public ticks seen for all processes (not internal
   * ticks), unlike the operation time, which may be causally related to older
   * messages from third parties, and the journal time, which has internal ticks
   * for the local clone identity. This clock has no identity.
   */
  TreeClockJson,
  /** Next entry key */
  EntryKey | undefined
];

/** Immutable expansion of JournalEntryJson */
export class SuSetJournalEntry {
  constructor(
    private readonly dataset: SuSetJournalDataset,
    private readonly json: JournalEntryJson,
    readonly key: EntryKey,
    // Decompose some content for convenience, unless provided
    readonly start = json[0],
    readonly operation = json[1],
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
      tick, [2, tick, (localTime ?? TreeClock.GENESIS).toJson(), '{}', '{}'], gwc?.toJson()
    ]];
  }

  builder(journal: SuSetJournal, op: MeldOperation, localTime: TreeClock): {
    next: (op: MeldOperation, localTime: TreeClock) => void, commit: Kvps
  } {
    const dataset = this.dataset;
    class EntryBuild {
      start: number;
      key: EntryKey;
      gwc: TreeClock;
      next?: EntryBuild;

      constructor(prevGwc: TreeClock, readonly op: MeldOperation, readonly localTime: TreeClock) {
        this.start = localTime.ticks;
        this.key = entryKey(localTime.ticks);
        this.gwc = prevGwc.update(op.time);
      }

      *build(): Iterable<SuSetJournalEntry> {
        const nextKey = this.next != null ? this.next.key : undefined;
        const json: JournalEntryJson = [
          this.start,
          this.op.encoded,
          this.gwc.toJson(),
          nextKey
        ];
        yield new SuSetJournalEntry(dataset, json, this.key,
          this.start, this.op.encoded, this.op.time, this.gwc, nextKey);
        if (this.next)
          yield* this.next.build();
      }
    }
    let head = new EntryBuild(this.gwc, op, localTime), tail = head;
    return {
      /** Adds another journal entry to this builder */
      next: (op, localTime) => {
        tail = tail.next = new EntryBuild(tail.gwc, op, localTime);
      },
      /** Commits the built journal entries to the journal */
      commit: batch => {
        const entries = [...head.build()];
        const newJson: JournalEntryJson = [...this.json];
        newJson[3] = entries[0].key; // Next key
        batch.put(this.key, MsgPack.encode(newJson));
        entries.forEach(entry => batch.put(entry.key, MsgPack.encode(entry.json)));
        journal.commit(entries.slice(-1)[0], tail.localTime)(batch);
      }
    };
  }
}

/** Immutable expansion of JournalJson */
export class SuSetJournal {
  /** Tail state cache */
  _tail: SuSetJournalEntry | null = null;

  constructor(
    private readonly dataset: SuSetJournalDataset,
    json: JournalJson,
    // Decompose some content for convenience, unless provided
    readonly tailKey = json.tail,
    readonly time = json.time != null ? TreeClock.fromJson(json.time) : null) {
  }

  get safeTime() {
    if (this.time == null)
      throw new Error('Journal time not available');
    return this.time;
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
    private ds: Dataset) {
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
      return new SuSetJournalEntry(this, MsgPack.decode(value), key);
  }

  async entryFor(tick: number) {
    const kvp = await this.ds.gte(entryKey(tick));
    if (kvp != null) {
      const [key, value] = kvp;
      if (key.startsWith(ENTRY_KEY_PRE)) {
        const firstAfter = new SuSetJournalEntry(this, MsgPack.decode(value), key);
        // Check that the entry's tick range covers the request
        if (firstAfter.start <= tick)
          return firstAfter;
      }
    }
  }
}
