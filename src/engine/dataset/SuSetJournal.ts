import { toPrefixedId } from './SuSetGraph';
import { MeldDelta, EncodedDelta } from '..';
import { TreeClock } from '../clocks';
import { MsgPack } from '../util';
import { Dataset, Kvps } from '.';

/** There is only one journal with a fixed key */
const JOURNAL_KEY = '_qs:journal';

/** Journal entries are keyed as `_qs:entry:#tick`. */
type EntryKey = ReturnType<typeof entryKey>;
function entryKey(tick: number) {
  return toPrefixedId('_qs:entry', tick.toString());
}

interface JournalJson {
  tail: EntryKey;
  time: any; // JSON-encoded TreeClock (the local clock)
}

type JournalEntryJson = [
  /** Raw delta - may contain Buffers */
  EncodedDelta,
  /** JSON-encoded transaction message time */
  any, // TODO: Push into delta as TID
  /**
   * JSON-encoded public clock time ('global wall clock' or 'Great Westminster
   * Clock'). This has latest public ticks seen for all processes (not internal
   * ticks), unlike the message time, which may be causally related to older
   * messages from third parties, and the journal time, which has internal ticks
   * for the local clone identity. This clock has no identity.
   */
  any,
  /** Next entry key */
  EntryKey | undefined
];

export interface EntryCreateDetails {
  delta: MeldDelta;
  localTime: TreeClock;
  remoteTime?: TreeClock;
}

/** Immutable */
export class SuSetJournalEntry {
  constructor(
    private readonly dataset: SuSetJournalDataset,
    private readonly json: JournalEntryJson,
    readonly key: EntryKey,
    readonly delta = json[0],  // undefined iff head
    readonly time = TreeClock.fromJson(json[1]) as TreeClock, // null iff temporary head
    readonly gwc = TreeClock.fromJson(json[2]) as TreeClock,
    readonly nextKey = json[3]) { // null iff temporary head
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    if (this.nextKey != null)
      return this.dataset.entry(this.nextKey);
  }

  static head(localTime?: TreeClock, gwc?: TreeClock): [EntryKey, Partial<JournalEntryJson>] {
    return [entryKey(localTime?.ticks ?? -1), [
      undefined, // No delta for head
      localTime?.toJson(),
      gwc?.toJson()
    ]];
  }

  builder(journal: SuSetJournal, entry: EntryCreateDetails): {
    next: (entry: EntryCreateDetails) => void, commit: Kvps
  } {
    const dataset = this.dataset;
    class EntryBuild {
      key: EntryKey;
      tick: number;
      gwc: TreeClock;
      time: TreeClock;
      next?: EntryBuild;
      constructor(prevGwc: TreeClock, readonly details: EntryCreateDetails) {
        this.tick = details.localTime.ticks;
        this.key = entryKey(this.tick);
        this.time = details.remoteTime ?? details.localTime;
        this.gwc = prevGwc.update(this.time);
      }
      *build(): Iterable<SuSetJournalEntry> {
        const nextKey = this.next != null ? this.next.key : undefined;
        const json: JournalEntryJson = [
          this.details.delta.encoded,
          this.time.toJson(),
          this.gwc.toJson(),
          nextKey
        ];
        yield new SuSetJournalEntry(dataset, json, this.key,
          this.details.delta.encoded, this.time, this.gwc, nextKey);
        if (this.next)
          yield* this.next.build();
      }
    }
    let head = new EntryBuild(this.gwc, entry), tail = head;
    return {
      /**
       * Adds another journal entry to this builder
       */
      next: entry => {
        tail = tail.next = new EntryBuild(tail.gwc, entry);
      },
      /**
       * Commits the built journal entries to the journal
       */
      commit: batch => {
        const entries = Array.from(head.build());
        const newJson: JournalEntryJson = [...this.json];
        newJson[3] = entries[0].key; // Next key
        batch.put(this.key, MsgPack.encode(newJson));
        entries.forEach(entry => batch.put(entry.key, MsgPack.encode(entry.json)));
        journal.commit(entries.slice(-1)[0], tail.details.localTime)(batch);
      }
    };
  }
}

export class SuSetJournal {
  /** Tail state cache */
  _tail: SuSetJournalEntry | null = null;

  constructor(
    private readonly dataset: SuSetJournalDataset,
    json: JournalJson,
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

  entry(tick: number) {
    return this.dataset.entry(entryKey(tick));
  }

  setLocalTime(localTime: TreeClock, newClone?: boolean): Kvps {
    return async batch => {
      let tailKey = this.tailKey;
      if (newClone) {
        // For a new clone, the journal's temp tail does not have a timestamp
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
}
