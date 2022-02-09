import { EncodedOperation } from '../index';
import { GlobalClock, TreeClock } from '../clocks';
import { MsgPack } from '../util';
import { KvpResult, Kvps, KvpStore, TxnContext } from '../dataset';
import { MeldEncoder } from '../MeldEncoding';
import { CausalOperator } from '../ops';
import { JournalOperation } from './JournalOperation';
import { JournalEntry, TickTid } from './JournalEntry';
import { EntryBuilder, JournalState } from './JournalState';
import { defaultIfEmpty, firstValueFrom, Observable, Subject as Source } from 'rxjs';
import { AgreeableOperationSpec, MeldOperation } from '../MeldOperation';

export { JournalState, JournalEntry, EntryBuilder };

/** There is only one journal with a fixed key. */
const JOURNAL_KEY = '_qs:journal';

/**
 * Journal entries are indexed by end-tick as
 * `_qs:tick:${zeroPad(tick.toString(36), 8)}`. This gives a maximum tick of
 * 36^8, about 3 trillion, about 90 years in milliseconds.
 */
export type TickKey = string;
const TICK_KEY_LEN = 8;
const TICK_KEY_RADIX = 36;
const TICK_KEY_PAD = '0'.repeat(TICK_KEY_LEN);
const TICK_KEY_MIN = '_qs:tick:!'; // < '0'
const TICK_KEY_MAX = '_qs:tick:~'; // > 'z'
export function tickKey(tick: number): TickKey {
  return `_qs:tick:${TICK_KEY_PAD.concat(tick.toString(TICK_KEY_RADIX)).slice(-TICK_KEY_LEN)}`;
}

/** Operations indexed by time hash (TID) */
function tidOpKey(tid: string) {
  return `_qs:op:${tid}`;
}

/** The previous tick & TID for each entry, indexed by time hash (TID) */
function tidPrevKey(tid: string) {
  return `_qs:tid:${tid}`;
}

/** Utility type to identify a journal entry in the indexes */
export interface EntryIndex {
  key: TickKey,
  tid: string
}

export class Journal {
  /** Journal state cache */
  private _state: JournalState | null = null;
  /** Entries being created */
  private _tail = new Source<JournalEntry>();

  constructor(
    private readonly store: KvpStore,
    private readonly encoder: MeldEncoder
  ) {
  }

  async initialised() {
    // Create the Journal if not exists
    return (await this.store.get(JOURNAL_KEY)) != null;
  }

  close(err?: any) {
    if (err)
      this._tail.error(err);
    else
      this._tail.complete();
  }

  get tail(): Observable<JournalEntry> {
    return this._tail;
  }

  decode(op: EncodedOperation) {
    return MeldOperation.fromEncoded(this.encoder, op);
  }

  toMeldOperation(op: AgreeableOperationSpec) {
    return MeldOperation.fromOperation(this.encoder, op);
  }

  reset(localTime: TreeClock, gwc: GlobalClock, agreed: TreeClock): Kvps {
    return new JournalState(this, localTime.ticks, localTime, gwc, agreed).commit;
  }

  async state() {
    if (this._state == null) {
      const value = await this.store.get(JOURNAL_KEY);
      if (value == null)
        throw new Error('Missing journal');
      this._state = JournalState.fromJson(this, MsgPack.decode(value));
    }
    return this._state;
  }

  saveState(journal: JournalState): Kvps {
    return batch => {
      batch.put(JOURNAL_KEY, MsgPack.encode(journal.json));
      this._state = journal;
    };
  }

  /**
   * Gets the identity of the previous operation seen from the process ID of the
   * given operation identity. This is not necessarily the strictly previous
   * journal entry by local tick.
   */
  async entryPrev(tid: string): Promise<TickTid | undefined> {
    const value = await this.store.get(tidPrevKey(tid));
    if (value != null)
      return MsgPack.decode(value);
  }

  /**
   * @param key tick or tick-key of entry prior to the requested one. If
   * `undefined`, the first entry in the journal is being requested.
   * @returns the entry after the entry or operation identified by `key`, if it
   * exists
   */
  async entryAfter(key: number | TickKey = TICK_KEY_MIN): Promise<JournalEntry | undefined> {
    return this.entryInTickRange({
      gt: typeof key == 'number' ? tickKey(key) : key,
      lt: TICK_KEY_MAX
    });
  }

  /**
   * CAUTION: A reverse seek is slower than a forward seek. Avoid this method if
   * possible.
   *
   * @param key tick or tick-key of entry after the requested one. If
   * `undefined`, the last entry in the journal is being requested.
   * @returns the entry before the entry or operation identified by `key`, if it
   * exists
   */
  async entryBefore(key: number | TickKey = TICK_KEY_MAX): Promise<JournalEntry | undefined> {
    return this.entryInTickRange({
      gt: TICK_KEY_MIN,
      lt: typeof key == 'number' ? tickKey(key) : key,
      reverse: true
    });
  }

  private entryInTickRange(range: { lt: string; gt: string, reverse?: boolean }) {
    return this.withLockedHistory(async () => {
      const [foundKey, value] = await firstValueFrom(
        this.store.read({ ...range, limit: 1 }).pipe(defaultIfEmpty([])));
      return foundKey != null && value != null ? {
        return: JournalEntry.fromJson(this, foundKey, MsgPack.decode(value))
      } : {};
    });
  }

  spliceEntries(
    remove: EntryIndex[],
    insert: JournalEntry[],
    { appending }: { appending: boolean }
  ): Kvps {
    return batch => {
      for (let { key, tid } of remove) {
        batch.del(key);
        batch.del(tidPrevKey(tid));
        batch.del(tidOpKey(tid));
      }
      for (let entry of insert) {
        batch.put(entry.key, MsgPack.encode(entry.json));
        // TID -> prev mapping
        batch.put(tidPrevKey(entry.operation.tid), MsgPack.encode(entry.prev));
        entry.operation.commit(batch);
        if (appending)
          this._tail.next(entry);
      }
    };
  }

  async operation(tid: string): Promise<JournalOperation | undefined>;
  async operation(tid: string, require: 'require'): Promise<JournalOperation>;
  async operation(tid: string, require?: 'require'): Promise<JournalOperation | undefined> {
    const value = await this.store.get(tidOpKey(tid));
    if (value != null)
      return JournalOperation.fromJson(this, MsgPack.decode(value), tid);
    else if (require)
      throw new Error(`Journal corrupted: operation ${tid} is missing`);
  }

  commitOperation(op: JournalOperation): Kvps {
    return batch => batch.put(tidOpKey(op.tid), MsgPack.encode(op.encoded));
  }

  async meldOperation(tid: string): Promise<MeldOperation> {
    return this.decode((await this.operation(tid, 'require')).encoded);
  }

  insertPastOperation(operation: EncodedOperation): Kvps {
    return this.commitOperation(JournalOperation.fromJson(this, operation));
  }

  /**
   * An operation is unreferenced if:
   * - it has no journal entry
   * - it is not in the GWC
   */
  disposeOperationIfUnreferenced(tid: string) {
    return this.withLockedHistory(async () => {
      // If the operation has a corresponding journal entry, it is not safe to dispose.
      if (await this.store.has(tidPrevKey(tid)))
        return { return: false };
      // If the operation is in the GWC, it is not safe to dispose.
      for (let gwcTid of (await this.state()).gwc.tids()) {
        if (tid === gwcTid)
          return { return: false };
      }
      // Otherwise unreferenced, so dispose
      return { kvps: batch => batch.del(tidOpKey(tid)), return: true };
    });
  }

  /**
   * Find the first journal entry with an operation that is causally contiguous
   * with the given operation, stopping if it reaches the `minFrom` tick for the
   * operation's clock. If such an entry is found, creates the reduction
   * operator from that entry and applies the reduction forwards.
   *
   * MUST be called `withLockedHistory`.
   *
   * @param op the operation whose causal history to operate on
   * @param createOperator creates a causal reduction operator from the given first operation
   * @param minFrom the least required value of the range of the operation with
   * the returned identity. Must not be <1 (genesis is never represented in the journal).
   * @returns found operations, up to and including this one
   */
  async causalReduce(
    op: JournalOperation,
    createOperator: (first: MeldOperation) => CausalOperator<AgreeableOperationSpec>,
    minFrom = 1
  ): Promise<MeldOperation> {
    // Work backward through the journal to find the first transaction ID (and associated tick)
    // that is causally contiguous with this one.
    const seekToFrom = async (tick: number, tid: string): Promise<TickTid> => {
      const [prevTick, prevTid] = await this.entryPrev(tid) ?? [];
      if (prevTid == null || prevTick == null || prevTick < minFrom || prevTick < tick - 1)
        return [tick, tid]; // This is as far back as we have
      else
        // Get previous tick for given tick (or our tick)
        return seekToFrom(prevTick, prevTid);
    };
    let [tick, tid] = await seekToFrom(op.tick, op.tid);
    // Begin the operation and fast-forward
    const operator = createOperator(await this.meldOperation(tid));
    while (tid !== op.tid) {
      tid = op.time.ticked(++tick).hash;
      // Small optimisation - no need to reload the given op
      operator.next(tid === op.tid ? op.asMeldOperation() : await this.meldOperation(tid));
    }
    return this.toMeldOperation(operator.commit());
  }

  /**
   * Prevents concurrent modification of the journal history while the given transaction executes.
   * It's OK for history to be appended without this lock.
   * @param prepare the transaction procedure
   */
  withLockedHistory<T = unknown>(
    prepare: (txc: TxnContext) => KvpResult<T> | Promise<KvpResult<T>>) {
    return this.store.transact({ lock: 'journal-body', prepare });
  }
}