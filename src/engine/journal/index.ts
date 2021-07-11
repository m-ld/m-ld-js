import { EncodedOperation } from '../index';
import { GlobalClock, TreeClock } from '../clocks';
import { MsgPack } from '../util';
import { KvpResult, Kvps, KvpStore, TxnContext } from '../dataset';
import { MeldEncoder, MeldOperation } from '../MeldEncoding';
import { CausalOperation, CausalOperator } from '../ops';
import { Triple } from '../quads';
import { JournalOperation, TickTid } from './JournalOperation';
import { JournalEntry } from './JournalEntry';
import { JournalState } from './JournalState';
import { defaultIfEmpty, firstValueFrom, Observable, Subject as Source } from 'rxjs';

export { JournalState, JournalEntry };

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
export const tickKey = (tick: number): TickKey =>
  `_qs:tick:${TICK_KEY_PAD.concat(tick.toString(TICK_KEY_RADIX)).slice(-TICK_KEY_LEN)}`;

/** Entries are also indexed by time hash (TID) */
function tidEntryKey(tid: string) {
  return `_qs:tid:${tid}`;
}

/** Operations indexed by time hash (TID) */
function tidOpKey(tid: string) {
  return `_qs:op:${tid}`;
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
    private readonly encoder: MeldEncoder) {
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

  toMeldOperation(op: CausalOperation<Triple, TreeClock>) {
    return MeldOperation.fromOperation(this.encoder, op);
  }

  reset(localTime: TreeClock, gwc: GlobalClock): Kvps {
    return new JournalState(this, localTime.ticks, localTime, gwc).commit;
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

  async entryPrev(tid: string): Promise<TickTid | undefined> {
    const value = await this.store.get(tidEntryKey(tid));
    if (value != null)
      return JournalEntry.prev(MsgPack.decode(value));
  }

  /**
   * @param key tick or tick-key of entry prior to the requested one. If `undefined`, the first
   *   entry in the journal is being requested.
   * @returns the entry after the entry or operation identified by `key`, if it exists
   */
  async entryAfter(key: number | TickKey = TICK_KEY_MIN): Promise<JournalEntry | undefined> {
    return this.withLockedHistory(async () => {
      const [foundKey, value] = await firstValueFrom(this.store.read({
        gt: typeof key == 'number' ? tickKey(key) : key,
        lt: TICK_KEY_MAX,
        limit: 1
      }).pipe(defaultIfEmpty([]))) ?? [];
      return foundKey != null && value != null ? {
        return: JournalEntry.fromJson(this, foundKey, MsgPack.decode(value))
      } : {};
    });
  }

  commitEntry(entry: JournalEntry, isTail = true): Kvps {
    return batch => {
      const encoded = MsgPack.encode(entry.json);
      batch.put(entry.key, encoded);
      batch.put(tidEntryKey(entry.operation.tid), encoded);
      entry.operation.commit(batch);
      if (isTail)
        this._tail.next(entry);
    };
  }

  async spliceEntries(remove: EntryIndex[], insert?: JournalEntry): Promise<unknown> {
    return this.withLockedHistory(() => ({
      kvps: batch => {
        for (let { key, tid } of remove) {
          batch.del(key);
          batch.del(tidEntryKey(tid));
          batch.del(tidOpKey(tid));
        }
        if (insert != null)
          this.commitEntry(insert, false)(batch);
      }
    }));
  }

  async operation(tid: string): Promise<JournalOperation | undefined> {
    const value = await this.store.get(tidOpKey(tid));
    if (value != null)
      return JournalOperation.fromJson(this, MsgPack.decode(value), tid);
  }

  commitOperation(op: JournalOperation): Kvps {
    return batch => batch.put(tidOpKey(op.tid), MsgPack.encode(op.operation));
  }

  async meldOperation(tid: string): Promise<MeldOperation> {
    const first = await this.operation(tid);
    if (first == null)
      throw notFound('operation', tid);
    return this.decode(first.json);
  }

  insertPastOperation(operation: EncodedOperation): Kvps {
    return this.commitOperation(JournalOperation.fromJson(this, operation));
  }

  disposeOperationIfUnreferenced(tid: string) {
    return this.withLockedHistory(async () => {
      // If the operation does not have a corresponding journal entry, it is safe to dispose.
      const referenced = await this.store.has(tidEntryKey(tid));
      return {
        kvps: !referenced ? batch => batch.del(tidOpKey(tid)) : undefined,
        return: !referenced
      };
    });
  }

  /**
   * MUST be called `withLockedHistory`.
   * @param op the operation whose causal history to operate on
   * @param createOperator creates a causal reduction operator from the given first operation
   * @param minFrom the least required value of the range of the operation with
   * the returned identity
   * @returns found operations, up to and including this one
   */
  async causalReduce(op: JournalOperation,
    createOperator: (first: MeldOperation) => CausalOperator<Triple, TreeClock>,
    minFrom = 1): Promise<MeldOperation> {
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
      tid = op.time.ticked(++tick).hash();
      operator.next(tid === op.tid ?
        op.asMeldOperation() : await this.meldOperation(tid));
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

export function notFound(type: 'entry' | 'operation', tid: string) {
  return new Error(`Journal corrupted: ${type} ${tid} is missing`);
}