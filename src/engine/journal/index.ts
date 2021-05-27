import { toPrefixedId } from '../dataset/SuSetGraph';
import { EncodedOperation } from '../index';
import { GlobalClock, TreeClock } from '../clocks';
import { MsgPack } from '../util';
import { Kvps, KvpStore } from '../dataset';
import { MeldEncoder, MeldOperation } from '../MeldEncoding';
import { CausalOperation } from '../ops';
import { Triple } from '../quads';
import { JournalOperation } from './JournalOperation';
import { JournalEntry } from './JournalEntry';
import { JournalState } from './JournalState';

export { JournalState, JournalEntry };

/** There is only one journal with a fixed key. */
const JOURNAL_KEY = '_qs:journal';

/**
 * Journal entries are indexed by end-tick as
 * `_qs:tick:${zeroPad(tick.toString(36), 8)}`. This gives a maximum tick of
 * 36^8, about 3 trillion, about 90 years in milliseconds.
 */
export type TickKey = ReturnType<typeof tickKey>;
const TICK_KEY_PRE = '_qs:tick';
const TICK_KEY_LEN = 8;
const TICK_KEY_RADIX = 36;
const TICK_KEY_PAD = new Array(TICK_KEY_LEN).fill('0').join('');
const TICK_KEY_MAX = toPrefixedId(TICK_KEY_PRE, '~'); // > 'z'
export function tickKey(tick: number) {
  return toPrefixedId(TICK_KEY_PRE,
    `${TICK_KEY_PAD}${tick.toString(TICK_KEY_RADIX)}`.slice(-TICK_KEY_LEN));
}

/** Entries are also indexed by time hash (TID) */
function tidEntryKey(tid: string) {
  return toPrefixedId('_qs:tid', tid);
}

/** Operations indexed by time hash (TID) */
function tidOpKey(tid: string) {
  return toPrefixedId('_qs:op', tid);
}

export class Journal {
  /** Journal state cache */
  private _state: JournalState | null = null;

  constructor(
    private readonly store: KvpStore,
    private readonly encoder: MeldEncoder) {
  }

  async initialised() {
    // Create the Journal if not exists
    return (await this.store.get(JOURNAL_KEY)) != null;
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

  async entryPrev(tid: string): Promise<number> {
    const value = await this.store.get(tidEntryKey(tid));
    if (value == null)
      throw missingOperationError(tid);
    return JournalEntry.prev(MsgPack.decode(value));
  }

  async entryAfter(key: number | TickKey): Promise<JournalEntry | undefined> {
    if (typeof key == 'number')
      key = tickKey(key);
    const kvp = await this.store.read({ gt: key, lt: TICK_KEY_MAX, limit: 1 }).toPromise();
    if (kvp != null) {
      const [key, value] = kvp;
      return JournalEntry.fromJson(this, key, MsgPack.decode(value));
    }
  }

  saveEntry(entry: JournalEntry): Kvps {
    return batch => {
      const encoded = MsgPack.encode(entry.json);
      batch.put(entry.key, encoded);
      batch.put(tidEntryKey(entry.operation.tid), encoded);
      entry.operation.commit(batch);
      // If the previous operation for this time is not causally contiguous with this one, AND does
      // not have a corresponding journal entry, it is safe to garbage collect. This operation does
      // not have to hold up the transaction.
    };
  }

  async operation(tid: string): Promise<JournalOperation | undefined> {
    const value = await this.store.get(tidOpKey(tid));
    if (value != null)
      return JournalOperation.fromJson(this, MsgPack.decode(value), tid);
  }

  saveOperation(op: JournalOperation): Kvps {
    return batch => batch.put(tidOpKey(op.tid), MsgPack.encode(op.operation));
  }

  async meldOperation(tid: string): Promise<MeldOperation> {
    const first = await this.operation(tid);
    if (first == null)
      throw missingOperationError(tid);
    return this.decode(first.json);
  }

  insertOperation(operation: EncodedOperation): Kvps {
    return this.saveOperation(JournalOperation.fromJson(this, operation));
  }
}

export function missingOperationError(tid: string) {
  return new Error(`Journal corrupted: operation ${tid} is missing`);
}

