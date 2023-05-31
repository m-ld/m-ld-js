import { JournalOperation } from './JournalOperation';
import type { Journal, TickKey } from '.';
import { EntryIndex } from '.';
import { EntryReversion, MeldOperation } from '../MeldOperation';
import { Attribution } from '../../api';
import { MeldOperationMessage } from '../MeldOperationMessage';

/**
 * Identifies an entry or operation by both tick and TID.
 *
 * CAUTION: the tick is that of the operation process clock, not necessarily the
 * local clock.
 */
export type TickTid = [
  tick: number,
  tid: string
];

/**
 * Lightweight encoding of a transaction operation reference (TID) and the
 * previous public tick from the entry's process clock.
 */
type JournalEntryJson = [
  /** Previous public tick and TID for this entry's clock (may be remote) */
  prev: TickTid,
  /** Operation transaction ID */
  tid: string,
  /** Triple TIDs that were actually removed when this entry was applied */
  reversion: EntryReversion,
  /** Original bound attribution of this entry */
  attribution: Attribution | null
];

/**
 * Immutable expansion of JournalEntryJson, with the referenced operation and
 * attribution.
 */
export class JournalEntry {
  static async fromJson(journal: Journal, key: TickKey, json: JournalEntryJson) {
    // Destructuring fields for convenience
    const [prev, tid, reversion, attribution] = json;
    const operation = await journal.operation(tid, 'require');
    return new JournalEntry(journal, key, prev, operation, reversion, attribution);
  }

  static fromOperation(
    journal: Journal,
    key: TickKey,
    prev: TickTid,
    operation: MeldOperation,
    reversion: EntryReversion,
    attribution: Attribution | null
  ) {
    return new JournalEntry(
      journal,
      key,
      prev,
      JournalOperation.fromOperation(journal, operation),
      reversion,
      attribution
    );
  }

  private constructor(
    private readonly journal: Journal,
    readonly key: TickKey,
    readonly prev: TickTid,
    readonly operation: JournalOperation,
    readonly reversion: EntryReversion,
    readonly attribution: Attribution | null
  ) {}

  get index(): EntryIndex {
    return { key: this.key, tid: this.operation.tid };
  }

  get json(): JournalEntryJson {
    return [this.prev, this.operation.tid, this.reversion, this.attribution];
  }

  next() {
    return this.journal.entryAfter(this.key);
  }

  previous() {
    return this.journal.entryBefore(this.key);
  }

  asMessage() {
    const [prevTick] = this.prev;
    return MeldOperationMessage.fromOperation(
      prevTick, this.operation.encoded, this.attribution, this.operation.time);
  }

  /**
   * Reverting an entry creates an operation that removes the entry's effect
   * from the SU-Set.
   *
   * Applying the returned patch is only coherent if nothing in the SU-Set is
   * caused-by this entry. So, entries must be undone from the tail of the
   * journal.
   */
  revert() {
    return this.operation.asMeldOperation().revert(this.reversion);
  }
}