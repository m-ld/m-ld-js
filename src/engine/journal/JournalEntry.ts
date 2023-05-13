import { JournalOperation } from './JournalOperation';
import type { Journal, TickKey } from '.';
import { EntryIndex } from '.';
import { MeldOperation } from '../MeldOperation';
import { TripleMap } from '../quads';
import { Iri } from '@m-ld/jsonld';
import { Attribution, UUID } from '../../api';
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
 * TIDs keyed by reference triple identifier as recorded in an operation
 */
export type EntryDeleted = { [key: Iri]: UUID[] };

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
  deleted: EntryDeleted,
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
    const [prev, tid, deleted, attribution] = json;
    const operation = await journal.operation(tid, 'require');
    return new JournalEntry(journal, key, prev, operation, deleted, attribution);
  }

  static fromOperation(
    journal: Journal,
    key: TickKey,
    prev: TickTid,
    operation: MeldOperation,
    deleted: TripleMap<UUID[]>,
    attribution: Attribution | null
  ) {
    return new JournalEntry(
      journal, key, prev,
      JournalOperation.fromOperation(journal, operation),
      operation.byRef('deletes', deleted), attribution);
  }

  private constructor(
    private readonly journal: Journal,
    readonly key: TickKey,
    readonly prev: TickTid,
    readonly operation: JournalOperation,
    readonly deleted: EntryDeleted,
    readonly attribution: Attribution | null
  ) {
  }

  get index(): EntryIndex {
    return { key: this.key, tid: this.operation.tid };
  }

  get json(): JournalEntryJson {
    return [this.prev, this.operation.tid, this.deleted, this.attribution];
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
   * Undoing an entry creates an operation that removes the entry's effect from
   * the SU-Set.
   *
   * Applying the returned patch is only coherent if nothing in the SU-Set is
   * caused-by this entry. So, entries must be undone from the tail of the
   * journal.
   */
  async undo() {
    const op = this.operation.asMeldOperation();
    // Undoing a journal entry involves:
    return {
      // 1. Deleting triples that were inserted. The TIDs of the inserted
      // triples always come from the entry itself, so we know exactly
      // what TripleTids were added and we can safely remove them.
      deletes: op.inserts,
      // 2. Inserting triples that were deleted. From the MeldOperation
      // by itself we don't know which TripleTids were actually removed
      // (a prior transaction may have removed the same ones). Instead,
      // the journal keeps track of the actual deletes made.
      inserts: op.byTriple('deletes', this.deleted)
    };
  }
}