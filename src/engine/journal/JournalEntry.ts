import { JournalOperation } from './JournalOperation';
import { OperationMessage } from '../index';
import type { Journal, TickKey } from '.';
import { EntryIndex } from '.';
import { MeldOperation } from '../MeldOperation';
import { UUID } from '../MeldEncoding';
import { TripleMap } from '../quads';
import { Iri } from 'jsonld/jsonld-spec';

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
  deleted: EntryDeleted
];

/** Immutable expansion of JournalEntryJson, with the referenced operation */
export class JournalEntry {
  static async fromJson(journal: Journal, key: TickKey, json: JournalEntryJson) {
    // Destructuring fields for convenience
    const [prev, tid, deleted] = json;
    const operation = await journal.operation(tid, 'require');
    return new JournalEntry(journal, key, prev, operation, deleted);
  }

  static fromOperation(
    journal: Journal,
    key: TickKey,
    prev: TickTid,
    operation: MeldOperation,
    deleted: TripleMap<UUID[]>
  ) {
    return new JournalEntry(
      journal, key, prev,
      JournalOperation.fromOperation(journal, operation),
      operation.byRef('deletes', deleted));
  }

  private constructor(
    private readonly journal: Journal,
    readonly key: TickKey,
    readonly prev: TickTid,
    readonly operation: JournalOperation,
    readonly deleted: EntryDeleted
  ) {
  }

  get index(): EntryIndex {
    return { key: this.key, tid: this.operation.tid };
  }

  get json(): JournalEntryJson {
    return [this.prev, this.operation.tid, this.deleted];
  }

  async next(): Promise<JournalEntry | undefined> {
    return this.journal.entryAfter(this.key);
  }

  asMessage(): OperationMessage {
    const [prevTick] = this.prev;
    return new OperationMessage(prevTick, this.operation.encoded, this.operation.time);
  }
}