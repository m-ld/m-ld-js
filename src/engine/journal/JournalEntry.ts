import { MeldOperation } from '../MeldEncoding';
import { GlobalClock, TreeClock } from '../clocks';
import { JournalOperation } from './JournalOperation';
import { OperationMessage } from '../index';
import { Journal, missingOperationError, tickKey, TickKey } from '.';

/**
 * Lightweight encoding of a transaction operation reference (TID) and the previous public tick
 * from the entry's process clock.
 */
type JournalEntryJson = [
  /** Previous public tick for this entry's clock (may be remote) */
  prev: number,
  /** Operation transaction ID */
  tid: string
];

/** Immutable expansion of JournalEntryJson, with the referenced operation */
export class JournalEntry {
  static async fromJson(journal: Journal, key: TickKey, json: JournalEntryJson) {
    // Destructuring fields for convenience
    const [prev, tid] = json;
    const operation = await journal.operation(tid);
    if (operation != null)
      return new JournalEntry(journal, key, prev, operation);
    else
      throw missingOperationError(tid);
  }

  static fromOperation(journal: Journal,
    operation: MeldOperation, localTime: TreeClock, gwc: GlobalClock) {
    return new JournalEntry(journal,
      tickKey(localTime.ticks),
      gwc.getTicks(operation.time),
      JournalOperation.fromOperation(journal, operation))
  }

  private constructor(
    private readonly journal: Journal,
    readonly key: TickKey,
    readonly prev: number,
    readonly operation: JournalOperation) {
  }

  get json(): JournalEntryJson {
    return [this.prev, this.operation.tid];
  }

  commit = this.journal.saveEntry(this);

  static prev(json: JournalEntryJson) {
    const [prev] = json;
    return prev;
  }

  async next(): Promise<JournalEntry | undefined> {
    return this.journal.entryAfter(this.key);
  }

  asMessage(): OperationMessage {
    return new OperationMessage(this.prev, this.operation.json, this.operation.time);
  }
}