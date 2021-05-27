import { EncodedOperation } from '../index';
import { TreeClock } from '../clocks';
import { MeldOperation } from '../MeldEncoding';
import { Kvps } from '../dataset';
import { CausalOperator } from '../ops';
import { Triple } from '../quads';
import { Journal } from '.';

/**
 * Immutable _partial_ expansion of EncodedOperation. This does not interpret the data (triples)
 * content like a `MeldOperation`, just the `from` and `time` components, and the derived
 * transaction ID.
 */
export class JournalOperation {
  static fromJson(journal: Journal, json: EncodedOperation, tid?: string) {
    // Destructuring fields for convenience
    const [, from, timeJson] = json;
    const time = TreeClock.fromJson(timeJson);
    tid ??= time.hash();
    return new JournalOperation(journal, tid, from, time, json);
  }

  static fromOperation(journal: Journal, operation: MeldOperation) {
    const { from, time, encoded } = operation;
    return new JournalOperation(journal, time.hash(), from, time, encoded);
  }

  constructor(
    private readonly journal: Journal,
    readonly tid: string,
    readonly from: number,
    readonly time: TreeClock,
    readonly operation: EncodedOperation) {
  }

  get json() {
    return this.operation;
  }

  commit: Kvps = this.journal.saveOperation(this);

  get tick() {
    return this.time.ticks;
  }

  asMeldOperation() {
    return this.journal.decode(this.operation);
  }

  /**
   * @param createOperator creates a causal reduction operator from the given first operation
   * @param minFrom the least required value of the range of the operation with
   * the returned identity
   * @returns found operations, up to and including this one
   */
  private async causalReduce(
    createOperator: (first: MeldOperation) => CausalOperator<Triple, TreeClock>,
    minFrom = 1): Promise<MeldOperation> {
    // TODO: Lock the journal against garbage compaction while processing!
    let { tid, tick } = await this.causalFrom(minFrom);
    const operator = createOperator(await this.journal.meldOperation(tid));
    while (tid !== this.tid) {
      tid = this.time.ticked(++tick).hash();
      operator.next(tid === this.tid ?
        this.asMeldOperation() : await this.journal.meldOperation(tid));
    }
    return this.journal.toMeldOperation(operator.commit());
  }

  /**
   * Works backward through the journal to find the first transaction ID (and associated tick) that
   * is causally contiguous with this one.
   * @param minFrom the least clock tick to go back to
   * @see CausalTimeRange.contiguous
   */
  private async causalFrom(minFrom: number) {
    type OpId = { tick: number, tid: string };
    const seekToFrom = async (current: OpId): Promise<OpId> => {
      const prev = await this.journal.entryPrev(current.tid);
      if (prev < minFrom || prev < current.tick - 1)
        return current; // This is as far back as we have
      else
        // Get previous tick for given tick (or our tick)
        return seekToFrom({ tick: prev, tid: this.time.ticked(prev).hash() });
    };
    return seekToFrom(this);
  }

  /**
   * Gets the causally-contiguous history of this operation, fused into a single
   * operation.
   * @see CausalTimeRange.contiguous
   */
  async fusedPast(): Promise<EncodedOperation> {
    return (await this.causalReduce(first => first.fusion())).encoded;
  }

  async cutSeen(op: MeldOperation): Promise<MeldOperation> {
    return this.causalReduce(prev => op.cutting().next(prev), op.from);
  }
}