import { EncodedOperation } from '../index';
import { TreeClock } from '../clocks';
import { Kvps } from '../dataset';
import { CausalTimeRange } from '../ops';
import type { Journal } from '.';
import { AgreeableOperationSpec, MeldOperation } from '../MeldOperation';

/**
 * Immutable _partial_ expansion of EncodedOperation. This does not interpret
 * the data (triples) content like a `MeldOperation`, just the `from` and `time`
 * components, and the derived transaction ID.
 */
export class JournalOperation implements CausalTimeRange<TreeClock> {
  static fromJson(journal: Journal, json: EncodedOperation, tid?: string) {
    // Destructuring fields for convenience
    const [, from, timeJson, , , agreed] = json;
    const time = TreeClock.fromJson(timeJson);
    tid ??= time.hash;
    return new JournalOperation(
      journal, tid, from, time, MeldOperation.agreed(agreed), json);
  }

  static fromOperation(journal: Journal, operation: MeldOperation) {
    const { from, time, encoded } = operation;
    return new JournalOperation(journal, time.hash,
      from, time, operation.agreed, encoded, operation);
  }

  private constructor(
    private readonly journal: Journal,
    readonly tid: string,
    readonly from: number,
    readonly time: TreeClock,
    readonly agreed: AgreeableOperationSpec['agreed'] | undefined,
    readonly encoded: EncodedOperation,
    private _meldOperation?: MeldOperation) {
  }

  commit: Kvps = this.journal.commitOperation(this);

  get tick() {
    return this.time.ticks;
  }

  asMeldOperation() {
    if (this._meldOperation == null)
      this._meldOperation = this.journal.decode(this.encoded);
    return this._meldOperation;
  }

  /**
   * Gets the causally-contiguous history of this operation (inclusive), fused
   * into a single operation.
   *
   * @see CausalTimeRange.contiguous
   */
  async fusedPast(): Promise<EncodedOperation> {
    return (await this.journal.causalReduce(this, first => first.fusion())).encoded;
  }

  /**
   * Cuts this operation _and all its contiguous causal history_ from the given
   * operation.
   *
   * @param op the operation to cut from
   */
  cutSeen(op: MeldOperation): Promise<MeldOperation> {
    return this.journal.causalReduce(this, prev => op.cutting().next(prev), op.from);
  }
}