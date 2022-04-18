import { EncodedOperation } from '../index';
import { TreeClock } from '../clocks';
import { Kvps } from '../dataset';
import type { Journal } from '.';
import { MeldOperation, MeldOperationRange, OperationAgreedSpec } from '../MeldOperation';
import { Iri } from 'jsonld/jsonld-spec';

/**
 * Immutable _partial_ expansion of EncodedOperation. This does not interpret
 * the data (triples) content like a `MeldOperation`, just the required
 * components for operation range, and the derived transaction ID.
 */
export class JournalOperation implements MeldOperationRange {
  static fromJson(journal: Journal, json: EncodedOperation, tid?: string) {
    // Destructuring fields for range definition
    const [, from, timeJson, , , principalId, agreed] = json;
    const time = TreeClock.fromJson(timeJson);
    tid ??= time.hash;
    return new JournalOperation(
      journal, tid, from, time, principalId, MeldOperation.agreed(agreed), json);
  }

  static fromOperation(journal: Journal, op: MeldOperation) {
    const { from, time, encoded, principalId, agreed } = op;
    return new JournalOperation(
      journal, time.hash, from, time, principalId, agreed, encoded, op);
  }

  // noinspection JSUnusedGlobalSymbols: IDE does not recognise fields implementing interface
  constructor(
    private readonly journal: Journal,
    readonly tid: string,
    readonly from: number,
    readonly time: TreeClock,
    readonly principalId: Iri | null,
    readonly agreed: OperationAgreedSpec | null,
    readonly encoded: EncodedOperation,
    private _meldOperation?: MeldOperation
  ) {}

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