import {
  CausalOperation, CausalOperator, CausalTimeRange, FusableCausalOperation, ItemTids, Operation
} from './ops';
import { isLiteralTriple, Triple, tripleIndexKey, TripleMap, unTypeTriple } from './quads';
import { TreeClock } from './clocks';
import { EncodedOperation } from '.';
import { MeldEncoder, RefTriplesOp } from './MeldEncoding';
import { Iri } from '@m-ld/jsonld';
import { RefTriple } from './jrql-util';
import { IndirectedData, isSharedDatatype, MeldError, SharedDatatype, UUID } from '../api';
import { concatIter, mapIter } from './util';

const inspect = Symbol.for('nodejs.util.inspect.custom');

export interface MeldOperationParts<T extends Triple = Triple, UpdateMeta = unknown>
  extends Operation<ItemTids<T>> {
  /**
   * Triple-operations pairs.
   * The same triple should not appear multiple times.
   */
  readonly updates: Iterable<[T, UpdateMeta]>;
}

/**
 * The 'range' of an operation comprises the information required to determine
 * whether operations are logically contiguous.
 * @see MeldOperation.contiguous
 */
export interface MeldOperationRange extends CausalTimeRange<TreeClock> {
  readonly principalId: Iri | null;
}

/**
 * A causal operation that may contain an agreement, and operations on shared
 * data types
 */
export interface MeldOperationSpec<T extends Triple = RefTriple>
  extends MeldOperationParts<T>, MeldOperationRange, CausalOperation<T, TreeClock> {
  readonly agreed: OperationAgreedSpec | null;
}

export type OperationAgreedSpec = { tick: number, proof: any };

/**
 * Reversion information keyed by reference triple identifier as recorded in an
 * operation. Includes TID arrays for operation deletions, and shared data type
 * reversions for operation updates.
 *
 * Note: for TID arrays, deletes in a fusion should never delete the same
 * triple-TID twice, so no need to use a Set per triple.
 */
export type EntryReversion = { [key: Iri]: unknown };

/**
 * Fusions may require fused reversion information, which is not available in an
 * operation per-se, so has to be passed in.
 */
export interface MeldFusionOperator extends CausalOperator<MeldOperationSpec> {
  /** Cumulative reversion; contains final reversion on commit */
  readonly reversion: EntryReversion;
  /** Override with operation reversion metadata */
  next(op: MeldOperation, reversion?: EntryReversion): this;
}

/**
 * Utility type to capture combination of shared data operation (in a m-ld
 * Operation) and corresponding local revert metadata (in the Journal).
 * NOTE: Not used in SharedDatatype, for brevity.
 */
export type LocalDataOperation<Operation = unknown, Revert = unknown> =
  [Operation, Revert?];

/**
 * An immutable operation, fully expanded from the wire or journal encoding to
 * provide access to updated triples and causal operators.
 *
 * The Triples embedded in an operation have stable blank node reification
 * identifiers, which are scoped to the operation and so may not be unique
 * across operations. These will likely be lost during manipulations such as
 * fusions, which compare triples by value.
 */
export class MeldOperation
  extends FusableCausalOperation<RefTriple, TreeClock>
  implements MeldOperationSpec {
  /**
   * Does operation two continue immediately from operation one, with no
   * intermediate causes from other processes or changes of principal?
   */
  static contiguous(one: MeldOperationRange, two: MeldOperationRange) {
    return CausalTimeRange.contiguous(one, two) &&
      one.principalId === two.principalId;
  }

  /**
   * Create an operation from the given specification. Note that the triples in
   * the spec will always be given new scoped identities.
   */
  static fromOperation(
    encoder: MeldEncoder,
    { from, time, deletes, inserts, updates, principalId, agreed }: MeldOperationSpec<Triple>
  ): MeldOperation {
    const refDeletes = encoder.identifyTriplesData(deletes);
    const refInserts = encoder.identifyTriplesData(inserts);
    const refUpdates = encoder.identifyTriplesData(updates);
    // Encoded inserts are only reified if fused
    const insTriples = from === time.ticks ?
      [...inserts].map(([triple]) => triple) :
      encoder.reifyTriplesTids(refInserts);
    const delTriples = encoder.reifyTriplesTids(refDeletes);
    const jsons = [delTriples, insTriples].map(encoder.jsonFromTriples);
    if (refUpdates.length > 0) {
      const opTriples = encoder.reifyTriplesOp(refUpdates.map(
        // Operations MUST NOT include the actual typed data
        ([triple, data]) => [unTypeTriple(triple), data]));
      jsons.push(encoder.jsonFromTriples(opTriples));
    }
    const [update, encoding] = MeldEncoder.bufferFromJson(jsons);
    return new MeldOperation({
      from,
      time,
      deletes: refDeletes,
      inserts: refInserts,
      updates: refUpdates,
      principalId,
      agreed
    }, [5,
      from,
      time.toJSON(),
      update,
      encoding,
      principalId != null ? encoder.compactIri(principalId) : null,
      agreed != null ? [agreed.tick, agreed.proof] : null
    ], jsons, encoder.indirectedData);
  };

  /**
   * Create an operation from the given wire encoding. Note that the triples in
   * the spec will retain their scoped identities from the encoding.
   */
  static fromEncoded(
    encoder: MeldEncoder,
    encoded: EncodedOperation
  ): MeldOperation {
    const [ver] = encoded;
    if (ver < 3)
      throw new Error(`Encoded operation version ${ver} not supported`);
    let [, from, timeJson, update, encoding, principalId, encAgree] = encoded;
    const jsons: [object, object, object?] = MeldEncoder.jsonFromBuffer(update, encoding);
    const [delTriples, insTriples, updTriples] = jsons.map(encoder.triplesFromJson);
    const time = TreeClock.fromJson(timeJson);
    const deletes = encoder.unreifyTriplesTids(delTriples);
    let inserts: MeldOperation['inserts'];
    if (from === time.ticks) {
      const tid = time.hash;
      inserts = insTriples.map(triple => [encoder.identifyTriple(triple), [tid]]);
    } else {
      // No need to calculate transaction ID if the encoding is fused
      inserts = encoder.unreifyTriplesTids(insTriples);
    }
    const updates = (updTriples && encoder.unreifyTriplesOp(updTriples)) ?? [];
    principalId = principalId != null ? encoder.expandTerm(principalId) : null;
    const agreed = this.agreed(encAgree);
    const spec = { from, time, deletes, inserts, updates, principalId, agreed };
    return new MeldOperation(spec, encoded, jsons, encoder.indirectedData);
  }

  static agreed(encoded: [number, any] | null) {
    if (encoded != null) {
      const [tick, proof] = encoded;
      return { tick, proof };
    }
    return null;
  }

  readonly agreed: OperationAgreedSpec | null;
  readonly principalId: Iri | null;
  readonly updates: RefTriplesOp;

  /**
   * Serialisation of triples is not required to be normalised. For any m-ld
   * operation, there are many possible serialisations. An operation carries its
   * serialisation with it, for journaling and hashing.
   *
   * @param spec fully-expanded operation specification
   * @param encoded ready for wire or journal
   * @param jsons used for logging
   * @param indirectedData the active indirected data, used for fusions
   * @private because the parameters are mutually redundant
   */
  private constructor(
    spec: MeldOperationSpec,
    readonly encoded: EncodedOperation,
    readonly jsons: object,
    readonly indirectedData: IndirectedData
  ) {
    super(spec, tripleIndexKey);
    this.agreed = spec.agreed;
    this.principalId = spec.principalId;
    this.updates = [...spec.updates];
  }

  fusion(reversion?: EntryReversion): MeldFusionOperator {
    return new class extends MeldOperationOperator {
      next(op: MeldOperation, reversion?: EntryReversion) {
        super.next(op, reversion);
        // Most recent agreement is significant
        if (op.agreed != null)
          this.agreed = op.agreed;
        return this;
      }
      updateDataUpdate(
        dt: UnknownSharedDatatype,
        thisOp: LocalDataOperation | null,
        nextOp: LocalDataOperation
      ) {
        return thisOp != null ? dt.fuse(thisOp, nextOp) : nextOp;
      }
      // noinspection JSUnusedGlobalSymbols - implements MeldFusionOperator
      get reversion(): EntryReversion {
        return this.deleteTids == null ? {} : Object.fromEntries(concatIter(
          mapIter(this.deleteTids, ([triple, meta]) => [triple['@id'], meta]),
          mapIter(this.updates, ([triple, [_, meta]]) => [triple['@id'], meta]))
        );
      }
    }(super.fusion(), this, reversion);
  }

  cutting(): CausalOperator<MeldOperationSpec> {
    return new class extends MeldOperationOperator {
      next(pre: MeldOperation) {
        super.next(pre);
        // Check if the last agreement is being cut away
        if (this.agreed != null && pre.time.ticks >= this.agreed.tick)
          this.agreed = null;
        return this;
      }
      updateDataUpdate(
        dt: UnknownSharedDatatype,
        [thisOp]: LocalDataOperation,
        [preOp]: LocalDataOperation
      ): LocalDataOperation {
        return [dt.cut(preOp, thisOp)];
      }
    }(super.cutting(), this);
  }

  /**
   * Creates a pseudo-operation that removes this operation's effect
   */
  revert(reversion: EntryReversion): MeldOperationParts<Triple, LocalDataOperation> {
    return {
      // 1. Deleting triples that were inserted. The TIDs of the inserted
      // triples always come from the entry itself, so we know exactly what
      // TripleTids were added and we can safely remove them.
      deletes: this.inserts,
      // 2. Inserting triples that were deleted. From the MeldOperation by
      // itself we don't know which TripleTids were actually removed (a prior
      // transaction may have removed the same ones). Instead, the journal keeps
      // track of the actual deletes made.
      inserts: this.deleteTidsByTriple(reversion),
      // 3. Reverting shared datatype operations. For each triple, provides the
      // operation performed and the corresponding reversion metadata
      updates: this.updateMetaByTriple(reversion)
    };
  }

  byRef<T>(
    byTriple: Pick<TripleMap<T>, 'get'>,
    key: keyof MeldOperationParts
  ): { [p: Iri]: T } {
    const result: { [id: Iri]: T } = {};
    for (let [triple] of this[key]) {
      const value = byTriple.get(triple);
      if (value != null)
        result[triple['@id']] = value;
    }
    return result;
  }

  *deleteTidsByTriple(reversion: EntryReversion): Iterable<[RefTriple, UUID[]]> {
    for (let [triple] of this.deletes) {
      if (triple['@id'] in reversion)
        yield [triple, reversion[triple['@id']] as UUID[]];
    }
  }

  *updateMetaByTriple(reversion?: EntryReversion): Iterable<[RefTriple, LocalDataOperation]> {
    for (let [triple, operation] of this.updates)
      yield [triple, [operation, reversion?.[triple['@id']]]];
  }

  toString() {
    return `${JSON.stringify(this.jsons)}`;
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();

  protected sizeof(item: Triple): number {
    return tripleIndexKey(item).length;
  }
}

type UnknownSharedDatatype = SharedDatatype<unknown, unknown, unknown>;

abstract class MeldOperationOperator implements CausalOperator<MeldOperationSpec> {
  protected principalId: Iri | null;
  protected agreed: OperationAgreedSpec | null;
  protected indirectedData: IndirectedData;
  protected readonly updates: TripleMap<LocalDataOperation, RefTriple>;
  protected readonly deleteTids?: TripleMap<UUID[], RefTriple>;

  constructor(
    private _super: CausalOperator<CausalOperation<RefTriple, TreeClock>>,
    original: MeldOperation,
    reversion?: EntryReversion
  ) {
    this.updates = new TripleMap(original.updateMetaByTriple(reversion));
    this.principalId = original.principalId;
    this.agreed = original.agreed;
    this.indirectedData = original.indirectedData;
    // Capture relevant reversion information
    if (reversion != null)
      this.deleteTids = new TripleMap(original.deleteTidsByTriple(reversion));
  }

  abstract updateDataUpdate(
    dt: UnknownSharedDatatype,
    thisOp: LocalDataOperation | null,
    nextOp: LocalDataOperation
  ): LocalDataOperation | undefined;

  next(op: MeldOperation, reversion?: EntryReversion): this {
    this._super.next(op);
    this.updateDataUpdates(op, reversion);
    if (reversion != null && this.deleteTids != null) {
      for (let [triple, tids] of op.deleteTidsByTriple(reversion))
        this.deleteTids.with(triple, () => []).push(...tids);
    }
    return this;
  }

  commit(): MeldOperationSpec {
    const superOp = this._super.commit();
    if (this.deleteTids != null) {
      // Our deleteTids are keyed by triples from the constituent ops.
      // These triples' @ids will not necessarily have been conserved.
      const finalDeleteTids: [RefTriple, UUID[]][] = [];
      for (let [triple] of superOp.deletes) { // should be unique
        const tids = this.deleteTids.get(triple);
        tids && finalDeleteTids.push([triple, tids]);
      }
      this.deleteTids.clear();
      this.deleteTids.setAll(finalDeleteTids);
    }
    const { principalId, agreed, updates: localUpdates } = this;
    const updates = mapIter(localUpdates,
      ([triple, [op]]): [RefTriple, unknown] => [triple, op]);
    return { ...superOp, updates, principalId, agreed };
  }

  get footprint() {
    return this._super.footprint;
  }

  private updateDataUpdates(next: MeldOperation, reversion: EntryReversion | undefined) {
    for (let [triple, nextOp] of next.updateMetaByTriple(reversion)) {
      const thisOp = this.updates.get(triple);
      if (!isLiteralTriple(triple)) // TODO: `updates` type should mandate this
        throw new MeldError('Bad update', 'Bad shared operation encoding');
      // Note, the datatype value here is the shared datatype IRI
      const dt = this.indirectedData(triple.object.datatype.value);
      if (dt == null || !isSharedDatatype(dt))
        throw new MeldError('Bad update', 'Shared datatype not available');
      const localOp = this.updateDataUpdate(dt, thisOp, nextOp);
      if (localOp != null)
        this.updates.set(triple, localOp);
      else if (thisOp != null)
        this.updates.delete(triple);
    }
  }
}