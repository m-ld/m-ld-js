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
 */
export type EntryReversion = { [key: Iri]: unknown[] };
/**
 * Utility type to capture combination of shared data operation (from a m-ld
 * Operation) and corresponding revert metadata (from the Journal)
 */
export type OperationReversion = { operation: unknown, revert: unknown };

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

  fusion(): CausalOperator<MeldOperationSpec> {
    return new class extends MeldOperationOperator {
      update(next: MeldOperationSpec) {
        this.updateOperations(next, (dt, thisOp, nextOp) =>
          thisOp != null ? dt.fuse(thisOp, nextOp)[0] : nextOp);
        // Most recent agreement is significant
        if (next.agreed != null)
          this.agreed = next.agreed;
      }
    }(super.fusion(), this);
  }

  cutting(): CausalOperator<MeldOperationSpec> {
    return new class extends MeldOperationOperator {
      update(pre: MeldOperationSpec) {
        this.updateOperations(pre, (dt, thisOp, preOp) =>
          dt.cut(preOp, thisOp ?? this.missingOp));
        // Check if the last agreement is being cut away
        if (this.agreed != null && pre.time.ticks >= this.agreed.tick)
          this.agreed = null;
      }
    }(super.cutting(), this);
  }

  /**
   * Creates a pseudo-operation that removes this operation's effect
   */
  revert(reversion: EntryReversion): MeldOperationParts<Triple, OperationReversion> {
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
    byTriple: Pick<TripleMap<T[]>, 'get'>,
    key: keyof MeldOperationParts
  ): { [p: Iri]: T[] } {
    const result: { [id: Iri]: T[] } = {};
    for (let [triple] of this[key]) {
      const value = byTriple.get(triple);
      if (value != null)
        result[triple['@id']] = value;
    }
    return result;
  }

  *deleteTidsByTriple(byRef: { [p: Iri]: unknown[] }): Iterable<[Triple, UUID[]]> {
    for (let [triple] of this.deletes) {
      if (triple['@id'] in byRef)
        yield [triple, <UUID[]>byRef[triple['@id']]];
    }
  }

  *updateMetaByTriple(byRef: { [p: Iri]: unknown }): Iterable<[Triple, OperationReversion]> {
    for (let [triple, operation] of this.updates) {
      if (triple['@id'] in byRef)
        yield [triple, { operation, revert: byRef[triple['@id']] }];
    }
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

abstract class MeldOperationOperator implements CausalOperator<MeldOperationSpec> {
  protected readonly updates = new TripleMap<unknown, RefTriple>();
  protected principalId: Iri | null;
  protected agreed: OperationAgreedSpec | null;

  constructor(
    protected operator: CausalOperator<CausalOperation<RefTriple, TreeClock>>,
    protected original: MeldOperation
  ) {
    this.updates.setAll(original.updates);
    this.principalId = original.principalId;
    this.agreed = original.agreed;
  }

  abstract update(op: MeldOperationSpec): void;

  next(op: MeldOperationSpec): this {
    this.operator.next(op);
    this.update(op);
    return this;
  }

  commit(): MeldOperationSpec {
    const { principalId, agreed, updates } = this;
    return { ...this.operator.commit(), updates, principalId, agreed };
  }

  get footprint() {
    return this.operator.footprint;
  }

  protected updateOperations(
    op: MeldOperationSpec,
    operate: <O>(dt: SharedDatatype<unknown, O>, thisOp: O | null, opOp: O) => O | undefined
  ) {
    for (let [triple, opOp] of op.updates) {
      const thisOp = this.updates.get(triple);
      if (!isLiteralTriple(triple)) // TODO: `updates` type should mandate this
        throw new MeldError('Bad update', 'Bad shared operation encoding');
      // Note, the datatype value here is the shared datatype IRI
      const dt = this.original.indirectedData(triple.object.datatype.value);
      if (dt == null || !isSharedDatatype(dt))
        throw new MeldError('Bad update', 'Shared datatype not available');
      const op = operate(dt, thisOp, opOp);
      if (op != null)
        this.updates.set(triple, op);
      else if (thisOp != null)
        this.updates.delete(triple);
    }
  }

  protected get missingOp(): never {
    throw new MeldError('Bad update', 'Operation missing');
  }
}
