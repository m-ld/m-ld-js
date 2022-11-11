import { CausalOperation, CausalOperator, CausalTimeRange, FusableCausalOperation } from './ops';
import { Triple, tripleIndexKey, TripleMap } from './quads';
import { TreeClock } from './clocks';
import { EncodedOperation } from '.';
import { MeldEncoder, RefTriple } from './MeldEncoding';
import { Iri } from '@m-ld/jsonld';

/**
 * The 'range' of an operation comprises the information required to determine
 * whether operations are logically contiguous.
 * @see MeldOperation.contiguous
 */
export interface MeldOperationRange extends CausalTimeRange<TreeClock> {
  readonly principalId: Iri | null;
}

/** A causal operation that may contain an agreement */
export interface MeldOperationSpec<T extends Triple = RefTriple>
  extends MeldOperationRange, CausalOperation<T, TreeClock> {
  readonly agreed: OperationAgreedSpec | null;
}

export type OperationAgreedSpec = { tick: number, proof: any };

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
    spec: MeldOperationSpec<Triple>
  ): MeldOperation {
    const { from, time, deletes, inserts, principalId, agreed } = spec;
    const [refDeletes, refInserts] =
      [deletes, inserts].map(encoder.identifyTriplesTids);
    const delTriples = encoder.reifyTriplesTids(refDeletes);
    // Encoded inserts are only reified if fused
    const insTriples = spec.from === spec.time.ticks ?
      [...inserts].map(([triple]) => triple) :
      encoder.reifyTriplesTids(refInserts);
    const jsons = [delTriples, insTriples].map(encoder.jsonFromTriples);
    const [update, encoding] = MeldEncoder.bufferFromJson(jsons);
    const encoded: EncodedOperation = [4,
      spec.from,
      spec.time.toJSON(),
      update,
      encoding,
      principalId != null ? encoder.compactIri(principalId) : null,
      agreed != null ? [agreed.tick, agreed.proof] : null
    ];
    return new MeldOperation({
      from, time, deletes: refDeletes, inserts: refInserts, principalId, agreed
    }, encoded, jsons);
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
    const jsons: [object, object] = MeldEncoder.jsonFromBuffer(update, encoding);
    const [delTriples, insTriples] = jsons.map(encoder.triplesFromJson);
    const time = TreeClock.fromJson(timeJson);
    const deletes = MeldEncoder.unreifyTriplesTids(delTriples);
    let inserts: MeldOperation['inserts'];
    if (from === time.ticks) {
      const tid = time.hash;
      inserts = insTriples.map(triple => [encoder.identifyTriple(triple), [tid]]);
    } else {
      // No need to calculate transaction ID if the encoding is fused
      inserts = MeldEncoder.unreifyTriplesTids(insTriples);
    }
    principalId = principalId != null ? encoder.expandTerm(principalId) : null;
    const agreed = this.agreed(encAgree);
    const spec = { from, time, deletes, inserts, principalId, agreed };
    return new MeldOperation(spec, encoded, jsons);
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

  /**
   * Serialisation of triples is not required to be normalised. For any m-ld
   * operation, there are many possible serialisations. An operation carries its
   * serialisation with it, for journaling and hashing.
   *
   * @param spec fully-expanded operation specification
   * @param encoded ready for wire or journal
   * @param jsons used for logging
   * @private because the parameters are mutually redundant
   */
  private constructor(
    spec: MeldOperationSpec,
    readonly encoded: EncodedOperation,
    readonly jsons: object
  ) {
    super(spec, tripleIndexKey);
    this.agreed = spec.agreed;
    this.principalId = spec.principalId;
  }

  fusion(): CausalOperator<MeldOperationSpec> {
    return new class extends MeldOperationOperator {
      update(next: MeldOperationSpec) {
        // Most recent agreement is significant
        if (next.agreed != null)
          this.agreed = next.agreed;
      }
    }(super.fusion(), this.principalId, this.agreed);
  }

  cutting(): CausalOperator<MeldOperationSpec> {
    return new class extends MeldOperationOperator {
      update(prev: MeldOperationSpec) {
        // Check if the last agreement is being cut away
        if (this.agreed != null && prev.time.ticks >= this.agreed.tick)
          this.agreed = null;
      }
    }(super.cutting(), this.principalId, this.agreed);
  }

  byRef<T>(key: 'deletes' | 'inserts', byTriple: TripleMap<T>): { [id: Iri]: T } {
    return this[key].reduce<{ [id: Iri]: T }>((result, [triple]) => {
      const value = byTriple.get(triple);
      if (value != null)
        result[triple['@id']] = value;
      return result;
    }, {});
  }

  byTriple<T>(key: 'deletes' | 'inserts', byRef: { [id: Iri]: T }): TripleMap<T> {
    return this[key].reduce<TripleMap<T>>((result, [triple]) => {
      if (triple['@id'] in byRef)
        result.set(triple, byRef[triple['@id']]);
      return result;
    }, new TripleMap<T>());
  }

  toString() {
    return `${JSON.stringify(this.jsons)}`;
  }

  protected sizeof(item: Triple): number {
    return tripleIndexKey(item).length;
  }
}

abstract class MeldOperationOperator implements CausalOperator<MeldOperationSpec> {
  constructor(
    protected operator: CausalOperator<CausalOperation<RefTriple, TreeClock>>,
    protected principalId: Iri | null,
    protected agreed: OperationAgreedSpec | null
  ) {}

  abstract update(op: MeldOperationSpec): void;

  next(op: MeldOperationSpec): this {
    this.operator.next(op);
    this.update(op);
    return this;
  }

  commit() {
    return {
      ...this.operator.commit(),
      principalId: this.principalId,
      agreed: this.agreed
    };
  }

  get footprint() {
    return this.operator.footprint;
  }
}