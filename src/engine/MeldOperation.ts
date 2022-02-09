import { CausalOperation, CausalOperator, FusableCausalOperation, ItemTids } from './ops';
import { Triple, tripleIndexKey, TripleMap } from './quads';
import { TreeClock } from './clocks';
import { EncodedOperation } from '.';
import { MeldEncoder, RefTriple } from './MeldEncoding';
import { Iri } from 'jsonld/jsonld-spec';

/** A causal operation that may contain an agreement */
export interface AgreeableOperationSpec extends CausalOperation<Triple, TreeClock> {
  agreed: number | undefined;
}

interface MeldOperationSpec
  extends AgreeableOperationSpec, CausalOperation<RefTriple, TreeClock> {
  // Type declaration overrides to reified triples
  inserts: ItemTids<RefTriple>[],
  deletes: ItemTids<RefTriple>[]
}

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
  extends FusableCausalOperation<Triple, TreeClock>
  implements MeldOperationSpec {
  agreed: number | undefined;

  static fromOperation(
    encoder: MeldEncoder,
    spec: AgreeableOperationSpec
  ): MeldOperation {
    const { from, time, deletes, inserts, agreed } = spec;
    const [refDeletes, refInserts] =
      [deletes, inserts].map(encoder.identifyTriplesTids);
    const delTriples = encoder.reifyTriplesTids(refDeletes);
    // Encoded inserts are only reified if fused
    const insTriples = spec.from === spec.time.ticks ?
      [...inserts].map(([triple]) => triple) :
      encoder.reifyTriplesTids(refInserts);
    const jsons = [delTriples, insTriples].map(encoder.jsonFromTriples);
    const [update, encoding] = MeldEncoder.bufferFromJson(jsons);
    const encoded: EncodedOperation =
      [4, spec.from, spec.time.toJSON(), update, encoding, spec.agreed];
    return new MeldOperation({
      from, time, deletes: refDeletes, inserts: refInserts, agreed
    }, encoded, jsons);
  };

  static fromEncoded(
    encoder: MeldEncoder,
    encoded: EncodedOperation
  ): MeldOperation {
    const [ver] = encoded;
    if (ver < 3)
      throw new Error(`Encoded operation version ${ver} not supported`);
    let [, from, timeJson, update, encoding, agreed] = encoded;
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
    return new MeldOperation({ from, time, deletes, inserts, agreed }, encoded, jsons);
  };

  // Type declaration overrides to reified triples
  inserts: ItemTids<RefTriple>[];
  deletes: ItemTids<RefTriple>[];

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
  }

  fusion(): CausalOperator<AgreeableOperationSpec> {
    return new class extends MeldOperationOperator {
      update(next: AgreeableOperationSpec) {
        // Most recent agreement is significant
        if (next.agreed != null)
          this.agreed = next.agreed;
      }
    }(super.fusion(), this.agreed);
  }

  cutting(): CausalOperator<AgreeableOperationSpec> {
    return new class extends MeldOperationOperator {
      update(prev: AgreeableOperationSpec) {
        // Check if the last agreement is being cut away
        if (this.agreed != null && prev.time.ticks >= this.agreed)
          this.agreed = undefined;
      }
    }(super.cutting(), this.agreed);
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

abstract class MeldOperationOperator implements CausalOperator<AgreeableOperationSpec> {
  constructor(
    protected operator: CausalOperator<CausalOperation<Triple, TreeClock>>,
    protected agreed: number | undefined
  ) {
  }

  abstract update(op: AgreeableOperationSpec): void;

  next(op: AgreeableOperationSpec): this {
    this.operator.next(op);
    this.update(op);
    return this;
  }

  commit() {
    return { ...this.operator.commit(), agreed: this.agreed };
  }

  get footprint() {
    return this.operator.footprint;
  }
}