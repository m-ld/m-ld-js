import { CausalClock } from './clocks';
import { Filter, IndexMap, IndexSet } from './indices';

export interface Operation<T> {
  readonly deletes: Iterable<T>;
  readonly inserts: Iterable<T>;
}

export abstract class MutableOperation<T> implements Operation<T> {
  readonly deletes: IndexSet<T>;
  readonly inserts: IndexSet<T>;

  constructor({ deletes = [], inserts = [] }: Partial<Operation<T>> = {}) {
    this.deletes = this.constructSet(deletes);
    this.inserts = this.constructSet(inserts);
    // del(a), ins(a) == ins(a)
    this.deletes.deleteAll(this.inserts);
  }

  protected abstract constructSet(items?: Iterable<T>): IndexSet<T>;

  get isEmpty() {
    return this.inserts.size === 0 && this.deletes.size === 0;
  }

  append(patch: Partial<Operation<T>>) {
    // ins(a), del(a) == del(a)
    this.inserts.deleteAll(patch.deletes);

    this.deletes.addAll(patch.deletes);
    this.inserts.addAll(patch.inserts);
    // del(a), ins(a) == ins(a)
    this.deletes.deleteAll(this.inserts);
    return this;
  }

  remove(key: keyof Operation<T>, quads: Iterable<T> | Filter<T>): T[] {
    return [...this[key].deleteAll(quads)];
  }
}

export interface CausalTimeRange<C extends CausalClock> {
  /**
   * First tick included in range, <= `time.ticks`. The first tick _must_
   * be causally contiguous with the operation `time`. So, it's always
   * legitimate to do `this.time.ticked(this.from)`.
   */
  readonly from: number;
  /**
   * Operation time at the operation source process
   */
  readonly time: C;
}

export namespace CausalTimeRange {
  /** Does time range two continue immediately from time range one? */
  export function contiguous<C extends CausalClock>(
    one: CausalTimeRange<C>, two: CausalTimeRange<C>) {
    // Check ticks and ID.
    return one.time.ticks + 1 === two.from &&
      one.time.hash() === two.time.ticked(one.time.ticks).hash();
  }

  /** Does time range one's tail overlap time range two's head? */
  export function overlaps<C extends CausalClock>(
    one: CausalTimeRange<C>, two: CausalTimeRange<C>) {
    return two.from >= one.from && two.from <= one.time.ticks &&
      // Do both times rewound to our from-time have the same hash?
      two.time.ticked(two.from).hash() === one.time.ticked(two.from).hash();
  }
}

/**
 * An operation on tuples of items and time hashes.
 * - Delete hashes may represent any time strictly prior to `from`.
 * - Insert hashes only represent times in this operation's time range.
 *   Therefore, they are redundant with the `time` if `this.from ===
 *   this.time.ticks`.
 */
export interface CausalOperation<T, C extends CausalClock>
  extends CausalTimeRange<C>, Operation<[T, string[]]> {
}

/** Immutable */
export class FusableCausalOperation<T, C extends CausalClock> implements CausalOperation<T, C> {
  readonly from: number;
  readonly time: C;
  readonly deletes: [T, string[]][];
  readonly inserts: [T, string[]][];

  constructor(
    { from, time, deletes, inserts }: CausalOperation<T, C>,
    readonly getIndex: (item: T) => string = item => `${item}`) {
    this.from = from;
    this.time = time;
    this.deletes = [...deletes];
    this.inserts = [...inserts];
  }

  fuse(next: CausalOperation<T, C>): CausalOperation<T, C> | undefined {
    // We can fuse iff we are causally contiguous with the next operation.
    if (CausalTimeRange.contiguous(this, next)) {
      // Not using mutable append, our semantics are different!
      const fused = this.mutable();
      // 1. Fuse all deletes
      fused.deletes.addAll(flatten(next.deletes));
      // 2. Remove anything we insert that is deleted
      const redundant = fused.inserts.deleteAll(flatten(next.deletes));
      fused.deletes.deleteAll(redundant);
      // 3. Fuse remaining inserts (we can't be deleting any of these)
      fused.inserts.addAll(flatten(next.inserts));
      return {
        from: this.from,
        time: next.time,
        deletes: this.expand(fused.deletes),
        inserts: this.expand(fused.inserts)
      };
    }
  }

  cutBy(prev: Omit<CausalOperation<T, C>, 'deletes'>): CausalOperation<T, C> | undefined {
    // We can cut iff our from is within the previous range
    if (CausalTimeRange.overlaps(prev, this)) {
      const cut = this.mutable(),
        // Also do some indexing
        prevFlatInserts = this.newFlatIndexSet(flatten(prev.inserts)),
        thisDeletesByTid = this.byTid(this.deletes),
        prevInsertsByTid = this.byTid(prev.inserts);
      // Remove any deletes where tid in exclusive-prev, unless inserted in prev
      for (let tick = prev.from; tick < this.from; tick++) {
        // Can use a hash after creation for external ticks as in fusions
        const tid = this.time.ticked(tick).hash();
        for (let item of thisDeletesByTid[tid] ?? [])
          if (!prevFlatInserts.has([item, tid]))
            cut.deletes.delete([item, tid]);
      }
      // Add deletes for any inserts from prev where tid in intersection, unless
      // still inserted in cut
      for (let tick = this.from; tick <= prev.time.ticks; tick++) {
        const tid = this.time.ticked(tick).hash();
        for (let item of prevInsertsByTid[tid] ?? [])
          if (!cut.inserts.has([item, tid]))
            cut.deletes.add([item, tid]);
      }
      return {
        from: prev.time.ticked().ticks,
        time: this.time,
        deletes: this.expand(cut.deletes),
        inserts: this.expand(cut.inserts)
      };
    }
  }

  private expand(itemTids: Iterable<[T, string]>): [T, string[]][] {
    const expanded = this.newIndexMap();
    for (let itemTid of itemTids) {
      const [item, tid] = itemTid;
      expanded.with(item, () => []).push(tid);
    }
    return [...expanded];
  }

  private mutable(op: CausalOperation<T, C> = this): MutableOperation<[T, string]> {
    const newFlatIndexSet = this.newFlatIndexSet;
    return new class extends MutableOperation<[T, string]> {
      constructSet(items?: Iterable<[T, string]>) {
        return newFlatIndexSet(items);
      }
    }({
      deletes: flatten(op.deletes),
      inserts: flatten(op.inserts)
    });
  }

  private byTid(part: Iterable<[T, string[]]>) {
    const pbt: { [tid: string]: T[] } = {};
    for (let [item, tid] of flatten(part))
      (pbt[tid] ??= []).push(item);
    return pbt;
  }

  private newIndexMap = () => {
    const getIndex = this.getIndex;
    return new class extends IndexMap<T, string[]> {
      getIndex(key: T): string {
        return getIndex(key);
      }
    }();
  }

  private newFlatIndexSet = (items?: Iterable<[T, string]>) => {
    const getIndex = this.getIndex;
    return new class WithTidsSet extends IndexSet<[T, string]> {
      construct(ts?: Iterable<[T, string]>) {
        return new WithTidsSet(ts);
      };
      getIndex(key: [T, string]) {
        const [item, tid] = key;
        return `${getIndex(item)}^${tid}`;
      };
    }(items);
  }
}

function* flatten<T>(itemsTids: Iterable<[T, string[]]>): Iterable<[T, string]> {
  for (let itemTids of itemsTids) {
    const [item, tids] = itemTids;
    for (let tid of tids)
      yield [item, tid];
  }
}
