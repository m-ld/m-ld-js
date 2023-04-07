import { CausalClock } from './clocks';
import { IndexMap, IndexMatch, IndexSet } from './indices';

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
    // Iterate the deletes only once
    const patchDeletes = [...patch.deletes ?? []];
    // ins(a), del(a) == del(a)
    this.inserts.deleteAll(patchDeletes);

    this.deletes.addAll(patchDeletes);
    this.inserts.addAll(patch.inserts);
    // del(a), ins(a) == ins(a)
    this.deletes.deleteAll(this.inserts);
    return this;
  }

  remove(key: keyof Operation<any>, items: IndexMatch<T>): T[] {
    return [...this[key].deleteAll(items)];
  }

  get footprint() {
    return this.deletes.footprint + this.inserts.footprint;
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
  /**
   * Does time range two continue immediately from time range one, with no
   * intermediate causes from other processes?
   */
  export function contiguous<C extends CausalClock>(
    one: CausalTimeRange<C>, two: CausalTimeRange<C>) {
    // Are the tick ranges contiguous, and can I reach two just by ticking one?
    // Note this approach precludes two being forked from one and so possibly
    // having a different process identity.
    return one.time.ticks + 1 === two.from &&
      one.time.ticked(two.from).equals(two.time.ticked(two.from));
  }

  /**
   * Does time range one's tail overlap time range two's head?
   */
  export function overlaps<C extends CausalClock>(
    one: CausalTimeRange<C>, two: CausalTimeRange<C>) {
    return two.from >= one.from && two.from <= one.time.ticks &&
      // Are the times the same if rewound to two's from-time?
      two.time.ticked(two.from).equals(one.time.ticked(two.from));
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
  extends CausalTimeRange<C>, Operation<ItemTids<T>> {
}

export namespace CausalOperation {
  export function flatten<T>(op: Operation<ItemTids<T>>) {
    return {
      deletes: flattenItemTids(op.deletes),
      inserts: flattenItemTids(op.inserts)
    };
  }
}

export type ItemTids<T> = [item: T, tids: string[]];
type ItemTid<T> = [item: T, tid: string];
namespace ItemTid {
  export const tid = (itemTid: ItemTid<unknown>) => itemTid[1];
}

export interface CausalOperator<T> {
  next(op: T): this;
  commit(): T;
  readonly footprint: number;
}

/** Immutable */
export abstract class FusableCausalOperation<T, C extends CausalClock>
  implements CausalOperation<T, C> {
  readonly from: number;
  readonly time: C;
  readonly deletes: ItemTids<T>[];
  readonly inserts: ItemTids<T>[];

  constructor(
    { from, time, deletes, inserts }: CausalOperation<T, C>,
    readonly getIndex: (item: T) => string = item => `${item}`
  ) {
    this.from = from;
    this.time = time;
    this.deletes = [...deletes];
    this.inserts = [...inserts];
  }

  get footprint() {
    let footprint = 0;
    for (let [item] of this.deletes)
      footprint += this.sizeof(item);
    for (let [item] of this.inserts)
      footprint += this.sizeof(item);
    return footprint;
  }

  protected abstract sizeof(item: T): number;

  /** Pre: We can fuse iff we are causally contiguous with the next operation */
  fusion(): CausalOperator<CausalOperation<T, C>> {
    // Not using mutable append, our semantics are different!
    const original = this;
    return new class {
      /** Lazy, in case next() never called */
      private fused: MutableOperation<ItemTid<T>> | undefined;
      private time: C | undefined;

      next(next: CausalOperation<T, C>) {
        // 0. Lazily create the fusion
        this.fused ??= original.mutable();
        // 1. Fuse all deletes
        this.fused.deletes.addAll(flattenItemTids(next.deletes));
        // 2. Remove anything we insert that is deleted
        const redundant = this.fused.inserts.deleteAll(flattenItemTids(next.deletes));
        this.fused.deletes.deleteAll(redundant);
        // 3. Fuse remaining inserts (we can't be deleting any of these)
        this.fused.inserts.addAll(flattenItemTids(next.inserts));
        this.time = next.time;
        return this;
      }

      commit(): CausalOperation<T, C> {
        if (this.fused != null && this.time != null) {
          return {
            from: original.from,
            time: this.time,
            deletes: original.expand(this.fused.deletes),
            inserts: original.expand(this.fused.inserts)
          };
        } else {
          const { from, time, deletes, inserts } = original;
          return { from, time, deletes, inserts };
        }
      }

      get footprint() {
        return (this.fused ?? original).footprint;
      }
    };
  }

  /** Pre: We can cut iff our from is within the previous range */
  cutting(): CausalOperator<CausalOperation<T, C>> {
    const original = this;
    return new class {
      /** Lazy, in case next() never called */
      private cut: MutableOperation<ItemTid<T>> | undefined;
      private from = original.from;

      next(prev: CausalOperation<T, C>) {
        // Lazily create the cutting
        this.cut ??= original.mutable();
        // Remove all overlapping deletes
        this.cut.deletes.deleteAll(flattenItemTids(prev.deletes));
        // Do some indexing for TID-based parts
        const prevFlatInserts = original.newFlatIndexSet(flattenItemTids(prev.inserts));
        // Remove any deletes where tid in exclusive-prev, unless inserted in prev
        for (let tick = prev.from; tick < this.from; tick++) {
          // Can use a hash after creation for external ticks as in fusions
          const iTid = original.time.ticked(tick).hash;
          this.cut.deletes.deleteAll(deleted =>
            ItemTid.tid(deleted) === iTid && !prevFlatInserts.has(deleted));
        }
        // Add deletes for any inserts from prev where tid in intersection, unless
        // still inserted in cut, and remove all inserts from B where tid in i
        for (let tick = this.from; tick <= prev.time.ticks; tick++) {
          const aTid = original.time.ticked(tick).hash;
          for (let inserted of prevFlatInserts)
            if (ItemTid.tid(inserted) === aTid)
              if (!this.cut.inserts.has(inserted))
                this.cut.deletes.add(inserted);
              else
                this.cut.inserts.delete(inserted);
        }
        this.from = prev.time.ticked().ticks;
        return this;
      }

      commit(): CausalOperation<T, C> {
        if (this.cut != null) {
          return {
            from: this.from,
            time: original.time,
            deletes: original.expand(this.cut.deletes),
            inserts: original.expand(this.cut.inserts)
          };
        } else {
          const { from, time, deletes, inserts } = original;
          return { from, time, deletes, inserts };
        }
      }

      get footprint() {
        return (this.cut ?? original).footprint;
      }
    };
  }

  private expand(itemTids: Iterable<ItemTid<T>>): ItemTids<T>[] {
    return [...expandItemTids(itemTids, this.newIndexMap())];
  }

  private mutable(op: CausalOperation<T, C> = this): MutableOperation<ItemTid<T>> {
    const newFlatIndexSet = this.newFlatIndexSet;
    return new class extends MutableOperation<ItemTid<T>> {
      constructSet(items?: Iterable<ItemTid<T>>) {
        return newFlatIndexSet(items);
      }
    }(CausalOperation.flatten(op));
  }

  private newIndexMap = () => {
    const getIndex = this.getIndex;
    return new class extends IndexMap<T, string[]> {
      getIndex(key: T): string {
        return getIndex(key);
      }
    }();
  };

  private newFlatIndexSet = (items?: Iterable<ItemTid<T>>) => {
    const getIndex = this.getIndex;
    return new class WithTidsSet extends IndexSet<ItemTid<T>> {
      construct(ts?: Iterable<ItemTid<T>>) {
        return new WithTidsSet(ts);
      };
      getIndex(key: ItemTid<T>) {
        const [item, tid] = key;
        return `${getIndex(item)}^${tid}`;
      };
    }(items);
  };
}

export function expandItemTids<T, M extends IndexMap<T, string[]>>(
  itemTids: Iterable<ItemTid<T>>,
  output: M
): M {
  for (let itemTid of itemTids) {
    const [item, tid] = itemTid;
    output.with(item, () => []).push(tid);
  }
  return output;
}

export function *flattenItemTids<T>(
  itemsTids: Iterable<ItemTids<T>>
): Iterable<ItemTid<T>> {
  for (let itemTids of itemsTids) {
    const [item, tids] = itemTids;
    for (let tid of tids)
      yield [item, tid];
  }
}
