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

  remove(key: 'deletes' | 'inserts', quads: Iterable<T> | Filter<T>): T[] {
    return [...this[key].deleteAll(quads)];
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
   * Does time range two continue immediately from time range one, with no intermediate causes from
   * other processes?
   */
  export function contiguous<C extends CausalClock>(
    one: CausalTimeRange<C>, two: CausalTimeRange<C>) {
    // Check ticks and ID.
    return one.time.ticks + 1 === two.from &&
      one.time.hash() === two.time.ticked(one.time.ticks).hash();
  }

  /**
   * Does time range one's tail overlap time range two's head?
   */
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
  extends CausalTimeRange<C>, Operation<ItemTids<T>> {
}

export type ItemTids<T> = [item: T, tids: string[]];
type ItemTid<T> = [item: T, tid: string];
namespace ItemTid {
  export const tid = (itemTid: ItemTid<unknown>) => itemTid[1];
}

export interface CausalOperator<T, C extends CausalClock> {
  next(op: CausalOperation<T, C>): CausalOperator<T, C>;
  commit(): CausalOperation<T, C>;
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
    readonly getIndex: (item: T) => string = item => `${item}`) {
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

  fusion(): CausalOperator<T, C> {
    // Not using mutable append, our semantics are different!
    const original = this;
    return new class {
      /** Lazy, in case next() never called */
      private fused: MutableOperation<ItemTid<T>> | undefined;
      private time: C | undefined;

      next(next: CausalOperation<T, C>): CausalOperator<T, C> {
        // 0. Lazily create the fusion
        this.fused ??= original.mutable();
        // 1. Fuse all deletes
        this.fused.deletes.addAll(flatten(next.deletes));
        // 2. Remove anything we insert that is deleted
        const redundant = this.fused.inserts.deleteAll(flatten(next.deletes));
        this.fused.deletes.deleteAll(redundant);
        // 3. Fuse remaining inserts (we can't be deleting any of these)
        this.fused.inserts.addAll(flatten(next.inserts));
        this.time = next.time;
        return this;
      }

      commit(): CausalOperation<T, C> {
        if (this.fused != null && this.time != null)
          return {
            from: original.from,
            time: this.time,
            deletes: original.expand(this.fused.deletes),
            inserts: original.expand(this.fused.inserts)
          };
        else
          return original;
      }

      get footprint() {
        return (this.fused ?? original).footprint;
      }
    };
  }

  /** Pre: We can fuse iff we are causally contiguous with the next operation */
  fuse(next: CausalOperation<T, C>): CausalOperation<T, C> {
    return this.fusion().next(next).commit();
  }

  cutting(): CausalOperator<T, C> {
    const original = this;
    return new class {
      /** Lazy, in case next() never called */
      private cut: MutableOperation<ItemTid<T>> | undefined;
      private from = original.from;

      next(prev: CausalOperation<T, C>): CausalOperator<T, C> {
        // Lazily create the cutting
        this.cut ??= original.mutable();
        // Remove all overlapping deletes
        this.cut.deletes.deleteAll(flatten(prev.deletes));
        // Do some indexing for TID-based parts
        const prevFlatInserts = original.newFlatIndexSet(flatten(prev.inserts));
        // Remove any deletes where tid in exclusive-prev, unless inserted in prev
        for (let tick = prev.from; tick < this.from; tick++) {
          // Can use a hash after creation for external ticks as in fusions
          const iTid = original.time.ticked(tick).hash();
          this.cut.deletes.deleteAll(deleted =>
            ItemTid.tid(deleted) === iTid && !prevFlatInserts.has(deleted));
        }
        // Add deletes for any inserts from prev where tid in intersection, unless
        // still inserted in cut, and remove all inserts from B where tid in i
        for (let tick = this.from; tick <= prev.time.ticks; tick++) {
          const aTid = original.time.ticked(tick).hash();
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
        if (this.cut != null)
          return {
            from: this.from,
            time: original.time,
            deletes: original.expand(this.cut.deletes),
            inserts: original.expand(this.cut.inserts)
          };
        else
          return original;
      }

      get footprint() {
        return (this.cut ?? original).footprint;
      }
    };
  }

  /** Pre: We can cut iff our from is within the previous range */
  cutBy(prev: CausalOperation<T, C>): CausalOperation<T, C> {
    return this.cutting().next(prev).commit();
  }

  private expand(itemTids: Iterable<ItemTid<T>>): ItemTids<T>[] {
    const expanded = this.newIndexMap();
    for (let itemTid of itemTids) {
      const [item, tid] = itemTid;
      expanded.with(item, () => []).push(tid);
    }
    return [...expanded];
  }

  private mutable(op: CausalOperation<T, C> = this): MutableOperation<ItemTid<T>> {
    const newFlatIndexSet = this.newFlatIndexSet;
    return new class extends MutableOperation<ItemTid<T>> {
      constructSet(items?: Iterable<ItemTid<T>>) {
        return newFlatIndexSet(items);
      }
    }({
      deletes: flatten(op.deletes),
      inserts: flatten(op.inserts)
    });
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

function *flatten<T>(
  itemsTids: Iterable<ItemTids<T>>): Iterable<[item: T, tid: string]> {
  for (let itemTids of itemsTids) {
    const [item, tids] = itemTids;
    for (let tid of tids)
      yield [item, tid];
  }
}
