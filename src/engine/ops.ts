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

export interface CausalOperation<T, C extends CausalClock> extends Operation<[T, string[]]> {
  /**
   * First tick included in operation, <= `time.ticks`. The first tick _must_
   * be causally contiguous with the operation `time`. So, it's always
   * legitimate to do `this.time.ticked(this.from)`.
   */
  readonly from: number;
  /**
   * Operation time at the operation source process
   */
  readonly time: C;
  /**
   * Deleted items with time hashes. Hashes may represent any time strictly
   * prior to `from`.
   */
  readonly deletes: [T, string[]][];
  /**
   * Inserted items with time hashes. Hashes only represent times in this
   * operation's time range. Therefore, they are redundant with the {@link time}
   * if `this.from === this.time.ticks`.
   */
  readonly inserts: [T, string[]][];
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
    this.deletes = deletes;
    this.inserts = inserts;
  }

  fuse(next: CausalOperation<T, C>): CausalOperation<T, C> | undefined {
    // We can fuse iff we are causally contiguous with the next operation
    if (this.time.ticked().equals(next.time.ticked(next.from))) {
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
        from: this.from, time: next.time,
        deletes: this.expand(fused.deletes),
        inserts: this.expand(fused.inserts)
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

  private mutable(): MutableOperation<[T, string]> {
    const newFlatIndexSet = this.newFlatIndexSet;
    return new class extends MutableOperation<[T, string]> {
      constructSet(items?: Iterable<[T, string]>) {
        return newFlatIndexSet().addAll(items);
      }
    }({
      deletes: flatten(this.deletes),
      inserts: flatten(this.inserts)
    });
  }

  private newIndexMap = () => {
    const getIndex = this.getIndex;
    return new class extends IndexMap<T, string[]> {
      getIndex(key: T): string {
        return getIndex(key);
      }
    }();
  }

  private newFlatIndexSet = () => {
    const getIndex = this.getIndex;
    return new class WithTidsSet extends IndexSet<[T, string]> {
      construct(ts?: Iterable<[T, string]>) {
        return new WithTidsSet(ts);
      };
      getIndex(key: [T, string]) {
        const [item, tid] = key;
        return `${getIndex(item)}^${tid}`;
      };
    }();
  }
}

function* flatten<T>(itemsTids: Iterable<[T, string[]]>): Iterable<[T, string]> {
  for (let itemTids of itemsTids) {
    const [item, tids] = itemTids;
    for (let tid of tids)
      yield [item, tid];
  }
}
