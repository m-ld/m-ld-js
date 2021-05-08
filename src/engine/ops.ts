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

/** Immutable */
export class CausalOperation<T, C extends CausalClock> implements Operation<[T, string[]]> {
  constructor(
    /**
     * First tick included in operation, <= `time.ticks`. The first tick _must_
     * be causally contiguous with the operation `time`. So, it's always
     * legitimate to do `this.time.ticked(this.from)`.
     */
    readonly from: number,
    /**
     * Operation time at the operation source process
     */
    readonly time: C,
    /**
     * Deleted items with time hashes. Hashes may represent any time strictly
     * prior to `from`.
     */
    readonly deletes: [T, string[]][],
    /**
     * Inserted items with time hashes. Hashes only represent times in this
     * operation's time range. Therefore, they are redundant with the {@link time}
     * if `this.from === this.time.ticks`.
     */
    readonly inserts: [T, string[]][]) {
  }

  get fromTime() {
    return this.time.ticked(this.from);
  }

  fuse(next: CausalOperation<T, C>): CausalOperation<T, C> | null {
    // We can fuse iff we are causally contiguous with the next operation
    if (this.time.ticked().equals(next.fromTime)) {
      // Not using mutable append, our semantics are different!
      const fused = this.mutable();
      // 1. Fuse all deletes
      fused.deletes.addAll(flatten(next.deletes));
      // 2. Remove anything we insert that is deleted
      const redundant = fused.inserts.deleteAll(flatten(next.deletes));
      fused.deletes.deleteAll(redundant);
      // 3. Fuse remaining inserts (we can't be deleting any of these)
      fused.inserts.addAll(flatten(next.inserts));
      const myGetIndex = this.getIndex;
      return new class extends CausalOperation<T, C> {
        getIndex(key: T): string {
          return myGetIndex(key);
        }
      }(this.from, next.time, this.expand(fused.deletes), this.expand(fused.inserts));
    }
    return null;
  }

  protected getIndex(_key: T): string {
    throw undefined;
  }

  private expand(itemTids: Iterable<[T, string]>): [T, string[]][] {
    const myGetIndex = this.getIndex.bind(this);
    const expanded = new class extends IndexMap<T, string[]> {
      protected getIndex(key: T): string {
        return myGetIndex(key);
      }
    }();
    for (let itemTid of itemTids) {
      const [item, tid] = itemTid;
      expanded.with(item, () => []).push(tid);
    }
    return [...expanded];
  }

  private mutable(): MutableOperation<[T, string]> {
    const myGetIndex = this.getIndex.bind(this);
    class WithTidsSet extends IndexSet<[T, string]> {
      construct(ts?: Iterable<[T, string]>) {
        return new WithTidsSet(ts);
      };
      getIndex(key: [T, string]) {
        const [item, tid] = key;
        return `${myGetIndex(item)}^${tid}`;
      };
    };
    return new class extends MutableOperation<[T, string]> {
      constructSet(items?: Iterable<[T, string]>) {
        return new WithTidsSet(items);
      }
    }({
      deletes: flatten(this.deletes),
      inserts: flatten(this.inserts)
    });
  }
}

function* flatten<T>(itemsTids: Iterable<[T, string[]]>): Iterable<[T, string]> {
  for (let itemTids of itemsTids) {
    const [item, tids] = itemTids;
    for (let tid of tids)
      yield [item, tid];
  }
}
