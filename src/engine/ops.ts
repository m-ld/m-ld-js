import { CausalClock } from './clocks';
import { Filter, IndexSet } from './indices';

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
     * Deleted items with time hashes. Hashes may represent any time prior to
     * `time`.
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

  fuse(next: this): this | null {
    // We can fuse iff we are causally contiguous with the next operation
    if (this.time.ticked().equals(next.fromTime)) {
      // TODO
    }
    return null;
  }
}

