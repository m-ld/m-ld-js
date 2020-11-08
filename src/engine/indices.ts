export abstract class Index<K, T> {
  private index = new Map<string, T>();

  protected abstract getIndex(key: K): string;

  value(key: K, value: T): T | null; // Adds
  value(key: K, value: null): T | null; // Removes
  value(key: K): T | null; // Gets
  value(key: K, value?: T | null): T | null {
    const i = this.getIndex(key);
    const old = this.index.get(i) ?? null;
    if (typeof value != 'undefined') {
      if (value != null)
        this.index.set(i, value);
      else
        this.index.delete(i);
    }
    return old;
  }

  get size() {
    return this.index.size;
  }

  [Symbol.iterator]() {
    return this.index.values();
  }
}

export abstract class IndexMap<K, V> extends Index<K, [K, V]> {
  constructor(map?: Iterable<[K, V]>) {
    super();
    if (map != null)
      this.setAll(map);
  }

  with(key: K, factory: () => V): V {
    let value = this.get(key);
    if (value == null) {
      value = factory();
      this.set(key, value);
    }
    return value;
  }

  get(key: K): V | null {
    return this.value(key)?.[1] ?? null;
  }

  set(key: K, value: V): V | null {
    return this.value(key, [key, value])?.[1] ?? null;
  }

  delete(key: K): V | null {
    return this.value(key, null)?.[1] ?? null;
  }

  setAll(entries: Iterable<[K, V]>): IndexMap<K, V> {
    for (let [key, value] of entries)
      this.set(key, value);
    return this;
  }
}

export abstract class IndexSet<T> extends Index<T, T> {
  constructor(ts?: Iterable<T>) {
    super();
    this.addAll(ts);
  }

  protected abstract construct(ts?: Iterable<T>): IndexSet<T>;

  has(t: T): boolean {
    return this.value(t) != null;
  }

  add(t: T): boolean {
    return this.value(t, t) == null;
  }

  delete(t: T): boolean {
    return this.value(t, null) != null;
  }

  addAll(ts: Iterable<T> | undefined): IndexSet<T> {
    for (let t of (ts ?? []))
      this.add(t);
    return this;
  }

  deleteAll(ts: Iterable<T> | Filter<T>): IndexSet<T> {
    const removed = this.construct();
    let filter: Filter<T>;
    if (typeof ts == 'function') {
      filter = ts;
    } else {
      const toRemove = ts instanceof IndexSet ? ts : this.construct(ts);
      filter = t => toRemove.has(t);
    }
    for (let t of this) {
      if (filter(t)) {
        this.delete(t);
        removed.add(t);
      }
    }
    return removed;
  }
}

export type Filter<T> = (t: T) => boolean;
