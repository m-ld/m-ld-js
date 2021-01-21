import { minIndexOfSparse } from './util';

/**
 * LSEQ-like CRDT helper class, for generating list position identifiers.
 *
 * 🚧 Not strictly an LSEQ:
 * - the base grows by multiples of the radix (not of two)
 * - uses a consistently skewed distribution, not an alternating boundary (and
 *   therefore not as adaptable)
 *
 * @see LSEQ an Adaptive Structure for Sequences in Distributed Collaborative
 * Editing (https://hal.archives-ouvertes.fr/hal-00921633/document)
 * @see Logoot: A Scalable Optimistic Replication Algorithm for Collaborative
 * Editing on P2P Networks (https://hal.inria.fr/inria-00432368/document) §4.1.
 * Logoot model _provides much of the basic structure and terminology used here._
 */
export class LseqDef {
  /**
   * Radix for string conversion. Allowed values per Number.parseInt argument
   */
  radix: number = 36;
  /**
   * Distribution skew, power of random position in gap.
   * >1 => leftward skew, better for top-to-bottom list.
   */
  skew: number = 3;
  /**
   * Length of site identifier. Longer strings are right-trimmed. Shorter
   * strings will be padded (but this should only occur in testing – if a site
   * identifiers are frequently shorter than this length, are they unique
   * enough? And if so, make the length shorter).
   */
  siteLength: number = 16;

  /**
   * Below-lower bound position identifier used for generating a new list head.
   * Do not use this value in a list (its `toString` method will throw).
   */
  get min() {
    return new LseqPosId([{ pos: 0, site: null }], this);
  }

  /**
   * Above-upper bound position identifier used for generating a new list tail.
   * Do not use this value in a list (its `toString` method will throw).
   */
  get max() {
    return new LseqPosId([{ pos: this.radix, site: null }], this);
  }

  /**
   * - id can be '0' (list start), 'Infinity' (list end) or
   * - id length must be a natural number from the sequence 1 + 2 + 3 + ...
   * - id characters must be valid for the given radix
   * - last id part must be >0
   * @param id position identifier as string
   */
  parse(id: string): LseqPosId {
    const ids = [];
    assert(id, 'badId', id);
    for (let idStart = 0, len = 1; idStart < id.length; idStart += (len++ + this.siteLength)) {
      const siteStart = idStart + len;
      assert(siteStart + this.siteLength <= id.length, 'badId', id);
      const pos = Number.parseInt(id.slice(idStart, siteStart), this.radix);
      assert(Number.isInteger(pos) && pos >= 0, 'badId', id);
      const site = id.slice(siteStart, siteStart + this.siteLength);
      ids.push({ pos, site });
    }
    assert(ids[ids.length - 1].pos > 0, 'badId', id);
    return new LseqPosId(ids, this);
  }

  compatible(that: LseqDef) {
    return this.radix === that.radix &&
      this.siteLength === that.siteLength;
  }
}

class LseqPosId {
  constructor(
    readonly ids: { pos: number, site: string | null }[],
    readonly lseq: LseqDef) {
  }

  equals(that: LseqPosId): boolean {
    return this.ids.length === that.ids.length &&
      this.ids.every((id, level) => id === that.ids[level]);
  }

  /**
   * `this` and `that` must be adjacent in the list.
   * @this is the lower bound for the new position
   * @param that the upper bound for the new position
   */
  // TODO: Make this method able to return multiple positions, and allocate the
  // range of positions optimally
  between(that: LseqPosId, site: string): LseqPosId {
    assert(this.lseq.compatible(that.lseq), 'incompatibleLseq');
    for (let level = 0; level < this.ids.length || level < that.ids.length; level++) {
      if (this.pos(level) > that.pos(level)) {
        return that.between(this, site);
      } else if (this.pos(level) === that.pos(level)) {
        const thisId = this.ids[level], thatId = that.ids[level];
        // Check for different site at this level
        // Sites can only be null if comparing min & min or max & max
        if (thisId.site == null || thatId.site == null)
          throw new Error(ERRORS.equalPosId);
        if (thisId.site > thatId.site)
          return that.between(this, site);
        else if (thisId.site < thatId.site)
          // We can legitimately extend ourselves up to the base
          for (level++; true; level++)
            if (this.pos(level) < this.base(level) - 1)
              return this.cloneWith(level, this.pos(level), this.base(level), site);
        // otherwise continue loop as equal pos and site
      } else if (this.pos(level) === that.pos(level) - 1) {
        // No gap at this level but space above this or below that.
        // Keep looping until someone can be extended up (this) or down (that)
        for (level++; true; level++) {
          if (this.pos(level) < this.base(level) - 1)
            return this.cloneWith(level, this.pos(level), this.base(level), site);
          else if (that.pos(level) > 1)
            return that.cloneWith(level, 0, that.pos(level), site);
        }
      } else {
        // Gap available at this level
        return this.cloneWith(level, this.pos(level), that.pos(level), site);
      }
    }
    throw new Error(ERRORS.equalPosId);
  }

  toString(): string {
    if (this.ids[this.ids.length - 1].pos === 0) // min
      return '0';
    else if (this.ids[0].pos === this.lseq.radix) // max
      return String.fromCharCode((this.lseq.radix - 1)
        .toString(this.lseq.radix).charCodeAt(0) + 1);
    else
      return this.ids.reduce((str, id, i) =>
        str + id.pos.toString(this.lseq.radix).padStart(i + 1, '0') + id.site, '');
  }

  private cloneWith(level: number, lbound: number, ubound: number, site: string): LseqPosId {
    site = site.slice(0, this.lseq.siteLength).padEnd(this.lseq.siteLength, '_');
    return new LseqPosId(this.ids
      .slice(0, level)
      // If extending lseq.min, fill in a null site. Also clone for safety.
      .map(({ pos, site: maybeSite }) => ({ pos, site: maybeSite ?? site }))
      .concat({ pos: this.newPos(lbound, ubound), site }), this.lseq);
  }

  private newPos(lbound: number, ubound: number): number {
    const skewedRand = Math.pow(Math.random(), this.lseq.skew);
    return lbound + Math.floor(skewedRand * (ubound - lbound - 1)) + 1;
  }

  private pos(level: number) {
    return this.ids[level]?.pos ?? 0;
  }

  private base(level: number) {
    return Math.pow(this.lseq.radix, level + 1);
  }
}

const ERRORS = {
  badId: 'Bad LSEQ position identifier',
  incompatibleLseq: 'LSEQ definitions are not compatible',
  equalPosId: 'No space between positions'
};

function assert(condition: any, err: keyof typeof ERRORS, detail?: string) {
  if (!condition)
    throw new Error(detail == null ? ERRORS[err] : `${ERRORS[err]}: ${detail}`);
}

export interface LseqIndexNotify<T> {
  deleted(value: T, posId: string, index: number): void,
  inserted(value: T, posId: string, index: number): void,
  reindexed(value: T, posId: string, index: number): void
}

export type PosItem<T> = { value: T, posId: string };

/**
 * Utility to rewrite cached numeric indexes and positions of an LSEQ, based on
 * inserts and deletions requested at given indexes or position IDs.
 */
export class LseqIndexRewriter<T> {
  /**
   * Deleted positions
   */
  deletes = new Set<string>();
  /**
   * Inserts for which the required index is known but the final position is
   * not. Sparse array. The second dimension is to capture multiple inserts at
   * an index (will be collapsed if sparse).
   */
  private indexInserts: T[][] = [];
  /**
   * Inserts for which the position is known but the index is not.
   */
  private posIdInserts = new Map<string, T>();

  constructor(
    readonly lseq: LseqDef,
    readonly site: string) {
  }

  get inserts() {
    return this.iterateInserts();
  }

  /**
   * @param posId the position ID of the deleted value
   */
  addDelete(posId: string) {
    this.deletes.add(posId);
  }

  /**
   * @param items opaque data to insert in the list
   * @param index index to insert at
   */
  addInsert(items: T[], index: number): void;
  /**
   * @param value opaque data to insert in the list
   * @param posId position ID to insert at
   */
  addInsert(value: T, posId: string): void;
  addInsert(data: T | T[], posIdOrIndex: number | string) {
    if (typeof posIdOrIndex == 'number')
      Object.assign(this.indexInserts[posIdOrIndex] ??= [], data);
    else
      this.posIdInserts.set(posIdOrIndex, <T>data);
  }

  /**
   * Indicates the domain of indexes and position IDs that this rewriter will
   * affect in a call to {@link rewriteIndexes}, so long as no further calls to
   * `addDelete` or `addInsert` are made.
   */
  get domain() {
    const posIds = [...this.deletes, ...this.posIdInserts.keys()].sort();
    return {
      index: {
        min: minIndexOfSparse(this.indexInserts) ?? Infinity,
        // TODO: Does not cover re-indexing beyond the final insert
        max: this.indexInserts.length - 1
      },
      posId: {
        min: posIds[0] ?? '\uFFFF',
        max: posIds[posIds.length - 1] ?? ''
      }
    };
  }

  /**
   * Starting from the minimum affected index, generates LSEQ position
   * identifiers for the inserted indexes and rewrites existing indexes
   * @param existing preconditions:
   * - sorted by posId
   * - contiguous (no gaps after first non-empty array position)
   * - index >= domain.index.min
   * - value >= domain.posId.min
   */
  rewriteIndexes(existing: PosItem<T>[], notify: LseqIndexNotify<T>) {
    let oldIndex = minIndexOfSparse(existing) ?? 0, newIndex = oldIndex;
    let posId = (oldIndex - 1) in existing ?
      this.lseq.parse(existing[oldIndex - 1].posId) : this.lseq.min;
    let prevPosId = '';
    const posIdInsertQueue: PosItem<T>[] = [...this.posIdInserts.entries()]
      .map(([posId, value]) => ({ posId, value }))
      .sort((e1, e2) => e1.posId.localeCompare(e2.posId));
    while (posId.toString() !== prevPosId || oldIndex < this.indexInserts.length ||
      // Don't keep iterating if all inserts are processed and index done
      (oldIndex < existing.length && oldIndex !== newIndex)) {
      prevPosId = posId.toString();
      const exists: PosItem<T> | undefined = existing[oldIndex];
      // posId is the lower bound on the next position created
      const upper = exists != null ? this.lseq.parse(exists.posId) : this.lseq.max,
        upperStr = upper.toString();
      // Insert items before this position if requested
      while (posIdInsertQueue.length > 0 && posIdInsertQueue[0].posId < upperStr) {
        const insert = posIdInsertQueue.shift() as PosItem<T>;
        notify.inserted(insert.value, insert.posId, newIndex);
        posId = this.lseq.parse(insert.posId);
        newIndex++;
      }
      // Insert items at this (old) index if requested
      for (let value of this.indexInserts[oldIndex] ?? []) {
        posId = posId.between(upper, this.site);
        notify.inserted(value, posId.toString(), newIndex);
        newIndex++;
      }
      if (exists != null) {
        if (this.deletes.has(exists.posId)) {
          notify.deleted(exists.value, exists.posId, oldIndex);
          // Do not increment the position or new index
        } else {
          // If the index of the old value has change, notify
          if (newIndex !== oldIndex)
            notify.reindexed(exists.value, exists.posId, newIndex);
          // Next loop iteration must jump over the old value
          posId = this.lseq.parse(exists.posId);
          newIndex++;
        }
      }
      oldIndex++;
    }
  }

  private * iterateInserts() {
    for (let index in this.indexInserts) // Copes with sparsity
      for (let value of this.indexInserts[index])
        if (value != null)
          yield { index: Number(index), value };
    for (let [posId, value] of this.posIdInserts.entries())
      yield { posId, value };
  }
}
