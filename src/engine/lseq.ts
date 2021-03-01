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
    return new LseqPosId(ids, this, id);
  }

  compatible(that: LseqDef) {
    return this.radix === that.radix &&
      this.siteLength === that.siteLength;
  }
}

class LseqPosId {
  /**
   * @param ids the parsed position Id
   * @param lseq the LSEQ definition to use
   * @param id the position Id as a string, if available
   */
  constructor(
    readonly ids: { pos: number, site: string | null }[],
    readonly lseq: LseqDef,
    private id?: string) {
  }

  equals(that: LseqPosId): boolean {
    return this.ids.length === that.ids.length &&
      this.ids.every((id, level) => id === that.ids[level]);
  }

  toString(): string {
    if (this.id == null) {
      if (this.ids[this.ids.length - 1].pos === 0) // min
        this.id = '0';
      else if (this.ids[0].pos === this.lseq.radix) // max
        this.id = String.fromCharCode((this.lseq.radix - 1)
          .toString(this.lseq.radix).charCodeAt(0) + 1);
      else
        this.id = this.ids.reduce((str, id, i) =>
          str + id.pos.toString(this.lseq.radix).padStart(i + 1, '0') + id.site, '');
    }
    return this.id;
  }

  /**
   * `this` and `that` must be adjacent in the list.
   * @this is the lower bound for the new position
   * @param that the upper bound for the new position
   */
  between(that: LseqPosId, site: string, count: number = 1): LseqPosId[] {
    assert(this.lseq.compatible(that.lseq), 'incompatibleLseq');
    for (let level = 0; level < this.ids.length || level < that.ids.length; level++) {
      if (this.pos(level) > that.pos(level)) {
        return that.between(this, site);
      } else if (this.pos(level) === that.pos(level)) {
        const thisSite = this.ids[level]?.site, thatSite = that.ids[level]?.site;
        // Check for different site at this level
        // Sites can be null if comparing min & min or max & max
        if (thisSite == null && thatSite == null)
          throw new Error(ERRORS.equalPosId);
        else if (thisSite != null && thatSite != null)
          if (thisSite > thatSite)
            return that.between(this, site);
          else if (thisSite < thatSite)
            // We can legitimately extend ourselves up to the base
            for (level++; true; level++)
              if (this.pos(level) < this.base(level) - 1)
                return this.alloc(level, this.pos(level), this.base(level), that, site, count);
        // otherwise continue loop as equal pos and site
      } else if (this.pos(level) === that.pos(level) - 1) {
        // No gap at this level but space above this or below that.
        // Keep looping until someone can be extended up (this) or down (that)
        for (level++; true; level++) {
          if (this.pos(level) < this.base(level) - 1)
            return this.alloc(level, this.pos(level), this.base(level), that, site, count);
          else if (that.pos(level) > 1)
            return that.alloc(level, 0, that.pos(level), that, site, count);
        }
      } else {
        // Gap available at this level
        return this.alloc(level, this.pos(level), that.pos(level), that, site, count);
      }
    }
    throw new Error(ERRORS.equalPosId);
  }

  private alloc(level: number, lbound: number, ubound: number, next: LseqPosId, site: string, count: number) {
    const positions = this.allocIntegers(lbound, ubound, count);
    const posIds = positions.map(pos => this.cloneWith(level, pos, site));
    if (posIds.length < count) {
      const div = Math.ceil((count - posIds.length) / posIds.length);
      for (let i = 0; i < posIds.length && posIds.length < count; i += div + 1)
        posIds.splice(i + 1, 0, ...posIds[i].between(posIds[i + 1] ?? next, site, div));
    }
    // Drop any excess due to div > 1
    return posIds.slice(0, count);
  }

  private cloneWith(level: number, pos: number, site: string): LseqPosId {
    site = site.slice(0, this.lseq.siteLength).padEnd(this.lseq.siteLength, '_');
    const ids: LseqPosId['ids'] = new Array(level);
    // If extending lseq.min, fill in a null site. Also clone for safety.
    for (let i = 0; i < level && i < this.ids.length; i++)
      ids[i] = { pos: this.ids[i].pos, site: this.ids[i].site ?? site };
    // Pad with zero up to the requested level
    ids.fill({ pos: 0, site }, this.ids.length);
    // Add the new level with a generated position
    ids.push({ pos, site });
    return new LseqPosId(ids, this.lseq);
  }

  /**
   * Allocates as many positions as it can between the bounds. May return an
   * array with fewer than count positions, if there's not enough room.
   */
  private allocIntegers(lbound: number, ubound: number, count: number): number[] {
    const available = (ubound - lbound - 1);
    const mid = Math.pow(0.5, this.lseq.skew) * available;
    const spread = Math.min(mid, available - mid);
    const gap = count > 1 ? Math.floor(spread / (count - 1)) : 0;
    let pos = lbound + Math.floor(mid - (spread / 2)) + 1;
    const positions: number[] = [];
    while (pos < ubound && positions.length < count) {
      positions.push(pos);
      pos += gap || 1;
    }
    return positions;
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
  inserted(value: T, posId: string, index: number, oldIndex: [number, number]): void,
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
    this.posIdInserts.delete(posId);
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
    if (typeof posIdOrIndex == 'number') {
      Object.assign(this.indexInserts[posIdOrIndex] ??= [], data);
    } else {
      // TODO: This is a bit lame
      if (this.deletes.has(posIdOrIndex))
        throw new Error('Cannot insert at a deleted position');
      this.posIdInserts.set(posIdOrIndex, <T>data);
    }
  }

  removeInsert(posId: string) {
    this.posIdInserts.delete(posId);
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
    let index = minIndexOfSparse(existing) ?? 0, newIndex = index;
    let posId = (index - 1) in existing ?
      this.lseq.parse(existing[index - 1].posId) : this.lseq.min;
    const posIdInsertQueue = new PosIdInsertsQueue<T>(this.posIdInserts);
    const indexInsertBatch = new IndexInsertBatch<T>(this.site, notify.inserted);
    for (let prevPosId = '';
      index < this.indexInserts.length ||
      posId.toString() !== prevPosId ||
      // Don't keep iterating if all inserts are processed and index done
      (index < existing.length && index !== newIndex);
      index++
    ) {
      prevPosId = posId.toString();
      const exists: PosItem<T> | undefined = existing[index];
      // posId is already the lower bound on the next position created
      const upper = exists != null ? this.lseq.parse(exists.posId) : this.lseq.max;
      // Prepare insert items before this position if they exist
      let subIndex = 0;
      for (let inserted of posIdInsertQueue.dequeue(upper)) {
        posId = this.lseq.parse(inserted.posId);
        notify.inserted(inserted.value, inserted.posId, newIndex++, [index, subIndex++]);
      }
      // Prepare insertion of items at this (old) index if requested
      newIndex += indexInsertBatch.addAll(this.indexInserts[index], newIndex,
        Math.min(index, existing.length)); // Report beyond-end as at-end
      if (exists != null) {
        if (this.deletes.has(exists.posId)) {
          notify.deleted(exists.value, exists.posId, index);
          // Do not increment the position or new index
        } else {
          // Flush any index-based insertions prior to next old item
          posId = indexInsertBatch.flush(posId, upper);
          // If the index of the old value has change, notify
          if (newIndex !== index)
            notify.reindexed(exists.value, exists.posId, newIndex);
          // Next loop iteration must jump over the old value
          posId = this.lseq.parse(exists.posId);
          newIndex++;
        }
      }
    }
    // Flush any remaining index-based insertions
    indexInsertBatch.flush(posId, this.lseq.max);
  }

  private *iterateInserts() {
    for (let index in this.indexInserts) // Copes with sparsity
      for (let value of this.indexInserts[index])
        if (value != null)
          yield { index: Number(index), value };
    for (let [posId, value] of this.posIdInserts.entries())
      yield { posId, value };
  }
}

class PosIdInsertsQueue<T> {
  queue: PosItem<T>[];

  constructor(posIdInserts: Map<string, T>) {
    this.queue = [...posIdInserts.entries()]
      .map(([posId, value]) => ({ posId, value }))
      .sort((e1, e2) => e1.posId.localeCompare(e2.posId));
  }

  *dequeue(upper: LseqPosId) {
    while (this.queue.length > 0 && this.queue[0].posId < upper.toString())
      yield (this.queue.shift() as PosItem<T>);
  }
}

class IndexInsertBatch<T> {
  batch: { start: number, values: T[], oldIndex: number } | null = null;

  constructor(
    readonly site: string,
    readonly inserted: LseqIndexNotify<T>['inserted']) {
  }

  /** @returns count inserted */
  addAll(indexInserts: T[] | undefined, start: number, oldIndex: number) {
    if (indexInserts != null) {
      this.batch ??= { start, values: [], oldIndex };
      const size = this.batch.values.length;
      const values = this.batch.values;
      // indexInserts may be sparse, so using forEach
      indexInserts.forEach(value => values.push(value));
      return this.batch.values.length - size;
    }
    return 0;
  }

  /** @returns last inserted position ID */
  flush(posId: LseqPosId, upper: LseqPosId) {
    if (this.batch) {
      const positions = posId.between(upper, this.site, this.batch.values.length);
      for (let i = 0; i < positions.length; i++) {
        posId = positions[i];
        this.inserted(this.batch.values[i], posId.toString(),
          this.batch.start + i, [this.batch.oldIndex, i]);
      }
    }
    this.batch = null;
    return posId;
  }
}
