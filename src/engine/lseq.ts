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
   * strings will error (insufficient entropy).
   */
  siteLength: number = 16;

  get min() {
    return new LseqPosId([{ pos: 0, site: null }], this);
  }

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
    assert(id, 'badId');
    for (let idStart = 0, len = 1; idStart < id.length; idStart += (len++ + this.siteLength)) {
      const siteStart = idStart + len;
      assert(siteStart + this.siteLength <= id.length, 'badId');
      const pos = Number.parseInt(id.slice(idStart, siteStart), this.radix);
      assert(Number.isInteger(pos) && pos >= 0, 'badId');
      const site = id.slice(siteStart, siteStart + this.siteLength);
      ids.push({ pos, site });
    }
    assert(ids[ids.length - 1].pos > 0, 'badId');
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
   * `this` and `that` assumed to be adjacent in LSEQ.
   * @param that 
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
    return this.ids.reduce((str, id, i) =>
      str + id.pos.toString(this.lseq.radix).padStart(i + 1, '0') + id.site, '');
  }

  private cloneWith(level: number, lbound: number, ubound: number, site: string): LseqPosId {
    return new LseqPosId(this.ids.slice(0, level).concat({
      pos: this.newPos(lbound, ubound),
      site: site.slice(0, this.lseq.siteLength).padEnd(this.lseq.siteLength, '_')
    }), this.lseq);
  }

  private newPos(lbound: number, ubound: number): number {
    return lbound + Math.floor(Math.pow(Math.random(), this.lseq.skew) * (ubound - lbound - 1)) + 1;
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

function assert(condition: any, err: keyof typeof ERRORS) {
  if (!condition)
    throw new Error(ERRORS[err]);
}
