import { MsgPack, sha1Digest } from './util';

const inspect = Symbol.for('nodejs.util.inspect.custom');

export interface CausalClock {
  /**
   * Ticks for this clock. This includes only ticks for this clock's ID
   */
  readonly ticks: number;
  /**
   * Sets the clock to a causally contiguous time.
   * @param ticks ticks to set. If omitted, `this.ticks + 1`.
   * @returns a copy of the clock with the new time
   */
  ticked(ticks?: number): this;
  /**
   * Are any of this clock's ticks less than the other clock's ticks?
   * @param other the clock to compare
   * @param includeIds whether to include this and the other's process
   * identities
   */
  anyLt(other: this, includeIds?: 'includeIds'): boolean;
  /**
   * @return A hash of the time, usable as a stable operation or message
   * identifier
   */
  hash(): string;
}

export class TreeClockFork {
  constructor(
    readonly left: TreeClock,
    readonly right: TreeClock) {
    if (!left || !right)
      throw new Error('Fork cannot have missing tines');
  }

  get allTicks(): number {
    return this.left.allTicks + this.right.allTicks;
  }

  get nonIdTicks(): number | null {
    const left = this.left.nonIdTicks, right = this.right.nonIdTicks;
    // Since a fork is not an event, zero ticks don't count
    return left || right ? (left ?? 0) + (right ?? 0) : null;
  }

  getTicks(forId: TreeClockFork): number | null {
    const left = this.left.getTicks(forId.left), right = this.right.getTicks(forId.right);
    return left != null || right != null ? (left ?? 0) + (right ?? 0) : null;
  }

  get hasId() {
    return this.left.hasId || this.right.hasId;
  }

  scrubId(): TreeClockFork {
    const left = this.left.scrubId(), right = this.right.scrubId();
    return this.left === left && this.right === right ? this : new TreeClockFork(left, right);
  }

  equals(that: TreeClockFork): boolean {
    return this.left.equals(that.left) && this.right.equals(that.right);
  }

  toString(): string {
    return `{ ${this.left}, ${this.right} }`;
  }

  toJson(forHash?: 'forHash'): [TreeClockJson, TreeClockJson] {
    return [this.left.toJson(forHash), this.right.toJson(forHash)];
  }

  static fromJson(json: [TreeClockJson, TreeClockJson]): TreeClockFork | null {
    if (json == null) {
      return null;
    } else {
      const [left, right] = json.map(TreeClock.fromJson);
      return left && right ? new TreeClockFork(left, right) : null;
    }
  }
}

export class TreeClock implements CausalClock {
  private constructor(
    private readonly _part: TreeClockFork | boolean,
    private readonly _ticks: number = 0) {
    if (_ticks < 0)
      throw new Error('Tree clock must have positive ticks');
  }

  get isId() {
    return this._part === true;
  }

  private get fork() {
    return this._part === true || this._part === false ? null : this._part;
  }

  static GENESIS = new TreeClock(true);
  // Hallows is private because it violates the contract that a TreeClock must
  // have an identity (somewhere in it).
  private static HALLOWS = new TreeClock(false);
  private static HALLOWS_FORK = new TreeClockFork(TreeClock.HALLOWS, TreeClock.HALLOWS);

  /**
   * Formal mapping from a clock time to a transaction ID. Used in the creation
   * of reified operation deletes and inserts. Injective but not safely one-way
   * (do not use as a cryptographic hash).
   * @param time the clock time
   */
  hash() {
    const buf = MsgPack.encode(this.toJson('forHash'));
    // If shorter than sha1 (20 bytes), do not hash
    return buf.length > 20 ? sha1Digest(buf) : buf.toString('base64');
  }

  /**
   * @returns the ticks for this clock. This includes only ticks for this clock's ID
   * @see getTicks(forId)
   */
  get ticks(): number {
    // ticks for this clock's embedded ID (should never return null)
    return this.getTicks(this) as number;
  }

  /**
   * Get all ticks irrespective of ID
   */
  get allTicks(): number {
    return this._ticks + (this.fork != null ? this.fork.allTicks : 0);
  }

  /**
   * Get the ticks for the union of all other process identities (like an inverse)
   */
  get nonIdTicks(): number | null {
    if (!this.isId && this.fork == null) {
      return this._ticks;
    } else if (this.fork) {
      // Post-order traversal to discover if there are any non-IDs in the fork
      const forkResult = this.fork.nonIdTicks;
      // Include our ticks if some non-ID ticks found in the fork
      if (forkResult != null)
        return this._ticks + forkResult;
    }
    return null; // We're all ID
  }

  /**
   * Get the ticks for a different process ID in the same process group
   * @param forId another clock to be used as the ID
   */
  getTicks(forId: TreeClock): number {
    return this._getTicks(forId) as number;
  }

  /**
   * Private variant returns undefined for a tree with no identity in it,
   * which never arises from the API
   */
  private _getTicks(forId: TreeClock): number | undefined {
    if (forId.isId) {
      // Want ID ticks and this is an ID
      return this.allTicks;
    } else if (this.fork || forId.fork) {
      // The ID tree has a fork
      // Post-order traversal to discover if there are any IDs in the fork
      // If we or ID tree don't have a matching fork, substitute hallows (no IDs)
      const forkResult = (this.fork ?? TreeClock.HALLOWS_FORK)
        .getTicks(forId.fork ?? TreeClock.HALLOWS_FORK);
      // Include our ticks if some matching ticks found in the fork
      if (forkResult != null)
        return this._ticks + forkResult;
    }
  }

  ticked(ticks?: number): this {
    // Note this class cannot be extended
    return this._ticked(ticks) as this;
  }

  /**
   * Private variant returns undefined for a tree with no identity in it,
   * which never arises from the API
   */
  private _ticked(ticks: number | null = null): TreeClock | undefined {
    if (ticks != null && ticks < 0) {
      throw new Error('Trying to set ticks < 0');
    } else if (this.isId) {
      return new TreeClock(true, ticks ?? this._ticks + 1);
    } else if (ticks != null && ticks < this._ticks) {
      // If ticks < this._ticks, we drop any fork
      return new TreeClock(this.hasId, ticks);
    } else if (this.fork) {
      const forkTicks = ticks == null ? null : ticks - this._ticks;
      const leftResult = this.fork.left._ticked(forkTicks);
      if (leftResult)
        return new TreeClock(new TreeClockFork(leftResult, this.fork.right), this._ticks);

      const rightResult = this.fork.right._ticked(forkTicks);
      if (rightResult)
        return new TreeClock(new TreeClockFork(this.fork.left, rightResult), this._ticks);
    }
  }

  forked(): TreeClockFork {
    return this._forked() as TreeClockFork;
  }

  /**
   * Private variant returns undefined for a tree with no identity in it,
   * which never arises from the API
   */
  private _forked(): TreeClockFork | undefined {
    if (this.isId) {
      return new TreeClockFork(
        new TreeClock(new TreeClockFork(
          TreeClock.GENESIS, TreeClock.HALLOWS), this._ticks),
        new TreeClock(new TreeClockFork(
          TreeClock.HALLOWS, TreeClock.GENESIS), this._ticks)
      );
    } else if (this.fork) {
      const leftResult = this.fork.left._forked();
      if (leftResult)
        return new TreeClockFork(
          new TreeClock(new TreeClockFork(leftResult.left, this.fork.right), this._ticks),
          new TreeClock(new TreeClockFork(leftResult.right, this.fork.right), this._ticks)
        );

      const rightResult = this.fork.right._forked();
      if (rightResult)
        return new TreeClockFork(
          new TreeClock(new TreeClockFork(this.fork.left, rightResult.left), this._ticks),
          new TreeClock(new TreeClockFork(this.fork.left, rightResult.right), this._ticks)
        );
    }
  }

  update(other: TreeClock): TreeClock {
    if (this.isId) {
      if (other.fork != null)
        throw new Error("Trying to update from overlapping forked clock");
      else if (other._ticks > this._ticks)
        return new TreeClock(true, other._ticks);
      else
        return this; // Typical case: we have the most ticks for our own ID
    } else {
      return new TreeClock(
        other.fork === null ? this._part : new TreeClockFork(
          (this.fork === null ? TreeClock.HALLOWS : this.fork.left).update(other.fork.left),
          (this.fork === null ? TreeClock.HALLOWS : this.fork.right).update(other.fork.right)),
        Math.max(this._ticks, other._ticks));
    }
  }

  get hasId(): boolean {
    return this.isId || this.fork?.hasId || false;
  }

  scrubId(): TreeClock {
    if (this.isId) {
      return new TreeClock(false, this._ticks);
    } else if (this.fork != null) {
      const fork = this.fork.scrubId();
      if (fork !== this.fork)
        return new TreeClock(fork, this._ticks);
    }
    return this;
  }

  anyLt(other: TreeClock, includeIds?: 'includeIds'): boolean {
    if (this.fork == null || other.fork == null) {
      if (includeIds) {
        return this.allTicks < other.allTicks;
      } else {
        const thisTicks = this.nonIdTicks, otherTicks = other.nonIdTicks;
        if (thisTicks != null && otherTicks != null) {
          return thisTicks < otherTicks;
        } else {
          return false; // Either is an ID but we don't want IDs, or both not IDs and we want IDs
        }
      }
    } else {
      return this.fork.left.anyLt(other.fork.left, includeIds) ||
        this.fork.right.anyLt(other.fork.right, includeIds);
    }
  }

  equals(that: this): boolean {
    return this.isId === that.isId &&
      this._ticks === that._ticks &&
      (this.fork === that.fork ||
        (this.fork !== null && that.fork !== null && this.fork.equals(that.fork)));
  }

  toString(): string {
    return JSON.stringify(this.toJson());
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();

  toJson(forHash?: 'forHash'): TreeClockJson {
    if (this.isId) {// 0 or 1 element array for ID
      return this._ticks > 0 ? [this._ticks] : [];
    } else if (this.fork == null) { // Plain number for non-ID
      return this._ticks;
    } else {
      const forkJson = this.fork.toJson(forHash);
      if (forHash) {
        const zero = forkJson.map(isZeroId);
        if (!zero.includes(null))
          if (zero.includes(true))
            return this._ticks > 0 ? [this._ticks] : [];
          else
            return this._ticks;
      }
      if (this._ticks == 0) // 2 element array for ticks and fork
        return forkJson;
      else // 3 element array for ticks and fork
        return [this._ticks, ...forkJson];
    }
  }

  static fromJson(json: TreeClockJson): TreeClock | null {
    if (json == null) {
      return null;
    } else if (typeof json == 'number') { // Plain number for non-ID
      return new TreeClock(false, json);
    } else if (Array.isArray(json)) { // Compact tuple format
      if (json.length == 0) { // ID with zero ticks
        return TreeClock.GENESIS;
      } else if (json.length == 1) { // ID with given ticks
        return new TreeClock(true, json[0]);
      } else if (json.length == 2) { // Zero-tick with fork
        return new TreeClock(TreeClockFork.fromJson(json) ?? false, 0);
      } else if (json.length == 3) { // Given ticks and fork
        const [ticks, left, right] = json;
        return new TreeClock(TreeClockFork.fromJson([left, right]) ?? false, ticks);
      }
    }
    return null;
  }
}

function isZeroId(json: TreeClockJson) {
  if (typeof json == 'number')
    return json === 0 ? false : null;
  else if (json.length === 0)
    return true;
  else
    return null;
}

export type TreeClockJson =
  number |
  [] |
  [number] |
  [TreeClockJson, TreeClockJson] |
  [number, TreeClockJson, TreeClockJson];