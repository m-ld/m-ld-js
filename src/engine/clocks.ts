const inspect = Symbol.for('nodejs.util.inspect.custom');

export interface CausalClock<T> {
  anyLt(other: T, includeIds?: 'includeIds'): boolean;
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

  equals(that: TreeClockFork): boolean {
    return this.left.equals(that.left) && this.right.equals(that.right);
  }

  toString(): string {
    return `{ ${this.left}, ${this.right} }`;
  }

  toJson(): [TreeClockJson, TreeClockJson] {
    return [this.left.toJson(), this.right.toJson()];
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

export class TreeClock implements CausalClock<TreeClock> {
  private constructor(
    readonly isId: boolean,
    private readonly _ticks: number = 0,
    private readonly _fork: TreeClockFork | null = null) {
    if (_ticks < 0)
      throw new Error('Tree clock must have positive ticks');
    // TODO: isId is redundant with fork - refactor?
    if (isId && _fork != null)
      throw new Error('Tree clock ID must be a leaf');
  }

  static GENESIS = new TreeClock(true);
  // Hallows is private because it violates the contract that a TreeClock must
  // have an identity (somewhere in it).
  private static HALLOWS = new TreeClock(false);
  private static HALLOWS_FORK = new TreeClockFork(TreeClock.HALLOWS, TreeClock.HALLOWS);

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
    return this._ticks + (this._fork != null ? this._fork.allTicks : 0);
  }

  /**
   * Get the ticks for the union of all other process identities (like an inverse)
   */
  get nonIdTicks(): number | null {
    if (!this.isId && this._fork == null) {
      return this._ticks;
    } else if (this._fork) {
      // Post-order traversal to discover if there are any non-IDs in the fork
      const forkResult = this._fork.nonIdTicks;
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
    } else if (this._fork || forId._fork) {
      // The ID tree has a fork
      // Post-order traversal to discover if there are any IDs in the fork
      // If we or ID tree don't have a matching fork, substitute hallows (no IDs)
      const forkResult = (this._fork ?? TreeClock.HALLOWS_FORK)
        .getTicks(forId._fork ?? TreeClock.HALLOWS_FORK);
      // Include our ticks if some matching ticks found in the fork
      if (forkResult != null)
        return this._ticks + forkResult;
    }
  }

  ticked(ticks?: number): TreeClock {
    return this._ticked(ticks) as TreeClock;
  }

  /**
   * Private variant returns undefined for a tree with no identity in it,
   * which never arises from the API
   */
  private _ticked(ticks: number | null = null): TreeClock | undefined {
    if (ticks != null && ticks < 0) {
      throw new Error('Trying to set ticks < 0');
    } else if (this.isId) {
      return new TreeClock(true, ticks ?? this._ticks + 1, this._fork);
    } else if (this._fork) {
      const leftResult = this._fork.left._ticked(ticks == null ? null : ticks - this._ticks);
      if (leftResult)
        return new TreeClock(false, this._ticks, new TreeClockFork(leftResult, this._fork.right));

      const rightResult = this._fork.right._ticked(ticks == null ? null : ticks - this._ticks);
      if (rightResult)
        return new TreeClock(false, this._ticks, new TreeClockFork(this._fork.left, rightResult));
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
        new TreeClock(false, this._ticks, new TreeClockFork(
          new TreeClock(true, 0, this._fork), new TreeClock(false, 0, this._fork))),
        new TreeClock(false, this._ticks, new TreeClockFork(
          new TreeClock(false, 0, this._fork), new TreeClock(true, 0, this._fork)))
      );
    } else if (this._fork) {
      const leftResult = this._fork.left._forked();
      if (leftResult)
        return new TreeClockFork(
          new TreeClock(false, this._ticks, new TreeClockFork(leftResult.left, this._fork.right)),
          new TreeClock(false, this._ticks, new TreeClockFork(leftResult.right, this._fork.right))
        );

      const rightResult = this._fork.right._forked();
      if (rightResult)
        return new TreeClockFork(
          new TreeClock(false, this._ticks, new TreeClockFork(this._fork.left, rightResult.left)),
          new TreeClock(false, this._ticks, new TreeClockFork(this._fork.left, rightResult.right))
        );
    }
  }

  update(other: TreeClock): TreeClock {
    if (this.isId) {
      if (other._fork != null)
        throw new Error("Trying to update from overlapping forked clock");
      else if (other._ticks > this._ticks)
        return new TreeClock(true, other._ticks);
      else
        return this; // Typical case: we have the most ticks for our own ID
    } else {
      return new TreeClock(
        false, Math.max(this._ticks, other._ticks),
        other._fork === null ? this._fork : new TreeClockFork(
          (this._fork === null ? TreeClock.HALLOWS : this._fork.left).update(other._fork.left),
          (this._fork === null ? TreeClock.HALLOWS : this._fork.right).update(other._fork.right)));
    }
  }

  scrubId(): TreeClock {
    if (this.isId) {
      return new TreeClock(false, this._ticks);
    } else if (this._fork != null) {
      return new TreeClock(false, this._ticks, new TreeClockFork(
        this._fork.left.scrubId(), this._fork.right.scrubId()));
    } else {
      return this;
    }
  }

  anyLt(other: TreeClock, includeIds?: 'includeIds'): boolean {
    if (this._fork == null || other._fork == null) {
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
      return this._fork.left.anyLt(other._fork.left, includeIds) ||
        this._fork.right.anyLt(other._fork.right, includeIds);
    }
  }

  equals(that: TreeClock): boolean {
    return this.isId === that.isId &&
      this._ticks === that._ticks &&
      (this._fork === that._fork ||
        (this._fork !== null && that._fork !== null && this._fork.equals(that._fork)));
  }

  toString(): string {
    return JSON.stringify(this.toJson());
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();

  toJson(): TreeClockJson {
    if (this.isId) // 0 or 1 element array for ID
      return this._ticks > 0 ? [this._ticks] : [];
    else if (this._fork == null) // Plain number for non-ID
      return this._ticks;
    else if (this._ticks == 0) // 2 element array for ticks and fork
      return this._fork.toJson();
    else // 3 element array for ticks and fork
      return [this._ticks, ...this._fork.toJson()];
  }

  static fromJson(json: TreeClockJson): TreeClock | null {
    if (json == null) {
      return null;
    } else if (typeof json == 'number') { // Plain number for non-ID
      return new TreeClock(false, json);
    } else if (Array.isArray(json)) { // Compact tuple format
      if (json.length == 0) { // ID with zero ticks
        return new TreeClock(true, 0);
      } else if (json.length == 1) { // ID with given ticks
        return new TreeClock(true, json[0]);
      } else if (json.length == 2) { // Zero-tick with fork
        return new TreeClock(false, 0, TreeClockFork.fromJson(json));
      } else if (json.length == 3) { // Given ticks and fork
        const [ticks, left, right] = json;
        return new TreeClock(false, ticks, TreeClockFork.fromJson([left, right]));
      }
    }
    return null;
  }
}

export type TreeClockJson =
  number |
  [] |
  [number] |
  [TreeClockJson, TreeClockJson] |
  [number, TreeClockJson, TreeClockJson];