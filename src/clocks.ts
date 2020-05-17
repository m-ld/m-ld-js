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

  getTicks(forId: TreeClockFork | false | null): number | null {
    const leftResult = this.left.getTicks(forId ? forId.left : forId),
      rightResult = this.right.getTicks(forId ? forId.right : forId);
    return leftResult != null || rightResult != null ?
      zeroIfNull(leftResult) + zeroIfNull(rightResult) : null;
  }

  equals(that: TreeClockFork): boolean {
    return this.left.equals(that.left) && this.right.equals(that.right);
  }

  toString(): string {
    return `{ ${this.left}, ${this.right} }`;
  }

  toJson(): any {
    return { left: this.left.toJson(), right: this.right.toJson() };
  }

  static fromJson(json: any): TreeClockFork | null {
    if (json) {
      const left = TreeClock.fromJson(json.left);
      const right = TreeClock.fromJson(json.right);
      return left && right ? new TreeClockFork(left, right) : null;
    }
    return null;
  }
}

export function zeroIfNull(value: number | null) {
  return value == null ? 0 : value;
}

export class TreeClock implements CausalClock<TreeClock> {
  constructor(
    readonly isId: boolean,
    private readonly ticks: number,
    private readonly fork: TreeClockFork | null) {
  }

  static GENESIS = new TreeClock(true, 0, null);
  static HALLOWS = new TreeClock(false, 0, null);
  private static HALLOWS_FORK = new TreeClockFork(TreeClock.HALLOWS, TreeClock.HALLOWS);

  /**
   * @returns the ticks for this clock. This includes only ticks for this clock's ID
   * @see getTicks(forId)
   */
  getTicks(): number;
  /**
   * Argument variants to:
   * - get the ticks for a different process ID in the same process group
   * - get the ticks for the union of all other process identities (like an inverse)
   * - get all ticks irrespective of ID
   * @param forId another clock to be used as the ID, or
   * `false` to invert the ID of the traversal, or
   * `null` to gather all ticks
   */
  getTicks(forId: TreeClock | false | null): number | null;
  getTicks(forId?: TreeClock | false | null): number | null {
    if (arguments.length == 0) {
      // No-args means ticks for this clock's embedded ID (should never return null)
      return zeroIfNull(this.getTicks(this));
    } else if (forId == null || (!forId && !this.isId) || (forId && forId.isId)) {
      // If (want all ticks) || (want non-ID ticks) || (want ID ticks and this is an ID)
      // Then always include the current ticks
      return this.ticks + (this.fork == null ? 0 :
        // and include all fork ticks unless we want non-ID ticks (both tines can't be ID)
        zeroIfNull(this.fork.getTicks(forId == null || forId ? null : false)));
    } else if (this.fork || (forId && forId.fork)) {
      // If (we have a fork) || (the ID tree has a fork)
      // Post-order traversal to discover if there are any IDs in the fork
      // If we or ID tree don't have a matching fork, substitute hallows (no IDs)
      const forkResult = (this.fork ?? TreeClock.HALLOWS_FORK)
        .getTicks(forId ? forId.fork ?? TreeClock.HALLOWS_FORK : forId);
      // Include our ticks if some matching ticks found in the fork
      if (forkResult != null)
        return this.ticks + forkResult;
    }
    return null;
  }

  ticked(): TreeClock {
    return this._ticked() as TreeClock;
  }

  /**
   * Private variant returns undefined for a tree with no identity in it,
   * which never arises from the API
   */
  private _ticked(): TreeClock | undefined {
    if (this.isId) {
      return new TreeClock(true, this.ticks + 1, this.fork);
    } else if (this.fork) {
      const leftResult = this.fork.left._ticked();
      if (leftResult)
        return new TreeClock(false, this.ticks, new TreeClockFork(leftResult, this.fork.right));

      const rightResult = this.fork.right._ticked();
      if (rightResult)
        return new TreeClock(false, this.ticks, new TreeClockFork(this.fork.left, rightResult));
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
        new TreeClock(false, this.ticks, new TreeClockFork(
          new TreeClock(true, 0, this.fork), new TreeClock(false, 0, this.fork))),
        new TreeClock(false, this.ticks, new TreeClockFork(
          new TreeClock(false, 0, this.fork), new TreeClock(true, 0, this.fork)))
      );
    } else if (this.fork) {
      const leftResult = this.fork.left.forked();
      if (leftResult)
        return new TreeClockFork(
          new TreeClock(false, this.ticks, new TreeClockFork(leftResult.left, this.fork.right)),
          new TreeClock(false, this.ticks, new TreeClockFork(leftResult.right, this.fork.right))
        );

      const rightResult = this.fork.right.forked();
      if (rightResult)
        return new TreeClockFork(
          new TreeClock(false, this.ticks, new TreeClockFork(this.fork.left, rightResult.left)),
          new TreeClock(false, this.ticks, new TreeClockFork(this.fork.left, rightResult.right))
        );
    }
  }

  update(other: TreeClock): TreeClock {
    if (this.isId) {
      if (other.isId && other.ticks > this.ticks)
        throw new Error("Trying to update from overlapping clock");
      return this;
    } else {
      return new TreeClock(
        false, Math.max(this.ticks, other.ticks),
        other.fork === null ? this.fork : new TreeClockFork(
          (this.fork === null ? TreeClock.HALLOWS : this.fork.left).update(other.fork.left),
          (this.fork === null ? TreeClock.HALLOWS : this.fork.right).update(other.fork.right)));
    }
  }

  mergeId(other: TreeClock): TreeClock {
    if (this.fork !== null && other.fork !== null) {
      const left = this.fork.left.mergeId(other.fork.left),
        right = this.fork.right.mergeId(other.fork.right);
      if (left.isId && right.isId) {
        return new TreeClock(true, this.ticks + left.getTicks() + right.getTicks(), null);
      } else {
        return new TreeClock(this.isId || other.isId, this.ticks, new TreeClockFork(left, right));
      }
    }
    else if (this.fork !== null) {
      return new TreeClock(this.isId || other.isId, this.ticks, this.fork);
    }
    else {
      return new TreeClock(this.isId || other.isId, this.ticks, other.fork == null ? null :
        new TreeClockFork(TreeClock.HALLOWS.mergeId(other.fork.left),
          TreeClock.HALLOWS.mergeId(other.fork.right)));
    }
  }

  anyLt(other: TreeClock, includeIds?: 'includeIds'): boolean {
    if (this.fork == null || other.fork == null) {
      if (includeIds || (!this.isId && !other.isId)) {
        return zeroIfNull(this.getTicks(includeIds ? null : false)) <
          zeroIfNull(other.getTicks(includeIds ? null : false));
      } else {
        return false; // Either is an ID but we don't want IDs, or both not IDs and we want IDs
      }
    } else {
      return this.fork.left.anyLt(other.fork.left, includeIds) ||
        this.fork.right.anyLt(other.fork.right, includeIds);
    }
  }

  equals(that: TreeClock): boolean {
    return this.isId === that.isId &&
      this.ticks === that.ticks &&
      (this.fork === that.fork ||
        (this.fork !== null && that.fork !== null && this.fork.equals(that.fork)));
  }

  toString(): string {
    const content = [
      this.isId ? 'ID' : null,
      this.ticks > 0 ? this.ticks : null,
      this.fork
    ].filter(p => p);
    return (content.length == 1 ? content[0] || '' : content).toString();
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();

  toJson(): any {
    return {
      isId: this.isId,
      ticks: this.ticks,
      fork: this.fork ? this.fork.toJson() : null
    };
  }

  static fromJson(json: any): TreeClock | null {
    return json ? new TreeClock(json.isId, json.ticks, TreeClockFork.fromJson(json.fork)) : null;
  }
}