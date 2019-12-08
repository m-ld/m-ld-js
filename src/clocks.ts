export class TreeClockFork {
  constructor(
    readonly left: TreeClock,
    readonly right: TreeClock) {
    if (!left || !right)
      throw new Error('Fork cannot have missing tines');
  }

  equals(that: TreeClockFork): boolean {
    return this.left.equals(that.left) && this.right.equals(that.right);
  }

  toString(): string {
    return `{ ${this.left}, ${this.right} }`;
  }
}

export function zeroIfNull(value: number) {
  return value == null ? 0 : value;
}

export class TreeClock {
  constructor(
    private readonly isId: boolean,
    private readonly ticks: number,
    private readonly fork: TreeClockFork) {
  }

  static GENESIS = new TreeClock(true, 0, null);
  static HALLOWS = new TreeClock(false, 0, null);

  getTicks(forId?: boolean): number {
    if (forId === undefined) {
      return zeroIfNull(this.getTicks(true));
    } else if (forId === null || forId === this.isId) {
      return this.ticks + (this.fork === null ? 0 :
        zeroIfNull(this.fork.left.getTicks(forId == null || forId ? null : false)) +
        zeroIfNull(this.fork.right.getTicks(forId == null || forId ? null : false)));
    } else if (this.fork !== null) {
      const leftResult = this.fork.left.getTicks(forId), rightResult = this.fork.right.getTicks(forId);
      if (leftResult !== null || rightResult !== null)
        return this.ticks + zeroIfNull(leftResult) + zeroIfNull(rightResult);
    }
    return null;
  }

  ticked(): TreeClock {
    if (this.isId) {
      return new TreeClock(true, this.ticks + 1, this.fork);
    } else if (this.fork !== null) {
      const leftResult = this.fork.left.ticked();
      if (leftResult)
        return new TreeClock(false, this.ticks, new TreeClockFork(leftResult, this.fork.right));

      const rightResult = this.fork.right.ticked();
      if (rightResult)
        return new TreeClock(false, this.ticks, new TreeClockFork(this.fork.left, rightResult));
    }
  }

  forked(): TreeClockFork {
    if (this.isId) {
      return new TreeClockFork(
        new TreeClock(false, this.ticks, new TreeClockFork(
          new TreeClock(true, 0, this.fork),
          new TreeClock(false, 0, this.fork))),
        new TreeClock(false, this.ticks, new TreeClockFork(
          new TreeClock(false, 0, this.fork),
          new TreeClock(true, 0, this.fork)))
      );
    } else if (this.fork != null) {
      const leftResult = this.fork.left.forked();
      if (leftResult != null)
        return new TreeClockFork(
          new TreeClock(false, this.ticks, new TreeClockFork(leftResult.left, this.fork.right)),
          new TreeClock(false, this.ticks, new TreeClockFork(leftResult.right, this.fork.right))
        );

      const rightResult = this.fork.right.forked();
      if (rightResult != null)
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
    if (this.fork != null && other.fork != null) {
      const left = this.fork.left.mergeId(other.fork.left),
        right = this.fork.right.mergeId(other.fork.right);
      if (left.isId && right.isId) {
        return new TreeClock(true, this.ticks + left.getTicks() + right.getTicks(), null);
      } else {
        return new TreeClock(this.isId || other.isId, this.ticks, new TreeClockFork(left, right));
      }
    }
    else if (this.fork != null) {
      return new TreeClock(this.isId || other.isId, this.ticks, this.fork);
    }
    else {
      return new TreeClock(this.isId || other.isId, this.ticks, other.fork == null ? null :
        new TreeClockFork(TreeClock.HALLOWS.mergeId(other.fork.left),
          TreeClock.HALLOWS.mergeId(other.fork.right)));
    }
  }

  anyLt(other: TreeClock): boolean {
    if (this.fork == null || other.fork == null) {
      if (!this.isId && !other.isId) {
        return this.getTicks(false) < other.getTicks(false);
      } else {
        return false; // Either is an ID but we don't want IDs, or both not IDs and we want IDs
      }
    } else {
      return this.fork.left.anyLt(other.fork.left) || this.fork.right.anyLt(other.fork.right);
    }
  }

  equals(that: TreeClock): boolean {
    return this.isId === that.isId &&
      this.ticks === that.ticks &&
      (this.fork === that.fork || this.fork.equals(that.fork));
  }

  toString(): string {
    const content = [
      this.isId ? 'ID' : null,
      this.ticks > 0 ? this.ticks : null,
      this.fork
    ].filter(p => p);
    return (content.length == 1 ? content[0] : content).toString();
  }
}