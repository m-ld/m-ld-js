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
   * identities
   */
  anyNonIdLt(other: this): boolean;
  /**
   * @return A hash of the time, usable as a stable operation or message
   * identifier
   */
  hash(): string;
}

export class Fork<T> {
  constructor(
    readonly left: T,
    readonly right: T) {
  }
}

export abstract class TickTree<T = unknown> {
  private readonly _part: Fork<this> | T;

  protected constructor(
    part: Fork<TickTree<T>> | T,
    readonly localTicks: number = 0) {
    if (localTicks < 0)
      throw new Error('Tree clock must have positive ticks');
    this._part = part as Fork<this> | T;
  }

  get value() {
    return this._part instanceof Fork ? null : this._part;
  }

  get fork() {
    return this._part instanceof Fork ? this._part as Fork<this> : null;
  }

  /** @returns a valid default branch even if this is not a fork */
  get leftBud(): this {
    return (this.fork?.left ?? this.bud());
  }

  /** @returns a valid default branch even if this is not a fork */
  get rightBud(): this {
    return (this.fork?.right ?? this.bud());
  }

  protected abstract bud(value?: T): this;

  /**
   * @returns the sum of all ticks in the tree
   */
  get deepTicks(): number {
    return this.localTicks + (this.fork != null ?
      this.fork.left.deepTicks + this.fork.right.deepTicks : 0);
  }

  /**
   * Get the ticks for a different process ID in the same process group
   * @param filter another clock to be used as the ID
   */
  getTicks(filter: TickTree<boolean>): number {
    const ticks = this._getTicks(filter);
    if (ticks == null)
      throw new Error('Trying to get ticks from a clock with no ID');
    return ticks;
  }

  /**
   * Private variant returns undefined for a tree with no identity in it,
   * which never arises from the API
   */
  private _getTicks(filter: TickTree<boolean>): number | undefined {
    if (filter.value) {
      // Want ID ticks and this is an ID
      return this.deepTicks;
    } else if (this.fork || filter.fork) {
      // The ID tree has a fork
      // Post-order traversal to discover if there are any IDs in the fork
      // If we or ID tree don't have a matching fork, substitute hallows (no IDs)
      const left = this.leftBud._getTicks(filter.leftBud),
        right = this.rightBud._getTicks(filter.rightBud);
      // Include our ticks if some matching ticks found in the fork
      if (left != null || right != null)
        return this.localTicks + (left ?? 0) + (right ?? 0);
    }
  }

  anyLt(other: TickTree): boolean {
    if (this.fork == null || other.fork == null) {
      return this.deepTicks < other.deepTicks;
    } else {
      return this.fork.left.anyLt(other.fork.left) ||
        this.fork.right.anyLt(other.fork.right);
    }
  }

  equals(that: this): boolean {
    return this.value === that.value &&
      this.localTicks === that.localTicks &&
      (this.fork === that.fork ||
        (this.fork !== null && that.fork !== null &&
          this.fork.left.equals(that.fork.left) &&
          this.fork.right.equals(that.fork.right)));
  }

  protected updateFromOther<O extends TickTree<unknown>>(other: O,
    recurse: (tree: this, that: O) => this): [part: Fork<this> | T, localTicks: number] {
    return [
      other.fork === null ? this._part : new Fork(
        recurse(this.leftBud, other.fork.left),
        recurse(this.rightBud, other.fork.right)),
      Math.max(this.localTicks, other.localTicks)];
  }

  toString(): string {
    return JSON.stringify(this.toJSON());
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();

  abstract toJSON(): any;
}

/**
 * A tree clock is a causal clock that carries the identity of a process. Only
 * one leaf node is marked as the identity at any time.
 */
export class TreeClock extends TickTree<boolean> implements CausalClock {
  get isId() {
    return this.value === true;
  }

  protected bud(isId?: boolean) {
    // Default is hallows; TreeClock is final
    return (isId ? TreeClock.GENESIS : TreeClock.HALLOWS) as this;
  }

  static GENESIS = new TreeClock(true);
  // Hallows is private because it violates the contract that a TreeClock must
  // have an identity (somewhere in it).
  private static HALLOWS = new TreeClock(false);

  /**
   * Formal mapping from a clock time to a transaction ID. Used in the creation
   * of reified operation deletes and inserts. Injective but not safely one-way
   * (do not use as a cryptographic hash).
   */
  hash() {
    const buf = MsgPack.encode(this.toJSON('forHash'));
    // If shorter than sha1 (20 bytes), do not hash
    return buf.length > 20 ? sha1Digest(buf) : buf.toString('base64');
  }

  /**
   * @returns the ticks for this clock. This includes only ticks for this clock's ID
   * @see getTicks(forId)
   */
  get ticks(): number {
    // ticks for this clock's embedded ID (should never return null)
    return this.getTicks(this);
  }

  /**
   * Get the ticks for the union of all other process identities (like an inverse)
   */
  get nonIdTicks(): number | null {
    if (!this.isId && this.fork == null) {
      return this.localTicks;
    } else if (this.fork) {
      // Post-order traversal to discover if there are any non-IDs in the fork
      const left = this.fork.left.nonIdTicks, right = this.fork.right.nonIdTicks;
      // Since a fork is not an event, zero ticks don't count
      if (left || right)
        // Include our ticks if some non-ID ticks found in the fork
        return this.localTicks + (left ?? 0) + (right ?? 0);
    }
    return null; // We're all ID
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
      if (ticks === this.localTicks)
        return this;
      else
        return new TreeClock(true, ticks ?? this.localTicks + 1);
    } else if (this.fork) {
      if (ticks != null && ticks < this.localTicks) {
        // If ticks < localTicks and we have a buried ID, we can drop the fork
        if (this.hasId)
          return new TreeClock(true, ticks);
      } else {
        const forkTicks = ticks == null ? null : ticks - this.localTicks;
        const leftResult = this.fork.left._ticked(forkTicks);
        if (leftResult)
          return leftResult === this.fork.left ? this :
            new TreeClock(new Fork(leftResult, this.fork.right), this.localTicks);

        const rightResult = this.fork.right._ticked(forkTicks);
        if (rightResult)
          return rightResult === this.fork.right ? this :
            new TreeClock(new Fork(this.fork.left, rightResult), this.localTicks);
      }
    }
  }

  forked(): Fork<TreeClock> {
    return this._forked() as Fork<TreeClock>;
  }

  /**
   * Private variant returns undefined for a tree with no identity in it,
   * which never arises from the API
   */
  private _forked(): Fork<TreeClock> | undefined {
    if (this.isId) {
      return new Fork(
        new TreeClock(new Fork(
          TreeClock.GENESIS, TreeClock.HALLOWS), this.localTicks),
        new TreeClock(new Fork(
          TreeClock.HALLOWS, TreeClock.GENESIS), this.localTicks)
      );
    } else if (this.fork) {
      const leftResult = this.fork.left._forked();
      if (leftResult)
        return new Fork(
          new TreeClock(new Fork(leftResult.left, this.fork.right), this.localTicks),
          new TreeClock(new Fork(leftResult.right, this.fork.right), this.localTicks)
        );

      const rightResult = this.fork.right._forked();
      if (rightResult)
        return new Fork(
          new TreeClock(new Fork(this.fork.left, rightResult.left), this.localTicks),
          new TreeClock(new Fork(this.fork.left, rightResult.right), this.localTicks)
        );
    }
  }

  update(other: TickTree): TreeClock {
    if (this.isId) {
      if (other.fork != null)
        throw new Error('Trying to update from overlapping forked clock');
      else if (other.localTicks > this.localTicks)
        return new TreeClock(true, other.localTicks);
      else
        return this; // Typical case: we have the most ticks for our own ID
    } else {
      return new TreeClock(...this.updateFromOther(other,
        (tree, time) => tree.update(time) as this));
    }
  }

  get hasId(): boolean {
    return this.isId || (this.fork != null && (this.fork.left.hasId || this.fork.right.hasId));
  }

  anyNonIdLt(other: TreeClock): boolean {
    if (this.fork == null || other.fork == null) {
      const thisTicks = this.nonIdTicks, otherTicks = other.nonIdTicks;
      if (thisTicks != null && otherTicks != null) {
        return thisTicks < otherTicks;
      } else {
        return false; // Either is an ID but we don't want IDs, or both not IDs and we want IDs
      }
    } else {
      return this.fork.left.anyNonIdLt(other.fork.left) ||
        this.fork.right.anyNonIdLt(other.fork.right);
    }
  }

  toJSON(forHash?: 'forHash'): TreeClockJson {
    if (this.isId) {
      return this.leafJson(true);
    } else if (this.fork == null) {
      return this.leafJson(false);
    } else { // We're a fork
      const forkJson: TreeClockJson =
        [this.fork.left.toJSON(forHash), this.fork.right.toJSON(forHash)];
      if (forHash) {
        const zero = forkJson.map(isZeroId);
        if (!zero.includes(null))
          return this.leafJson(zero.includes(true));
      }
      if (this.localTicks == 0) // 2 element array for ticks and fork
        return forkJson;
      else // 3 element array for ticks and fork
        return [this.localTicks, ...forkJson];
    }
  }

  private leafJson(isId: boolean): TreeClockJson {
    if (isId) // 0 or 1 element array for ID
      return this.localTicks > 0 ? [this.localTicks] : [];
    else // Plain number for non-ID
      return this.localTicks;
  }

  static fromJson(json: TreeClockJson): TreeClock {
    if (typeof json == 'number') { // Plain number for non-ID
      return new TreeClock(false, json);
    } else if (Array.isArray(json) && json.length <= 3) {
      if (json.length == 0) { // ID with zero ticks
        return TreeClock.GENESIS;
      } else if (json.length == 1) { // ID with given ticks
        return new TreeClock(true, json[0]);
      } else if (json.length == 2) { // Zero-tick with fork
        return new TreeClock(TreeClock.forkFromJson(json), 0);
      } else if (json.length == 3) { // Given ticks and fork
        const [ticks, left, right] = json;
        return new TreeClock(TreeClock.forkFromJson([left, right]), ticks);
      }
    }
    throw new Error('Bad clock JSON');
  }

  private static forkFromJson(json: [TreeClockJson, TreeClockJson]): Fork<TreeClock> {
    const [left, right] = json.map(TreeClock.fromJson);
    return new Fork(left, right);
  }
}

/**
 * @returns - `true` if 0 ticks and is ID
 * - `false` if 0 ticks and not ID
 * - `null` if non-zero ticks
 */
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

/**
 * Public clock time, carries latest public ticks seen for all processes (never
 * internal ticks). It also carries the latest seen time hash (transaction ID)
 * for every process ID (leaf).
 */
export class GlobalClock extends TickTree<string> {
  static GENESIS = new GlobalClock('').update(TreeClock.GENESIS);

  protected bud(tid?: string): this {
    return new GlobalClock(tid ?? this.value ?? '', 0) as this;
  }

  update(time: TreeClock): GlobalClock {
    return this._update(time, time.hash());
  }

  private _update(time: TreeClock, tid: string): GlobalClock {
    if (time.isId) {
      if (this.fork != null)
        throw new Error('Global clock is in the future');
      return new GlobalClock(tid, Math.max(this.localTicks, time.ticks));
    } else {
      return new GlobalClock(...this.updateFromOther(time,
        (tree, time) => tree._update(time, tid) as this));
    }
  }

  tid(time: TreeClock): string {
    const tid = this._tid(time);
    if (tid == null)
      throw new Error('Global clock is in the future');
    return tid;
  }

  private _tid(time: TreeClock): string | null {
    if (time.isId) {
      return this.value;
    } else if (this.fork != null || time.fork != null) {
      return this.leftBud._tid(time.leftBud) ??
        this.rightBud._tid(time.rightBud);
    }
    return null;
  }

  *tids(): IterableIterator<string> {
    if (this.value) { // Note '' is not yielded
      yield this.value;
    } else if (this.fork != null) {
      yield *this.fork.left.tids();
      yield *this.fork.right.tids();
    }
  }

  toJSON(): GlobalClockJson {
    if (this.value != null)
      return [this.localTicks, this.value];
    else if (this.fork != null)
      return [this.localTicks, [this.fork.left.toJSON(), this.fork.right.toJSON()]];
    else
      throw new Error();
  }

  static fromJSON(json: GlobalClockJson): GlobalClock {
    const [ticks, parts] = json;
    if (typeof parts == 'string') {
      return new GlobalClock(parts, ticks);
    } else if (Array.isArray(json) && json.length == 2) {
      const [left, right] = parts;
      return new GlobalClock(new Fork(
        GlobalClock.fromJSON(left),
        GlobalClock.fromJSON(right)), ticks);
    }
    throw new Error('Bad global clock JSON');
  }
}

export type GlobalClockJson = [number, string | [GlobalClockJson, GlobalClockJson]];