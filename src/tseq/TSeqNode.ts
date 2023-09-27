import { TSeqContainer } from './TSeqContainer';
import { hasInserts, TSeqCharTick, TSeqName } from './types';
import { TSeqPreApply } from './TSeq';
import { TSeqOperable } from './TSeqOperable';
import { compare } from '../engine/util';

/**
 * Node in a {@link TSeq} tree. Only the root node, the TSeq itself, does not
 * hold a character.
 * @internal
 */
export abstract class TSeqNode extends TSeqContainer<TSeqCharNode> {
  /** Arrays per-process, sorted by process ID */
  private readonly rest: TSeqProcessArray[] = [];

  protected constructor(
    /** The location of this node in the TSeq tree */
    readonly path: TSeqName[]
  ) {
    super();
  }

  /**
   * Possibly expensive invariant checking, intended to be called in testing
   * @internal
   */
  checkInvariants() {
    super.checkInvariants();
    const sorted = [...this.rest].sort(TSeqProcessArray.compare);
    if (!this.rest.every((arr, i) => arr === sorted[i]))
      throw 'arrays are not in order';
  }

  /** @inheritDoc */
  children() {
    return this.rest;
  }

  push(pid: string, content: string, tick: number) {
    return this.rest[this.getRestIndex(pid, true)].push(content, tick);
  }

  get hasRest() {
    return !!this.rest.length;
  }

  /** @returns the actually-affected character nodes (ignoring empty -> empty) */
  *preApply(
    [[pid, index], ...tail]: TSeqName[],
    content: (TSeqCharTick | undefined)[],
    charIndex: number,
    force?: boolean
  ): Iterable<TSeqPreApply> {
    // If a pure delete, don't force creation
    const restIndex = this.getRestIndex(pid, hasInserts(content));
    if (restIndex > -1) {
      for (let i = 0; i < restIndex; i++)
        charIndex += this.rest[i].charLength;
      yield *this.rest[restIndex].preApply(index, tail, content, charIndex, force);
    }
  }

  gc(array: TSeqProcessArray) {
    if (array.chars().next().done) {
      const index = this.getRestIndex(array.pid);
      this.rest.splice(index, 1);
    }
  }

  private getRestIndex(pid: string, create = true): number {
    // TODO: This could be a binary search
    for (let i = 0; i < this.rest.length; i++) {
      if (this.rest[i].pid === pid) {
        return i;
      } else if (this.rest[i].pid > pid) {
        if (create) {
          const rest = new TSeqProcessArray(pid, this);
          this.rest.splice(i, 0, rest);
          return i;
        } else {
          return -1;
        }
      }
    }
    if (create)
      return this.rest.push(new TSeqProcessArray(pid, this)) - 1;
    else
      return -1;
  }

  toJSON(): any[] {
    return this.rest.map(array => array.toJSON());
  }

  restFromJSON(json: any[]) {
    this.rest.push(...json
      .map(array => TSeqProcessArray.fromJSON(array, this))
      .sort(TSeqProcessArray.compare));
  }
}

/** @internal */
export class TSeqCharNode extends TSeqNode implements TSeqOperable {
  private _char = '';
  private _tick = 0;

  /** Holds the pre-image of this char node, only used during an operation */
  private _pre?: TSeqCharTick;

  constructor(
    readonly container: TSeqProcessArray,
    /** Index into sparse container array (NOT count of non-empty chars before) */
    readonly index: number
  ) {
    super(container.parentNode.path.concat([[container.pid, index]]));
  }

  get parent() {
    return this.container;
  }

  checkInvariants() {
    super.checkInvariants();
    if (this.container.at(this.index) !== this)
      throw 'incorrect index';
    if (this._pre != null)
      throw 'uncommitted';
  }

  toJSON(): [string, number, ...any[]][] {
    return [this._char, this._tick, ...super.toJSON()];
  }

  static fromJSON(json: any, container: TSeqProcessArray, index: number): TSeqCharNode {
    const rtn = new TSeqCharNode(container, index);
    const [char, tick, ...rest] = json;
    rtn.set(char, tick);
    rtn.restFromJSON(rest);
    rtn.commit();
    return rtn;
  }

  get char() {
    return this._char;
  }

  get tick() {
    return this._tick;
  }

  get charTick(): TSeqCharTick {
    return [this.char, this.tick];
  }

  get pre() {
    return this._pre;
  }

  get next(): TSeqCharNode | undefined {
    return this.container.at(this.index + 1);
  }

  set(char: ''): void;
  set(char: string, tick: number): void;
  set(char: string, tick?: number) {
    if (char.length > 1)
      throw new RangeError('TSeq node value is one character');
    this._pre ??= this.charTick;
    this.container.incCharLength(char.length - this.char.length);
    this._char = char;
    if (char && tick != null)
      this._tick = tick;
  }

  commit() {
    delete this._pre;
    this.container.gc(this);
  }

  /**
   * 'Empty' is equivalent to an empty slot in the enclosing array, so subject
   * to garbage collection.
   */
  get isEmpty() {
    return !this.char && !this.hasRest;
  }

  static isEmpty(node: TSeqCharNode | undefined) {
    return !node || node.isEmpty;
  }

  get charLength(): number {
    return super.charLength + (this.char ? 1 : 0);
  }

  *preApply(
    path: TSeqName[],
    content: (TSeqCharTick | undefined)[],
    charIndex: number,
    force?: boolean
  ): Iterable<TSeqPreApply> {
    yield *super.preApply(path, content, charIndex + (this.char ? 1 : 0), force);
  }

  *chars(fromCharIndex = 0): IterableIterator<TSeqCharNode> {
    if (this.char) {
      if (fromCharIndex === 0)
        yield this;
      else
        fromCharIndex--;
    }
    yield *super.chars(fromCharIndex);
  }
}

/** @internal */
export class TSeqProcessArray extends TSeqContainer<TSeqCharNode> {
  /**
   * An empty position is equivalent to an empty node
   * @see TSeqCharNode#isEmpty
   */
  private readonly array: (TSeqCharNode | undefined)[] = [];
  /** inclusive minimum index */
  private start = 0;

  constructor(
    readonly pid: string,
    readonly parentNode: TSeqNode
  ) {
    super();
  }

  get parent() {
    return this.parentNode;
  }

  *children() {
    for (let node of this.array) {
      if (node)
        yield node;
    }
  }

  toJSON() {
    const { pid, start, array } = this;
    return [pid, start, ...array.map(node => node?.toJSON())];
  }

  static fromJSON(json: any[], parentNode: TSeqNode): TSeqProcessArray {
    let [pid, index, ...arrayJson] = json;
    const rtn = new TSeqProcessArray(pid, parentNode);
    rtn.start = index;
    rtn.array.length = arrayJson.length;
    for (let i = 0; i < arrayJson.length; i++, index++) {
      if (arrayJson[i])
        rtn.array[i] = TSeqCharNode.fromJSON(arrayJson[i], rtn, index);
    }
    return rtn;
  }

  static compare = (r1: TSeqProcessArray, r2: TSeqProcessArray) => compare(r1.pid, r2.pid);

  /** exclusive maximum index */
  get end() {
    return this.start + this.array.length;
  }

  private trimEnd() {
    let length = this.array.length;
    while (length > 0 && TSeqCharNode.isEmpty(this.array[length - 1]))
      length--;
    this.array.length = length;
  }

  private trimStart() {
    const trimCount = this.array.findIndex(n => !TSeqCharNode.isEmpty(n));
    if (trimCount > 0) {
      this.start += trimCount;
      this.array.splice(0, trimCount);
    }
  }

  gc(node: TSeqCharNode) {
    if (node.isEmpty) {
      if (node.index === this.start) {
        this.trimStart();
      } else if (node.index === this.end - 1) {
        this.trimEnd();
      } else if (node.index > this.start && node.index < this.end - 1) {
        delete this.array[this.arrayIndex(node.index)];
      }
      this.parentNode.gc(this);
    }
  }

  /** Caution: `undefined` is equivalent to an empty node */
  at(index: number): TSeqCharNode | undefined {
    const arrayIndex = this.arrayIndex(index);
    if (arrayIndex >= 0)
      return this.array[arrayIndex];
  }

  private arrayIndex(index: number) {
    return index - this.start;
  }

  setIfEmpty(index: number, char: string, tick: number) {
    let node = this.at(index);
    if (TSeqCharNode.isEmpty(node)) {
      node = this.ensureAt(index);
      node.set(char, tick);
      return true;
    }
  }

  private *newNodes(firstIndex: number, content: string, tick: number) {
    for (let c = 0; c < content.length; c++) {
      const node = new TSeqCharNode(this, firstIndex + c);
      node.set(content.charAt(c), tick);
      yield node;
    }
  }

  private ensureAt(index: number) {
    if (index < this.start) {
      this.array.unshift(...Array(this.start - index));
      this.start = index;
    }
    return this.array[this.arrayIndex(index)] ??= new TSeqCharNode(this, index);
  }

  push(content: string, tick: number): TSeqCharNode[] {
    this.array.push(...this.newNodes(this.end, content, tick));
    return <TSeqCharNode[]>this.array.slice(-content.length);
  }

  unshift(pid: string, content: string, tick: number): TSeqCharNode[] {
    if (pid === this.pid) {
      this.start -= content.length;
      this.array.unshift(...this.newNodes(this.start, content, tick));
      return <TSeqCharNode[]>this.array.slice(0, content.length);
    } else {
      this.start--;
      const node = new TSeqCharNode(this, this.start);
      this.array.unshift(node);
      return node.push(pid, content, tick);
    }
  }

  *preApply(
    indexInArray: number,
    path: TSeqName[],
    content: (TSeqCharTick | undefined)[],
    charIndex: number,
    force?: boolean
  ): Iterable<TSeqPreApply> {
    for (let i = this.start; i < indexInArray; i++)
      charIndex += this.at(i)?.charLength ?? 0;
    if (path.length) {
      // Don't create a non-existent node if it's just deletes
      const node = hasInserts(content) ? this.ensureAt(indexInArray) : this.at(indexInArray);
      if (node)
        yield *node.preApply(path, content, charIndex);
    } else {
      for (let indexInRun = 0; indexInRun < content.length; indexInRun++, indexInArray++) {
        const charTick = content[indexInRun];
        const node = this.at(indexInArray);
        if (charTick != null && (TSeqCharTick.inApplyOrder(node?.charTick, charTick) || force)) {
          const [char] = charTick;
          const pre = { post: charTick, charIndex, indexInRun };
          if (char) // Setting
            yield Object.assign(pre, { node: node ?? this.ensureAt(indexInArray) });
          else if (node?.char) // Deleting
            yield Object.assign(pre, { node });
          if (node != null)
            charIndex += node.charLength;
        }
      }
    }
  }
}
