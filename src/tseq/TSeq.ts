abstract class TSeqCharContainer {
  /** Characters contributed to TSeq string */
  private _charLength = 0;

  checkInvariants() {
    for (let child of this.children())
      child.checkInvariants();
  }

  get charLength() {
    return this._charLength;
  }

  incCharLength(inc: number) {
    this._charLength += inc;
    this.parent?.incCharLength(inc);
  }

  *chars(fromCharIndex = 0): IterableIterator<TSeqCharNode> {
    for (let child of this.children()) {
      if (fromCharIndex === 0 || fromCharIndex < child.charLength) {
        yield *child.chars(fromCharIndex);
        fromCharIndex = 0;
      } else {
        fromCharIndex -= child.charLength;
      }
    }
  }

  abstract get parent(): TSeqCharContainer | undefined;

  abstract children(): Iterable<TSeqCharContainer>;
}

abstract class TSeqNode extends TSeqCharContainer {
  /** Arrays per-process, sorted by process ID */
  private readonly rest: TSeqProcessArray[] = [];

  protected constructor(
    readonly path: TSeqName[]
  ) {
    super();
  }

  checkInvariants() {
    super.checkInvariants();
    const sorted = [...this.rest].sort(TSeqProcessArray.compare);
    if (!this.rest.every((arr, i) => arr === sorted[i]))
      throw 'arrays are not in order';
  }

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
    [[[pid, index], ...tail], content]: TSeqOperation,
    charIndex: number
  ): Iterable<TSeqPreApply> {
    // If a pure delete, don't force creation
    const restIndex = this.getRestIndex(pid, content.some(([char]) => char));
    if (restIndex > -1) {
      for (let i = 0; i < restIndex; i++)
        charIndex += this.rest[i].charLength;
      yield *this.rest[restIndex].preApply(
        index, [tail, content], charIndex);
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

class TSeqCharNode extends TSeqNode {
  private _char = '';
  private _tick = 0;

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
  }
  
  toJSON(): [string, number, ...any[]][] {
    return [this._char, this._tick, ...super.toJSON()];
  }

  static fromJSON(json: any, container: TSeqProcessArray, index: number): TSeqCharNode {
    const rtn = new TSeqCharNode(container, index);
    const [char, tick, ...rest] = json;
    rtn.set(char, tick);
    rtn.restFromJSON(rest);
    return rtn;
  }

  get char() {
    return this._char;
  }

  get tick() {
    return this._tick;
  }

  set(char: ''): TSeqCharTick;
  set(char: string, tick: number): TSeqCharTick;
  set(char: string, tick?: number) {
    if (char.length > 1)
      throw new RangeError('TSeq node value is one character');
    const old = [this.char, this.tick];
    this.container.incCharLength(char.length - this.char.length);
    this._char = char;
    if (char && tick != null)
      this._tick = tick;
    return old;
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

  *preApply(op: TSeqOperation, charIndex: number): Iterable<TSeqPreApply> {
    yield *super.preApply(op, charIndex + (this.char ? 1 : 0));
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

class TSeqProcessArray extends TSeqCharContainer {
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

  static compare = (r1: TSeqProcessArray, r2: TSeqProcessArray) => r1.pid.localeCompare(r2.pid);

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
  at(index: number) {
    const arrayIndex = this.arrayIndex(index);
    if (arrayIndex >= 0)
      return this.array[arrayIndex];
  }

  private arrayIndex(index: number) {
    return index - this.start;
  }

  setIfEmpty(index: number, char: string, tick: number): TSeqCharNode | undefined {
    let node = this.at(index);
    if (TSeqCharNode.isEmpty(node)) {
      node = this.ensureAt(index);
      node.set(char, tick);
      return node;
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
    index: number,
    [path, content]: TSeqOperation,
    charIndex: number
  ): Iterable<TSeqPreApply> {
    for (let i = this.start; i < index; i++)
      charIndex += this.at(i)?.charLength ?? 0;
    if (path.length) {
      // Don't create a non-existent node if it's just deletes
      const node = content.some(([char]) => char) ? this.ensureAt(index) : this.at(index);
      if (node)
        yield *node.preApply([path, content], charIndex);
    } else {
      for (let c = 0; c < content.length; c++, index++) {
        const charTick = content[c];
        const [char, tick] = charTick;
        const node = this.at(index);
        if (char) { // Setting
          // Ignore if our tick is ahead
          if (node != null && tick <= node.tick)
            continue;
          yield { node: node ?? this.ensureAt(index), charTick, charIndex };
        } else if (node?.char) { // Deleting
          // Ignore if our tick is ahead
          if (tick < node.tick)
            return;
          yield { node, charTick, charIndex };
        }
        if (node != null)
          charIndex += node.charLength;
      }
    }
  }
}

export type TSeqSplice = Parameters<TSeq['splice']>;
const $index = 0, $deleteCount = 1, $content = 2;

type TSeqName = [pid: string, index: number];
type TSeqCharTick = [char: string, tick: number];
export type TSeqOperation = [path: TSeqName[], content: TSeqCharTick[]];
export type TSeqRevertOperation = TSeqOperation;

interface TSeqPreApply {
  node: TSeqCharNode;
  charTick: TSeqCharTick;
  charIndex: number;
}

export class TSeq extends TSeqNode {
  private tick = 0;

  constructor(
    readonly pid: string
  ) {
    super([]);
  }

  /** Possibly expensive invariant checking, intended to be called in testing */
  checkInvariants() {
    super.checkInvariants();
    if (this.charLength !== this.toString().length)
      throw 'incorrect character length';
  }

  toJSON(): any {
    return { tick: this.tick, rest: super.toJSON() };
  }

  static fromJSON(pid: string, json: any): TSeq {
    const { tick, rest } = json;
    const rtn = new TSeq(pid);
    rtn.tick = tick;
    rtn.restFromJSON(rest);
    return rtn;
  }

  get parent() {
    return undefined;
  }

  toString() {
    let rtn = '';
    for (let node of this.chars())
      rtn += node.char;
    return rtn;
  }

  apply(operations: TSeqOperation[], cb?: (revert: TSeqRevertOperation[]) => void) {
    const nodePreApply: TSeqPreApply[] = [];
    // Pre-apply to get the character indexes
    for (let operation of operations)
      nodePreApply.push(...this.preApply(operation, 0));
    // Apply the content, accumulating metadata
    const nodePrior = cb && new Map<TSeqCharNode, TSeqCharTick>();
    const splices: TSeqSplice[] = [];
    // Operations can 'jump' intermediate affected characters
    nodePreApply.sort(({ charIndex: c1 }, { charIndex: c2 }) =>
      c1 === c2 ? 0 : c1 > c2 ? 1 : -1);
    for (let { node, charTick: [char, tick], charIndex } of nodePreApply) {
      const afterCharTick = node.set(char, tick);
      nodePrior?.set(node, afterCharTick);
      this.addToSplices(splices, charIndex, char);
    }
    // Gather reversion operations prior to garbage collection
    const revert = nodePrior ? [...this.toOps(nodePrior)] : [];
    for (let { node } of nodePreApply)
      node.container.gc(node);
    cb?.(revert);
    return splices;
  }

  private addToSplices(splices: TSeqSplice[], charIndex: number, char: string) {
    let last = splices[splices.length - 1];
    if (last != null && charIndex < last[$index])
      throw new RangeError('Expecting spliced chars in order');
    if (last == null || charIndex > last[$index] + last[$deleteCount])
      splices.push(last = [charIndex, 0, '']);
    last[$deleteCount] -= char.length - 1;
    last[$content] += char;
  }

  splice(index: number, deleteCount: number, content = ''): TSeqOperation[] {
    if (index < 0)
      throw new RangeError();
    if (!deleteCount && !content.length)
      return []; // Shortcut
    const deletes = this.delete(index, deleteCount);
    const inserts = this.insert(index, content);
    // TODO: Supply the reverts to a callback, as per the apply method
    const ops = [...this.toOps([...deletes, ...inserts])];
    for (let node of deletes)
      node.container.gc(node);
    return ops;
  }

  private delete(index: number, deleteCount: number) {
    if (deleteCount > 0) {
      const deletes: TSeqCharNode[] = Array(deleteCount);
      let d = 0;
      for (let node of this.chars(index)) {
        if (d === deleteCount) break;
        (deletes[d++] = node).set('');
      }
      deletes.length = d; // In case beyond end
      return deletes;
    } else {
      return [];
    }
  }

  private insert(index: number, content: string) {
    if (!content)
      return [];
    // Any insert ticks our clock
    this.tick++;
    const inserts: TSeqCharNode[] = Array(content.length);
    const charIt = this.chars(index > 0 ? index - 1 : 0);
    let itRes = index > 0 ? charIt.next() : null;
    let node = itRes && (itRes.done ? null : itRes.value), c = 0;
    if (node?.container.pid === this.pid && !node.hasRest) {
      while (node && c < content.length) {
        if (!node.container.setIfEmpty(node.index + 1, content.charAt(c), this.tick))
          break;
        inserts[c++] = node = charIt.next().value!;
      }
      content = content.slice(c);
    }
    if (content) {
      itRes = charIt.next();
      const next: TSeqCharNode | null = itRes.done ? null : itRes.value;
      const newNodes = !next || next.container === node?.container ?
        // Append to the current
        (node ?? this).push(this.pid, content, this.tick) :
        // Prepend to the array containing the next
        next.container.unshift(this.pid, content, this.tick);
      for (let n of newNodes)
        inserts[c++] = n;
    }
    return inserts;
  }

  /**
   * All nodes must still be attached to their containers, i.e. prior to garbage
   * collection.
   */
  private *toOps(
    nodeStates: Map<TSeqCharNode, TSeqCharTick> | TSeqCharNode[]
  ): Iterable<TSeqOperation> {
    const nodes = Array.isArray(nodeStates) ? nodeStates : [...nodeStates.keys()];
    const charTick: (node: TSeqCharNode) => TSeqCharTick = Array.isArray(nodeStates) ?
      node => [node.char, node.tick] : node => nodeStates.get(node)!;
    // TODO: This reconstructs runs which are already about known in the insert
    // method, combining them with the deletes
    const anchors = new Map(nodes.map(node => [node, [] as TSeqCharTick[]]));
    for (let [node, run] of anchors) {
      for (let next: TSeqCharNode | undefined = node; next != null;) {
        next = next.container.at(next.index + 1);
        if (next && anchors.has(next)) {
          run.push(charTick(next), ...anchors.get(next)!);
          anchors.delete(next);
        }
      }
    }
    for (let [node, run] of anchors) {
      run.unshift(charTick(node));
      yield [node.path, run];
    }
  }
}
