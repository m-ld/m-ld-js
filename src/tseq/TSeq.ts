abstract class TSeqNode implements Iterable<TSeqCharNode> {
  /** Arrays per-process, sorted by process ID */
  private readonly rest: TSeqArray[] = [];

  static compare = (n1: TSeqNode, n2: TSeqNode) => {
    const path1 = n1.path, path2 = n2.path;
    for (let i = 0; i < path1.length && i < path2.length; i++) {
      const [pid1, index1] = path1[i];
      const [pid2, index2] = path2[i];
      if (pid1 < pid2)
        return -1;
      if (pid1 > pid2)
        return 1;
      if (index1 < index2)
        return -1;
      if (index1 > index2)
        return 1;
    }
    return 0;
  };

  protected constructor(
    readonly path: TSeqName[]
  ) {}

  push(pid: string, content: string, tick: number) {
    return this.rest[this.getRestIndex(pid, true)].push(content, tick);
  }

  get hasRest() {
    return !!this.rest.length;
  }

  /** @returns the actually-affected character nodes (ignoring empty -> empty) */
  *applyAt(
    [[[pid, index], ...tail], content]: TSeqOperation
  ): Iterable<[TSeqCharNode, TSeqCharTick]> {
    // If a pure delete, don't force creation
    const restIndex = this.getRestIndex(pid, content.some(([char]) => char));
    if (restIndex > -1)
      yield *this.rest[restIndex].applyAt(index, [tail, content]);
  }

  gc(array: TSeqArray) {
    if (array[Symbol.iterator]().next().done) {
      const index = this.getRestIndex(array.pid);
      this.rest.splice(index, 1);
    }
  }

  *[Symbol.iterator](): IterableIterator<TSeqCharNode> {
    for (let array of this.rest)
      yield *array;
  }

  private getRestIndex(pid: string, create = true): number {
    // TODO: This could be a binary search
    for (let i = 0; i < this.rest.length; i++) {
      if (this.rest[i].pid === pid) {
        return i;
      } else if (this.rest[i].pid > pid) {
        if (create) {
          const rest = new TSeqArray(pid, this);
          this.rest.splice(i, 0, rest);
          return i;
        } else {
          return -1;
        }
      }
    }
    if (create)
      return this.rest.push(new TSeqArray(pid, this)) - 1;
    else
      return -1;
  }

  toJSON(): any[] {
    return this.rest.map(array => array.toJSON());
  }

  restFromJSON(json: any[]) {
    this.rest.push(...json
      .map(array => TSeqArray.fromJSON(array, this))
      .sort((r1, r2) => r1.pid.localeCompare(r2.pid)));
  }
}

class TSeqCharNode extends TSeqNode {
  private _char = '';
  private _tick = 0;

  constructor(
    readonly container: TSeqArray,
    /** Index into sparse container array */
    readonly index: number
  ) {
    super(container.parentNode.path.concat([[container.pid, index]]));
  }

  toJSON(): [string, number, ...any[]][] {
    return [this._char, this._tick, ...super.toJSON()];
  }

  static fromJSON(json: any, container: TSeqArray, index: number): TSeqCharNode {
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
    this._char = char;
    if (tick != null)
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

  *[Symbol.iterator](): IterableIterator<TSeqCharNode> {
    if (this.char)
      yield this;
    yield *super[Symbol.iterator]();
  }
}

/**
 * Implements Iterable to allow use in loops
 */
class TSeqCharIterator implements Iterable<TSeqCharNode> {
  private readonly charIt: Iterator<TSeqCharNode>;
  private _charIndex = -1;
  private _charNode: TSeqCharNode | undefined;

  constructor(container: Iterable<TSeqCharNode>) {
    this.charIt = container[Symbol.iterator]();
  }

  get charIndex() {
    return this._charIndex;
  }

  get charNode() {
    return this._charNode;
  }

  seekTo(predicate: true | ((node: TSeqCharNode) => boolean)) {
    for (let node of this) {
      if (predicate === true || predicate(node))
        return this;
    }
    return this;
  }

  next() {
    return this.seekTo(true);
  }

  /** Continues iteration from last */
  *[Symbol.iterator](): Iterator<TSeqCharNode> {
    for (let result = this.charIt.next(); !result.done; result = this.charIt.next()) {
      this._charIndex++;
      yield this._charNode = result.value;
    }
    delete this._charNode;
  }
}

class TSeqArray implements Iterable<TSeqCharNode> {
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
  ) {}

  toJSON() {
    const { pid, start, array } = this;
    return [pid, start, ...array.map(node => node?.toJSON())];
  }

  static fromJSON(json: any[], parentNode: TSeqNode): TSeqArray {
    let [pid, index, ...arrayJson] = json;
    const rtn = new TSeqArray(pid, parentNode);
    rtn.start = index;
    rtn.array.length = arrayJson.length;
    for (let i = 0; i < arrayJson.length; i++) {
      if (arrayJson[i])
        rtn.array[i] = TSeqCharNode.fromJSON(arrayJson[i], rtn, index++);
    }
    return rtn;
  }

  /** exclusive maximum index */
  get end() {
    return this.start + this.array.length;
  }

  private trimEnd() {
    while (this.array.length > 0 && TSeqCharNode.isEmpty(this.array[this.array.length - 1]))
      this.array.length--;
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

  *applyAt(
    index: number,
    [path, content]: TSeqOperation
  ): Iterable<[TSeqCharNode, TSeqCharTick]> {
    if (path.length) {
      // Don't create a non-existent node if it's just deletes
      const node = content.some(([char]) => char) ? this.ensureAt(index) : this.at(index);
      if (node)
        yield *node.applyAt([path, content]);
    } else {
      for (let c = 0; c < content.length; c++, index++) {
        const [char, tick] = content[c];
        let node = this.at(index);
        if (char) {
          // Ignore if our tick is ahead
          if (node != null && tick <= node.tick)
            continue;
          node ??= this.ensureAt(index);
          yield [node, node.set(char, tick)];
        } else if (node?.char) {
          // Ignore if our tick is ahead
          if (tick < node.tick)
            return;
          yield [node, node.set(char, tick)];
        }
      }
    }
  }

  *[Symbol.iterator](): Iterator<TSeqCharNode> {
    for (let node of this.array)
      if (node)
        yield *node;
  }

  // nodeAfter(index: number) {
  //   while (++index < this.end) {
  //     const [node] = this.at(index) ?? [];
  //     if (node) return node;
  //   }
  //   // Reached the end of the array, go to the next array in the parent
  //   return this.parentNode.next(this.pid);
  // }
}

type TSeqName = [pid: string, index: number];
type TSeqCharTick = [char: string, tick: number];
export type TSeqOperation = [path: TSeqName[], content: TSeqCharTick[]];
export type TSeqRevertOperation = TSeqOperation;

export class TSeq extends TSeqNode {
  private tick = 0;

  constructor(
    readonly pid: string
  ) {
    super([]);
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

  toString() {
    let rtn = '';
    for (let node of this)
      rtn += node.char;
    return rtn;
  }

  apply(operations: TSeqOperation[], cb?: (revert: TSeqRevertOperation[]) => void) {
    const affected: [TSeqCharNode, TSeqCharTick][] = [];
    // TODO: Return splices
    for (let operation of operations)
      affected.push(...this.applyAt(operation));
    for (let [node] of affected)
      node.container.gc(node);
    cb?.([...this.toRuns(affected)]);
    return affected.length > 0;
  }

  splice(index: number, deleteCount: number, content = ''): TSeqOperation[] {
    if (index < 0)
      throw new RangeError();
    if (!deleteCount && !content.length)
      return []; // Shortcut
    const deletes = this.delete(index, deleteCount);
    const inserts = this.insert(index, content);
    // TODO: Supply the reverts to a callback, as per the apply method
    const ops = [...this.toRuns(
      [...deletes, ...inserts].map(node => [node, [node.char, node.tick]]))];
    for (let node of deletes)
      node.container.gc(node);
    return ops;
  }

  /** Seeks to just-before the given index */
  private charItFrom(index: number) {
    const charIt = new TSeqCharIterator(this);
    if (index > 0)
      charIt.seekTo(() => charIt.charIndex === index - 1);
    return charIt;
  }

  private delete(index: number, deleteCount: number) {
    if (deleteCount > 0) {
      const charIt = this.charItFrom(index);
      const deletes: TSeqCharNode[] = Array(deleteCount);
      let d = 0;
      for (let node of charIt) {
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
    const charIt = this.charItFrom(index);
    const inserts: TSeqCharNode[] = Array(content.length);
    let node = charIt.charNode, c = 0;
    if (node?.container.pid === this.pid && !node.hasRest) {
      while (node && c < content.length) {
        if (!node.container.setIfEmpty(node.index + 1, content.charAt(c), this.tick))
          break;
        inserts[c++] = node = charIt.next().charNode!;
      }
      content = content.slice(c);
    }
    if (content) {
      const next = charIt.next().charNode;
      const newNodes = !next || next.container === node?.container ?
        // Append to the current
        (node ?? this).push(this.pid, content, this.tick) :
        // Prepend to the array containing the next
        next.container!.unshift(this.pid, content, this.tick);
      for (let n of newNodes)
        inserts[c++] = n;
    }
    return inserts;
  }

  private *toRuns(nodeStates: [TSeqCharNode, TSeqCharTick][]): Iterable<TSeqOperation> {
    const nodeState = new Map(nodeStates);
    // TODO: This reconstructs runs which are already about known in the insert
    // method, but adding them to the deletes
    const affected = new Map<TSeqCharNode, TSeqCharTick[]>(
      [...nodeState].map(([node]) => [node, []]));
    for (let [node, run] of affected) {
      for (let next: TSeqCharNode | undefined = node; next != null;) {
        next = next.container.at(next.index + 1);
        if (next && affected.has(next)) {
          run.push(nodeState.get(next)!, ...affected.get(next)!);
          affected.delete(next);
        }
      }
    }
    for (let [node, run] of affected) {
      run.unshift(nodeState.get(node)!);
      yield [node.path, run];
    }
  }
}
