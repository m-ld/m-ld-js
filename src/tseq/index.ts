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
        return -1
      if (index1 > index2)
        return 1;
    }
    return 0;
  }

  protected constructor(
    readonly path: TSeqName[]
  ) {
  }

  // next(afterPid?: string): TSeqCharNode | undefined {
  //   for (
  //     let i = afterPid ? this.rest.findIndex(a => a.pid > afterPid) : 0;
  //     i > -1 && i < this.rest.length;
  //     i++
  //   ) {
  //     const [node] = this.rest[i];
  //     if (node) return node;
  //   }
  // }

  push(pid: string, content: string) {
    return this.rest[this.getRestIndex(pid, true)].push(content);
  }

  get hasRest() {
    return !!this.rest.length;
  }

  /** @returns the actually-affected character nodes (ignoring empty -> empty) */
  *applyAt([[[pid, index], ...tail], content]: TSeqRun): Iterable<TSeqCharNode> {
    // If a pure delete, don't force creation
    const restIndex = this.getRestIndex(pid, typeof content == 'string');
    if (restIndex > -1)
      yield *this.rest[restIndex].applyAt(index, [tail, content]);
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

  toJSON(): any {
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

  constructor(
    readonly container: TSeqArray,
    /** Index into sparse container array */
    readonly index: number
  ) {
    super(container.parentNode.path.concat([[container.pid, index]]));
  }

  toJSON() {
    return this.hasRest ? [this._char, ...super.toJSON()] : this._char;
  }

  static fromJSON(json: any, container: TSeqArray, index: number): TSeqCharNode {
    const rtn = new TSeqCharNode(container, index);
    if (Array.isArray(json)) {
      const [char, ...rest] = json;
      rtn.char = char;
      rtn.restFromJSON(rest);
    } else {
      rtn.char = json;
    }
    return rtn;
  }

  get char() {
    return this._char;
  }

  set char(char: string) {
    if (char.length > 1)
      throw new RangeError('TSeq node value is none or one character');
    if (char === '\x00') // Used in serialisations
      char = '';
    if (!this._char === !char)
      this.container.onCharSet(this);
    this._char = char;
  }

  /**
   * 'Empty' is equivalent to an empty slot in the enclosing array, so subject
   * to garbage collection.
   */
  get isEmpty() {
    return !this.char && !this.hasRest;
  }

  // next(afterPid?: string): TSeqCharNode | undefined {
  //   // When run out of arrays, go to container
  //   return super.next(afterPid) ?? this.container.nodeAfter(this.index);
  // }

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
  /** character length, counts only filled character nodes */
  private _length = 0;

  constructor(
    readonly pid: string,
    readonly parentNode: TSeqNode
  ) {
  }

  toJSON() {
    const { pid, start, array } = this;
    if (array.every(node => !node?.hasRest))
      return [pid, start, array.map(node => node?.char || '\x00').join('')];
    else
      return [pid, start, ...array.map(node => node?.toJSON() ?? '')];
  }

  static fromJSON(json: any[], parentNode: TSeqNode): TSeqArray {
    let [pid, start, ...array] = json;
    const rtn = new TSeqArray(pid, parentNode);
    rtn.start = start;
    if (array.length === 1 && typeof array[0] == 'string')
      rtn.array.push(...rtn.newNodes(start, array[0]));
    else
      rtn.array.push(...array.map((c, i) =>
        TSeqCharNode.fromJSON(c, rtn, start + i)));
    return rtn;
  }

  /** exclusive maximum index */
  get end() {
    return this.start + this.array.length;
  }

  get length() {
    return this._length;
  }

  onCharSet(node: TSeqCharNode) {
    this._length += (node.char ? 1 : -1);
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

  setIfEmpty(index: number, char: string): TSeqCharNode | undefined {
    let node = this.at(index);
    if (node) {
      if (node.isEmpty) {
        node.char = char;
        return node;
      }
    } else {
      return this.setAt(index, char);
    }
  }

  private *newNodes(firstIndex: number, content: string) {
    for (let c = 0; c < content.length; c++) {
      const node = new TSeqCharNode(this, firstIndex + c);
      node.char = content.charAt(c);
      yield node;
    }
  }

  private setAt(index: number, char?: string) {
    if (index < this.start) {
      this.array.unshift(...Array(this.start - index));
      this.start = index;
    }
    const node = this.array[this.arrayIndex(index)] ??= new TSeqCharNode(this, index);
    if (char)
      node.char = char;
    return node;
  }

  push(content: string): TSeqCharNode[] {
    this.array.push(...this.newNodes(this.end, content));
    return <TSeqCharNode[]>this.array.slice(-content.length);
  }

  unshift(pid: string, content: string): TSeqCharNode[] {
    if (pid === this.pid) {
      this.start -= content.length;
      this.array.unshift(...this.newNodes(this.start, content));
      return <TSeqCharNode[]>this.array.slice(0, content.length);
    } else {
      this.start--;
      const node = new TSeqCharNode(this, this.start);
      this.array.unshift(node);
      return node.push(pid, content);
    }
  }

  *applyAt(index: number, [path, content]: TSeqRun): Iterable<TSeqCharNode> {
    if (path.length) {
      // Don't create a non-existent node if it's just deletes
      const node = typeof content == 'number' ? this.at(index) : this.setAt(index);
      if (node)
        yield *node.applyAt([path, content]);
    } else {
      yield *new TSeqArray.RunApplication(this, index).apply(content);
    }
  }

  // TODO: Does not need to be a class any more
  private static RunApplication = class {
    constructor(
      readonly array: TSeqArray,
      private index: number
    ) {
    }

    *apply(content: string | number): Iterable<TSeqCharNode> {
      if (typeof content == 'number') {
        // Applying a run of unsets
        for (let c = 0; c < content; c++, this.index++)
          yield *this.setChar('');
      } else {
        // Applying a run of combined unsets and sets
        for (let c = 0; c < content.length; c++, this.index++)
          yield *this.setChar(content.charAt(c));
      }
    }

    private *setChar(char: string) {
      if (char === '\x00')
        char = '';
      let node = this.array.at(this.index);
      if (char || (node && node.char)) {
        node ??= this.array.setAt(this.index, char);
        node.char = char;
        yield node;
      }
    }
  };

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
type TSeqRun = [path: TSeqName[], content: string | number];

export class TSeqOperation {
  static fromJSON(json: any) {
    const { run, tick } = json;
    return new TSeqOperation(run, tick);
  }

  constructor(
    readonly run: TSeqRun,
    readonly tick: number
  ) {
    const [path] = run;
    if (!path.length)
      throw new RangeError('a run must have a path');
  }

  get pid() {
    const [path] = this.run, [pid] = path[path.length - 1];
    return pid;
  }

  get hasInserts() {
    const [, content] = this.run;
    return typeof content == 'string';
  }
}

export class TSeq extends TSeqNode {
  private readonly ticks: { [pid: string]: number } = {};

  constructor(
    readonly pid: string
  ) {
    super([]);
  }

  toJSON() {
    const { ticks } = this;
    return [ticks, ...super.toJSON()];
  }

  static fromJSON(pid: string, json: any): TSeq {
    const [ticks, ...rest] = json;
    const rtn = new TSeq(pid);
    Object.assign(rtn.ticks, ticks);
    rtn.restFromJSON(rest);
    return rtn;
  }

  toString() {
    let rtn = '';
    for (let node of this)
      rtn += node.char;
    return rtn;
  }

  apply(operations: TSeqOperation[]) {
    let changed = false;
    for (let operation of operations) {
      // Inserts tick the process clock
      const expectedTick =
        (this.ticks[operation.pid] ?? 0) + (operation.hasInserts ? 1 : 0);
      if (operation.tick < expectedTick)
        continue; // Ignore old operation
      if (operation.tick > expectedTick)
        throw new RangeError(`missed operation from ${operation.pid}`);
      // TODO: Return splices
      for (let _ of this.applyAt(operation.run))
        changed = true;
      this.ticks[operation.pid] = operation.tick;
    }
    return changed;
  }

  splice(index: number, deleteCount: number, content = ''): TSeqOperation[] {
    if (index < 0)
      throw new RangeError();
    if (!deleteCount && !content.length)
      return []; // Shortcut
    // Seek to just-before the given index
    const deletes = this.delete(index, deleteCount);
    const inserts = this.insert(index, content);
    return [...this.toRuns(...deletes, ...inserts)];
  }

  private charItFrom(index: number) {
    const charIt = new TSeqCharIterator(this);
    if (index > 0)
      charIt.seekTo(() => charIt.charIndex === index - 1);
    return charIt;
  }

  private delete(index: number, deleteCount: number) {
    // If the char iterator has no node, nothing to delete
    if (deleteCount > 0) {
      const charIt = this.charItFrom(index);
      const deletes: TSeqCharNode[] = Array(deleteCount);
      let d = 0;
      for (let node of charIt) {
        if (d === deleteCount) break;
        (deletes[d++] = node).char = '';
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
    this.ticks[this.pid] ??= 0;
    this.ticks[this.pid]++;
    const charIt = this.charItFrom(index);
    const inserts: TSeqCharNode[] = Array(content.length);
    let node = charIt.charNode, c = 0;
    if (node?.container.pid === this.pid && !node.hasRest) {
      while (node && c < content.length) {
        if (!node.container.setIfEmpty(node.index + 1, content.charAt(c)))
          break;
        inserts[c++] = node = charIt.next().charNode!;
      }
      content = content.slice(c);
    }
    if (content) {
      const next = charIt.next().charNode;
      const newNodes = !next || next.container === node?.container ?
        // Append to the current
        (node ?? this).push(this.pid, content) :
        // Prepend to the array containing the next
        next.container!.unshift(this.pid, content);
      for (let n of newNodes)
        inserts[c++] = n;
    }
    return inserts;
  }

  private *toRuns(...nodes: TSeqCharNode[]): Iterable<TSeqOperation> {
    // TODO: This reconstructs runs which are already about known in the insert
    // method, but interlacing them with deletes
    const affected = new Map<TSeqCharNode, TSeqCharNode[]>(
      nodes.map(node => [node, []]));
    for (let [node, run] of affected) {
      for (let next: TSeqCharNode | undefined = node; next != null;) {
        next = next.container.at(next.index + 1);
        if (next && affected.has(next)) {
          run.push(next, ...affected.get(next)!);
          affected.delete(next);
        }
      }
    }
    for (let [node, run] of affected) {
      run.unshift(node);
      yield new TSeqOperation([
        node.path,
        run.every(node => !node.char) ?
          run.length : // A pure deletion with count
          run.map(node => node.char ? node.char : '\x00').join('')
      ], this.ticks[node.container.pid]);
    }
  }
}
