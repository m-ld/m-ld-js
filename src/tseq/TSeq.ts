import { TSeqCharNode, TSeqNode } from './TSeqNode';
import { TSeqOperable } from './TSeqOperable';
import { TSeqCharTick, TSeqLocalOperation, TSeqOperation, TSeqRevert, TSeqSplice } from './types';
import { compare, concatIter } from '../engine/util';

/**
 * Result of a pre-apply step during {@link TSeq#apply}.
 * @internal
 */
export interface TSeqPreApply {
  node: TSeqCharNode;
  post: TSeqCharTick;
  charIndex: number;
  indexInRun: number;
}

/**
 * A CRDT for plain text. Characters are positioned into an n-ary tree, in which
 * each node is a sparse array, extensible in either positive or negative index
 * direction, 'belonging' to a process identity. The tree is read in depth-first
 * order. The use of owned-arrays reflects an optimisation for the typical text
 * document pattern of editing – a user extends some selected text with new
 * text.
 *
 * A TSeq requires the framework guarantees of a 'shared data type' in **m-ld**:
 * operations must be {@link apply applied} only once, and in causal order.
 *
 * @category Experimental
 * @experimental
 */
export class TSeq extends TSeqNode {
  private tick = 0;

  constructor(
    /** The local process ID */
    readonly pid: string
  ) {
    super([]);
  }

  /** @inheritDoc */
  checkInvariants() {
    super.checkInvariants();
    if (this.charLength !== this.toString().length)
      throw 'incorrect character length';
  }

  /** @inheritDoc */
  toJSON(): any {
    return { tick: this.tick, rest: super.toJSON() };
  }

  /** Create a new TSeq from a representation created with {@link toJSON}. */
  static fromJSON(pid: string, json: any): TSeq {
    const { tick, rest } = json;
    const rtn = new TSeq(pid);
    rtn.tick = tick;
    rtn.restFromJSON(rest);
    return rtn;
  }

  /** @inheritDoc */
  get parent() {
    return undefined;
  }

  toString() {
    let rtn = '';
    for (let node of this.chars())
      rtn += node.char;
    return rtn;
  }

  /**
   * Applies or reverts (see `revert` param) remote operations to this local
   * clone of the TSeq.
   *
   * @param operation the operation to apply, or `null` if only reverting.
   * @param reversions are local operations to be reverted _prior_ to applying
   * `operation`, in reverse order of original application. The effect of these
   * will be included in the returned splice, but not in the `revert` metadata.
   * @param revert will be populated with reversion metadata for the `operation`
   */
  apply(
    operation: TSeqOperation | null,
    reversions: TSeqLocalOperation[] = [],
    revert: TSeqRevert = []
  ) {
    // Pre-apply to get the character indexes
    const nodePreApply = new Map<TSeqCharNode, TSeqPreApply>();
    for (let [operation, revert] of reversions) {
      if (revert == null)
        throw new TypeError('Cannot revert without reversion metadata');
      for (let run = 0; run < operation.length; run++) {
        const [path] = operation[run];
        for (let pre of this.preApply(path, revert[run], 0, true))
          nodePreApply.set(pre.node, pre);
      }
    }
    if (operation != null) {
      for (let run = 0; run < operation.length; run++) {
        const [path, content] = operation[run];
        for (let pre of this.preApply(path, content, 0)) {
          (revert[run] ??= [])[pre.indexInRun] = pre.node.charTick;
          nodePreApply.set(pre.node, pre);
        }
      }
    }
    const preApply = [...nodePreApply.values()];
    // Apply the content, accumulating metadata
    const splices: TSeqSplice[] = [];
    // Operations can 'jump' intermediate affected characters
    preApply.sort((p1, p2) => compare(p1.charIndex, p2.charIndex));
    for (let { node, post: [char, tick], charIndex } of preApply) {
      node.set(char, tick);
      const [oldChar] = node.pre ?? [''];
      this.addToSplices(splices, charIndex, oldChar, char);
    }
    for (let { node } of preApply)
      node.commit();
    return splices;
  }

  private addToSplices(
    splices: TSeqSplice[],
    charIndex: number,
    del: string,
    ins: string
  ) {
    let last = splices[splices.length - 1];
    if (last != null && charIndex < last[TSeqSplice.$index])
      throw new RangeError('Expecting spliced chars in order');
    if (last == null || charIndex > last[TSeqSplice.$index] + last[TSeqSplice.$deleteCount])
      splices.push(last = [charIndex, 0, '']);
    last[TSeqSplice.$deleteCount] += del.length;
    last[TSeqSplice.$content] += ins;
  }

  /**
   * Changes the text content of this TSeq by removing or replacing existing
   * characters and/or adding new characters in place.
   * @param index zero-based index at which to start changing the text
   * @param deleteCount the number of characters in the text to remove from `index`
   * @param content the characters to add to the text, beginning from `index`
   * @param revert will be populated with reversion metadata for the returned
   * operation.
   */
  splice(
    index: number,
    deleteCount: number,
    content = '',
    revert?: TSeqRevert
  ): TSeqOperation {
    if (index < 0)
      throw new RangeError();
    if (revert?.length)
      throw new RangeError('Expecting empty revert to populate');
    if (!deleteCount && !content.length)
      return []; // Shortcut
    const nodes = new Set(concatIter(
      this.delete(index, deleteCount),
      this.insert(index, content)
    ));
    // TODO: This reconstructs runs which are already about known in the insert
    // method, combining them with the deletes
    const operation = TSeqOperable.toRuns(nodes, revert);
    for (let node of nodes)
      node.commit();
    return operation;
  }

  private delete(index: number, deleteCount: number): TSeqCharNode[] {
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

  private insert(index: number, content: string): TSeqCharNode[] {
    if (!content)
      return [];
    // Any insert ticks our clock
    this.tick++;
    const inserts: TSeqCharNode[] = Array(content.length);
    if (!isFinite(index)) index = this.charLength;
    const charIt = this.chars(index > 0 ? index - 1 : 0);
    let itRes = index > 0 ? charIt.next() : null;
    let node = itRes && (itRes.done ? null : itRes.value), c = 0;
    if (node?.container.pid === this.pid && !node.hasRest) {
      while (node && c < content.length) {
        if (node.container.setIfEmpty(node.index + 1, content.charAt(c), this.tick))
          inserts[c++] = node = charIt.next().value!; // The node we just set
        else break;
      }
      content = content.slice(c);
    }
    if (content) {
      itRes = charIt.next();
      const next: TSeqCharNode | null = itRes.done ? null : itRes.value;
      const newNodes = !next || next.container.isAncestorOf(node?.container) ?
        // Append to the current
        (node ?? this).push(this.pid, content, this.tick) :
        // Prepend to the array containing the next
        next.container.unshift(this.pid, content, this.tick);
      for (let n of newNodes)
        inserts[c++] = n;
    }
    return inserts;
  }
}