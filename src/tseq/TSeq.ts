import { concatIter } from '../engine/util';
import { TSeqCharNode, TSeqNode } from './TSeqNode';
import { TSeqOperable } from './TSeqOperable';
import { TSeqCharTick, TSeqOperation, TSeqRevertOperation, TSeqSplice } from './types';

/**
 * Result of a pre-apply step during {@link TSeq#apply}.
 * @internal
 */
export interface TSeqPreApply {
  node: TSeqCharNode;
  charTick: TSeqCharTick;
  charIndex: number;
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
   * Applies remote operations to this local clone of the TSeq
   * @param operations the operations to apply
   * @param cb a callback to recover reversion information if required
   */
  apply(operations: TSeqOperation, cb?: (revert: TSeqRevertOperation) => void) {
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
    const revert = nodePrior ? TSeqOperable.toRevertOps(nodePrior) : [];
    for (let { node } of nodePreApply)
      node.container.gc(node);
    cb?.(revert);
    return splices;
  }

  private addToSplices(splices: TSeqSplice[], charIndex: number, char: string) {
    let last = splices[splices.length - 1];
    if (last != null && charIndex < last[TSeqSplice.$index])
      throw new RangeError('Expecting spliced chars in order');
    if (last == null || charIndex > last[TSeqSplice.$index] + last[TSeqSplice.$deleteCount])
      splices.push(last = [charIndex, 0, '']);
    last[TSeqSplice.$deleteCount] -= char.length - 1;
    last[TSeqSplice.$content] += char;
  }

  /**
   * Changes the text content of this TSeq by removing or replacing existing
   * characters and/or adding new characters in place.
   * @param index zero-based index at which to start changing the text
   * @param deleteCount the number of characters in the text to remove from `index`
   * @param content the characters to add to the text, beginning from `index`
   */
  splice(index: number, deleteCount: number, content = ''): TSeqOperation {
    if (index < 0)
      throw new RangeError();
    if (!deleteCount && !content.length)
      return []; // Shortcut
    const deletes = this.delete(index, deleteCount);
    const inserts = this.insert(index, content);
    // TODO: Supply the reverts to a callback, as per the apply method
    // TODO: This reconstructs runs which are already about known in the insert
    // method, combining them with the deletes
    const ops = [...TSeqOperable.toRuns(concatIter(deletes, inserts))];
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
        if (node.container.setIfEmpty(node.index + 1, content.charAt(c), this.tick))
          inserts[c++] = node = charIt.next().value!; // The node we just set
        else
          break;
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
