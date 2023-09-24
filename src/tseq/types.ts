import { TextSplice } from '../jrql-support';

/** @internal */
export type TSeqSplice = TextSplice;
/** @internal */
export namespace TSeqSplice {
  export const $index = 0, $deleteCount = 1, $content = 2;
}

/** @internal */
export type TSeqName = [pid: string, index: number];
/** @internal */
export type TSeqCharTick = [char: string, tick: number];

/** @internal */
export namespace TSeqCharTick {
  /**
   * @returns `true` if charTick2 should be applied after charTick1
   */
  export function inApplyOrder(
    charTick1: TSeqCharTick | undefined,
    charTick2: TSeqCharTick
  ) {
    if (charTick1 == null)
      return true; // Starting from nothing
    const [_, tick1] = charTick1, [char2, tick2] = charTick2;
    return char2 ?
      tick2 > tick1 : // Set always needs new tick
      tick2 >= tick1; // Delete-wins
  }
}

// NOTE Typedoc cannot cope with named tuple members
/**
 * A 'run' is a sequence of affected characters (content) at an anchor position
 * in the tree identified by a path, which is an array of names.
 * @todo array of char-ticks can be run-length compressed
 * @experimental
 * @category Experimental
 */
export type TSeqRun = [TSeqName[], TSeqCharTick[]];
/**
 * An operation against the TSeq data types comprises a set of runs.
 * @experimental
 * @category Experimental
 */
export type TSeqOperation = TSeqRun[];
/**
 * A revert of a TSeq operation encodes the prior state of each char-tick in an
 * operation. It has the same length as its corresponding operation, where each
 * index in the outer array matches an operation run. An `undefined` entry
 * indicates that no change happened for that char-tick.
 * @experimental
 * @category Experimental
 */
export type TSeqRevert = (TSeqCharTick | undefined)[][];
/**
 * Utility to determine if some run content includes any character inserts
 */
export function hasInserts(content: (TSeqCharTick | undefined)[]) {
  return content.some(charTick => charTick?.[0]);
}