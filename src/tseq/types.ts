import type { TSeq } from './TSeq';

/** @internal */
export type TSeqSplice = Parameters<TSeq['splice']>;
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
 * A revert of a TSeq operation has the same structure as an operation itself,
 * but note that it cannot be applied because it will generally identify clock
 * ticks in the past, and so be ignored.
 * @experimental
 * @category Experimental
 */
export type TSeqRevertOperation = TSeqOperation;
