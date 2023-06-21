import type { TSeq } from './TSeq';

export type TSeqSplice = Parameters<TSeq['splice']>;
export namespace TSeqSplice {
  export const $index = 0, $deleteCount = 1, $content = 2;
}

export type TSeqName = [pid: string, index: number];
export type TSeqCharTick = [char: string, tick: number];

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

export type TSeqRun = [anchor: TSeqName[], content: TSeqCharTick[]];
export type TSeqOperation = TSeqRun[];
export type TSeqRevertOperation = TSeqOperation;
