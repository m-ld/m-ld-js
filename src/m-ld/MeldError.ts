export type MELD_ERROR = [number, string];

export const NONE_VISIBLE: MELD_ERROR = [1, 'No visible clones'];
export const BAD_UPDATE: MELD_ERROR = [2, 'Bad Update'];
export const HAS_UNSENT: MELD_ERROR = [3, 'Unsent updates'];

export class MeldError extends Error {
  constructor(readonly err: MELD_ERROR, detail?: string) {
    super(detail);
  }
}