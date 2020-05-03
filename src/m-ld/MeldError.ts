class MeldErrorStatus {
  constructor(readonly code: number, readonly message: string) { }

  equals(status: any) {
    return status instanceof MeldErrorStatus && status.code === this.code;
  }
}

export const NONE_VISIBLE = new MeldErrorStatus(1, 'No visible clones');
export const BAD_UPDATE = new MeldErrorStatus(2, 'Bad Update');
export const HAS_UNSENT = new MeldErrorStatus(3, 'Unsent updates');

export class MeldError extends Error {
  constructor(readonly status: MeldErrorStatus, detail?: string) {
    super(detail);
  }
}