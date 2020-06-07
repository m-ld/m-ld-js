// TODO: Fix the constants in the spec
export enum MeldErrorStatus {
  'No error' = 1,
  'Unknown error',
  'No visible clones',
  'Bad Update',
  'Bad response',
  'Request rejected',
  'Unsent updates',
  'Clone has closed',
  'Meld is offline',
  'Clone data is locked'
}

export class MeldError extends Error {
  readonly status: MeldErrorStatus;

  constructor(status: keyof typeof MeldErrorStatus | MeldErrorStatus, detail?: any) {
    super((typeof status == 'string' ? status : MeldErrorStatus[status]) + (detail != null ? `: ${detail}` : ''));
    this.status = typeof status == 'string' ? MeldErrorStatus[status] : status;
  }

  static from(err: any): MeldError {
    if (err == null)
      return new MeldError('No error');
    else if (err instanceof MeldError)
      return err;
    else if (typeof err.status == 'number' && err.status in MeldErrorStatus)
      return new MeldError(err.status, err.message);
    else
      return new MeldError('Unknown error', err.message);
  }
}
