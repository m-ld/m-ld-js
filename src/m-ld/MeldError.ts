import { MeldErrorStatus } from '@m-ld/m-ld-spec';

// Errors are used unchanged form m-ld-spec
export { MeldErrorStatus };

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
