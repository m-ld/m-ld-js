import { throwError } from 'rxjs';

class MeldErrorStatus {
  constructor(readonly code: number, readonly message: string) { }

  equals(status: any) {
    return status instanceof MeldErrorStatus && status.code === this.code;
  }
}

export const NONE_VISIBLE = new MeldErrorStatus(1, 'No visible clones');
export const BAD_UPDATE = new MeldErrorStatus(2, 'Bad Update');
export const HAS_UNSENT = new MeldErrorStatus(3, 'Unsent updates');
export const IS_CLOSED = new MeldErrorStatus(4, 'Clone has closed');

export class MeldError extends Error {
  constructor(readonly status: MeldErrorStatus, detail?: string) {
    super(detail);
  }
}

export function notClosed<T>(isClosed: (t: T) => boolean) {
  return {
    async: checkNotClosed(isClosed, Promise.reject),
    rx: checkNotClosed(isClosed, throwError)
  };
}

export function checkNotClosed<T>(isClosed: (t: T) => boolean, reject: (err: any) => any) {
  return function (_t: any, _p: string, descriptor: PropertyDescriptor) {
    const method = descriptor.value;
    descriptor.value = function (this: T, ...args: any[]) {
      if (isClosed(this))
        return reject(new MeldError(IS_CLOSED));
      else
        return method.apply(this, args);
    }
  }
}
