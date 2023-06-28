import { Observable, throwError } from 'rxjs';
import { MeldError } from '../api';

type SyncMethod<T> = (this: T, ...args: any[]) => any;
type AsyncMethod<T> = (this: T, ...args: any[]) => Promise<any>;
type RxMethod<T> = (this: T, ...args: any[]) => Observable<any>;

export function check<T>(
  assertion: (t: T) => boolean,
  otherwise: () => Error
) {
  return {
    sync: checkWith<T, SyncMethod<T>>(assertion, otherwise, err => { throw err; }),
    async: checkWith<T, AsyncMethod<T>>(assertion, otherwise, Promise.reject.bind(Promise)),
    rx: checkWith<T, RxMethod<T>>(assertion, otherwise, throwError)
  };
}

export function checkWith<T, M extends (this: T, ...args: any[]) => any>(
  assertion: (t: T) => boolean,
  otherwise: () => Error,
  reject: (err: any) => any
) {
  return function (_t: any, _p: string, descriptor: TypedPropertyDescriptor<M>) {
    const method = <M>descriptor.value;
    descriptor.value = <M>function (this: T, ...args: any[]) {
      if (assertion(this))
        return method.apply(this, args);
      else
        return reject(otherwise());
    };
  };
}

export const checkNotClosed =
  check((m: { closed: boolean }) => !m.closed,
    () => new MeldError('Clone has closed'));
