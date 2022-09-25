import { AsyncSubject, firstValueFrom, OperatorFunction } from 'rxjs';
import { tap } from 'rxjs/operators';

export class Future<T = void> implements PromiseLike<T> {
  private readonly subject = new AsyncSubject<T>();
  private _pending = true;
  private _promise: Promise<T>;

  constructor(value?: T) {
    this._promise = firstValueFrom(this.subject);
    this._promise.catch(() => {}); // Suppress UnhandledPromiseRejection
    if (value !== undefined)
      this.resolve(value);
  }

  get pending() {
    return this._pending;
  }

  get settle() {
    return [this.resolve, this.reject];
  }

  resolve = (value: T) => {
    this._pending = false;
    this.subject.next(value);
    this.subject.complete();
  };

  reject = (err: any) => {
    this._pending = false;
    this.subject.error(err);
  };

  then: Promise<T>['then'] = (onfulfilled, onrejected) => {
    return this._promise.then(onfulfilled, onrejected);
  };
}

export function tapLast<T>(done: Future<T | undefined>): OperatorFunction<T, T> {
  let last: T | undefined;
  return tap({
    next: item => { last = item; },
    complete: () => done.resolve(last),
    error: done.reject
  });
}

/**
 * CAUTION: the future will not be resolved if the subscriber unsubscribes.
 * To capture unsubscription, use the RxJS `finalize` operator.
 */
export function tapComplete<T>(done: Future): OperatorFunction<T, T> {
  return tap({ complete: () => done.resolve(), error: done.reject });
}