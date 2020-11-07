import {
  concat, Observable, OperatorFunction, Subscription, throwError,
  AsyncSubject, ObservableInput, onErrorResumeNext, NEVER, from
} from "rxjs";
import { publish, tap, mergeMap } from "rxjs/operators";
import { LogLevelDesc, getLogger, getLoggers } from 'loglevel';
import * as performance from 'marky';
import { encode as rawEncode, decode as rawDecode } from '@ably/msgpack-js';

export namespace MsgPack {
  export const encode = (value: any) => Buffer.from(rawEncode(value).buffer);
  export const decode = (buffer: ArrayBuffer) => rawDecode(Buffer.from(buffer));
}

export function flatten<T>(bumpy: T[][]): T[] {
  return ([] as T[]).concat(...bumpy);
}

export function fromArrayPromise<T>(reEmits: Promise<T[]>): Observable<T> {
  // Rx weirdness exemplified in 26 characters
  return from(reEmits).pipe(mergeMap(from));
}

export function toJson(thing: any): any {
  if (thing == null)
    return null;
  else if (typeof thing.toJson == 'function')
    return thing.toJson();
  else if (thing instanceof Error)
    return { ...thing, message: thing.message };
  else
    return thing;
}

export class Future<T = void> implements PromiseLike<T> {
  private readonly subject = new AsyncSubject<T>();

  constructor(value?: T) {
    if (value !== undefined) {
      this.subject.next(value);
      this.subject.complete();
    }
  }

  get pending() {
    return !this.subject.isStopped;
  }

  get settle() {
    return [this.resolve, this.reject];
  }

  resolve = (value: T) => {
    this.subject.next(value);
    this.subject.complete();
  }

  reject = (err: any) => {
    this.subject.error(err);
  }

  then: PromiseLike<T>['then'] = (onfulfilled, onrejected) => {
    return this.subject.toPromise().then(onfulfilled, onrejected);
  }
}

export function tapCount<T>(done: Future<number>): OperatorFunction<T, T> {
  let n = 0;
  return tap({
    next: () => n++,
    complete: () => done.resolve(n),
    error: done.reject
  });
}

export function tapLast<T>(done: Future<T | undefined>): OperatorFunction<T, T> {
  let last: T | undefined;
  return tap({
    next: item => { last = item; },
    complete: () => done.resolve(last),
    error: done.reject
  });
}

export function tapComplete<T>(done: Future): OperatorFunction<T, T> {
  return tap({ complete: () => done.resolve(), error: done.reject });
}

/**
 * Delays notifications from a source until a signal is received from a notifier.
 * @see https://ncjamieson.com/how-to-write-delayuntil/
 */
export function delayUntil<T>(notifier: Observable<any>): OperatorFunction<T, T> {
  return source =>
    source.pipe(
      publish(published => {
        const delayed = new Observable<T>(subscriber => {
          let buffering = true;
          const buffer: T[] = [];
          const subscription = new Subscription();
          subscription.add(
            notifier.subscribe(
              () => {
                buffer.forEach(value => subscriber.next(value));
                subscriber.complete();
              },
              error => subscriber.error(error),
              () => {
                buffering = false;
                buffer.length = 0;
              }
            )
          );
          subscription.add(
            published.subscribe(
              value => buffering && buffer.push(value),
              error => subscriber.error(error)
            )
          );
          subscription.add(() => {
            buffer.length = 0;
          });
          return subscription;
        });
        return concat(delayed, published);
      })
    );
}

export function onErrorNever<T, R>(v: ObservableInput<T>): Observable<R> {
  return onErrorResumeNext(v, NEVER);
}

export function getIdLogger(ctor: Function, id: string, logLevel: LogLevelDesc = 'info') {
  const loggerName = `${ctor.name}.${id}`;
  const loggerInitialised = loggerName in getLoggers();
  const log = getLogger(loggerName);
  if (!loggerInitialised) {
    const originalFactory = log.methodFactory;
    log.methodFactory = (methodName, logLevel, loggerName) => {
      const method = originalFactory(methodName, logLevel, loggerName);
      return (...msg: any[]) => method.apply(undefined, [new Date().toISOString(), id, ctor.name].concat(msg));
    };
  }
  log.setLevel(logLevel);
  return log;
}

type SyncMethod<T> = (this: T, ...args: any[]) => any;
type AsyncMethod<T> = (this: T, ...args: any[]) => Promise<any>;
type RxMethod<T> = (this: T, ...args: any[]) => Observable<any>;

export function check<T>(assertion: (t: T) => boolean, otherwise: () => Error) {
  return {
    sync: checkWith<T, SyncMethod<T>>(assertion, otherwise, err => { throw err; }),
    async: checkWith<T, AsyncMethod<T>>(assertion, otherwise, Promise.reject.bind(Promise)),
    rx: checkWith<T, RxMethod<T>>(assertion, otherwise, throwError)
  };
}

export function checkWith<T, M extends (this: T, ...args: any[]) => any>(
  assertion: (t: T) => boolean, otherwise: () => Error, reject: (err: any) => any) {
  return function (_t: any, _p: string, descriptor: TypedPropertyDescriptor<M>) {
    const method = <M>descriptor.value;
    descriptor.value = <M>function (this: T, ...args: any[]) {
      if (assertion(this))
        return method.apply(this, args);
      else
        return reject(otherwise());
    };
  }
}

export class Stopwatch {
  readonly name: string;
  lap: Stopwatch;
  laps: { [name: string]: Stopwatch } = {};
  entry: PerformanceEntry | undefined;

  constructor(
    scope: string, name: string) {
    performance.mark(this.name = `${scope}-${name}`);
    this.lap = this;
  }

  next(name: string): Stopwatch {
    this.lap.stop();
    this.lap = this.laps[name] = new Stopwatch(this.name, name);
    return this;
  }

  stop(): PerformanceEntry {
    if (this.lap !== this)
      this.lap.stop();
    return this.entry = performance.stop(this.name);
  }
}
