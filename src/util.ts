import { Quad } from 'rdf-js';
import { fromRDF } from 'jsonld';
import {
  concat, Observable, OperatorFunction, Subscription, throwError,
  AsyncSubject, ObservableInput, onErrorResumeNext, NEVER, from
} from "rxjs";
import { publish, tap, flatMap } from "rxjs/operators";
import AsyncLock = require('async-lock');
import { LogLevelDesc, getLogger, getLoggers } from 'loglevel';
import * as performance from 'marky';

export function flatten<T>(bumpy: T[][]): T[] {
  return ([] as T[]).concat(...bumpy);
}

export function toArray<T>(value?: T | T[]): T[] {
  return value == null ? [] : ([] as T[]).concat(value).filter(v => v != null);
}

export function fromArrayPromise<T>(reEmits: Promise<T[]>): Observable<T> {
  // Rx weirdness exemplified in 26 characters
  return from(reEmits).pipe(flatMap(from));
}

export function jsonFrom(payload: Buffer): any {
  return JSON.parse(payload.toString());
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

export function rdfToJson(quads: Quad[]): Promise<any> {
  // Using native types to avoid unexpected value objects
  return fromRDF(quads, { useNativeTypes: true });
}

export class Future<T = void> implements PromiseLike<T> {
  private readonly subject = new AsyncSubject<T>();

  constructor(value?: T) {
    if (value !== undefined) {
      this.subject.next(value);
      this.subject.complete();
    }
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

export function shortId(based: number | string = 8) {
  let genChar: () => number, len: number;
  if (typeof based == 'number') {
    let d = new Date().getTime();
    genChar = () => (d + Math.random() * 16);
    len = based;
  } else {
    let i = 0;
    genChar = () => based.charCodeAt(i++);
    len = based.length;
  }

  return ('a' + 'x'.repeat(len - 1)).replace(/[ax]/g, function (c) {
    return (genChar() % (c == 'a' ? 6 : 16) + (c == 'a' ? 10 : 0) | 0).toString(16);
  });
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

export class SharableLock extends AsyncLock {
  private readonly refs: { [key: string]: { count: number, unref: Future } } = {};

  enter(key: string): Promise<void> {
    if (key in this.refs) {
      this.refs[key].count++;
      return Promise.resolve();
    } else {
      return new Promise((resolve, reject) => {
        this.acquire(key, () => {
          const unref = new Future;
          this.refs[key] = { count: 1, unref };
          resolve(); // Return control to caller as soon as lock acquired
          return unref;
        }).catch(reject); // Should only ever be due to lock acquisition
      });
    }
  }

  leave(key: string) {
    if (key in this.refs && --this.refs[key].count == 0) {
      const unref = this.refs[key].unref;
      delete this.refs[key];
      unref.resolve();
    }
  }
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

type AsyncMethod<T> = (this: T, ...args: any[]) => Promise<any>;
type RxMethod<T> = (this: T, ...args: any[]) => Observable<any>;

export function check<T>(assertion: (t: T) => boolean, otherwise: () => Error) {
  return {
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
