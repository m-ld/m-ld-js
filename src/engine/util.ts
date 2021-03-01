import {
  concat, Observable, OperatorFunction, Subscription, throwError,
  AsyncSubject, ObservableInput, onErrorResumeNext, NEVER, from, BehaviorSubject, Subject, Observer
} from "rxjs";
import { publish, tap, mergeMap, switchAll, scan, endWith, pluck, filter } from "rxjs/operators";
import { LogLevelDesc, getLogger, getLoggers } from 'loglevel';
import * as performance from 'marky';
import { encode as rawEncode, decode as rawDecode } from '@ably/msgpack-js';
import { EventEmitter } from 'events';

export namespace MsgPack {
  export const encode = (value: any) => Buffer.from(rawEncode(value).buffer);
  export const decode = (buffer: ArrayBuffer) => rawDecode(Buffer.from(buffer));
}

export function flatten<T>(bumpy: T[][]): T[] {
  return ([] as T[]).concat(...bumpy);
}

export function fromArrayPromise<T>(promise: Promise<T[]>): Observable<T> {
  // Rx weirdness exemplified in 26 characters
  return from(promise).pipe(mergeMap(from));
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

export function settled(result: PromiseLike<unknown>): Promise<unknown> {
  return new Promise(done => result.then(done, done));
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

export function toArrays<T>(groupBy: (value: T) => unknown): OperatorFunction<T, T[]> {
  return source => source.pipe(
    endWith(null),
    scan<T, { out: T[] | null, buf: T[] }>(({ buf }, value) => {
      if (value == null) // Terminator
        return { out: buf.length ? buf : null, buf: [] };
      else if (!buf.length) // First value
        return { out: null, buf: [value] };
      else if (groupBy(buf[buf.length - 1]) !== groupBy(value))
        return { out: buf, buf: [value] };
      else
        return { out: null, buf: [...buf, value] };
    }, { out: null, buf: [] }),
    pluck('out'),
    filter<T[]>(out => out != null)
  );
}

/**
 * Delays notifications from a source until a signal is received from a notifier.
 * @see https://ncjamieson.com/how-to-write-delayuntil/
 */
export function delayUntil<T>(notifier: ObservableInput<unknown>): OperatorFunction<T, T> {
  return source =>
    source.pipe(
      publish(published => {
        const delayed = new Observable<T>(subscriber => {
          let buffering = true;
          const buffer: T[] = [];
          const subscription = new Subscription();
          subscription.add(
            from(notifier).subscribe(
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

export function onErrorNever<T>(v: ObservableInput<T>): Observable<T> {
  return onErrorResumeNext(v, NEVER);
}

export function observeStream<T>(startStream: () => Promise<EventEmitter>): Observable<T> {
  return new Observable<T>(subs => {
    const subscription = new Subscription;
    startStream().then(stream => {
      if (!subscription.closed) {
        subscription.add(() => {
          const maybeAsyncIterator = <any>stream;
          if (maybeAsyncIterator != null &&
            typeof maybeAsyncIterator.close == 'function' &&
            !maybeAsyncIterator.ended)
            (<any>stream).close();
        });
        stream
          .on('data', datum => {
            if (!subscription.closed)
              subs.next(datum);
          })
          .on('error', err => subs.error(err))
          .on('end', () => subs.complete());
      }
    }).catch(err => subs.error(err));
    return subscription;
  });
}

export class HotSwitch<T> extends Observable<T> {
  private readonly in: BehaviorSubject<Observable<T>>;

  constructor(position: Observable<T> = NEVER) {
    super(subs => this.in.pipe(switchAll()).subscribe(subs));
    this.in = new BehaviorSubject<Observable<T>>(position);
  }

  switch(to: Observable<T>) {
    this.in.next(to);
  }
}

export class PauseableSource<T> extends Observable<T> implements Observer<T> {
  private readonly subject = new Subject<T>();
  private readonly switch = new HotSwitch<T>(this.subject);

  constructor() {
    super(subs => this.switch.subscribe(subs));
  }

  get closed() {
    return this.subject.closed;
  };

  next = (value: T) => this.subject.next(value);
  error = (err: any) => this.subject.error(err);
  complete = () => this.subject.complete();

  pause(until: ObservableInput<unknown>) {
    this.switch.switch(this.subject.pipe(delayUntil(until)));
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

export function poisson(mean: number) {
  const threshold = Math.exp(-mean);
  let rtn = 0;
  for (let p = 1.0; p > threshold; p *= Math.random())
    rtn++;
  return rtn - 1;
}

export function memoise<K extends object, V extends {}>(fn: (key: K) => V): (key: K) => V {
  const cache = new WeakMap<K, V>();
  return key => {
    let value = cache.get(key);
    if (value == null)
      cache.set(key, value = fn(key));
    return value;
  };
}

export function lazy<V>(create: (key: string) => V):
  ((key: string) => V) & Iterable<V> {
  const cache: { [key: string]: V } = {};
  return Object.assign(
    (key: string) => cache[key] ??= create(key),
    { [Symbol.iterator]: () => Object.values(cache)[Symbol.iterator]() });
}

export function minIndexOfSparse<T>(arr: T[]) {
  let min: number | undefined;
  // some() skips empty array positions
  arr.some((_, i) => (min = i) != null);
  return min;
}

export async function asyncBinaryFold<T, R>(
  input: T[],
  map: (t: T) => R | Promise<R>,
  fold: (r1: R, r2: R) => R | Promise<R>): Promise<R | null> {
  return input.reduce<Promise<R | null>>(async (r, t) => {
    const r1 = await r;
    const r2 = await map(t);
    return r1 == null ? r2 : await fold(r1, r2);
  }, Promise.resolve(null));
}

export function mapObject(
  o: {}, fn: (k: string, v: any) => { [key: string]: any } | undefined): { [key: string]: any } {
  return Object.assign({}, ...Object.entries(o).map(([k, v]) => fn(k, v)));
}

export function* deepValues(o: any,
  filter: (o: any, path: string[]) => boolean = o => typeof o != 'object',
  path: string[] = []): IterableIterator<[string[], any]> {
  if (filter(o, path))
    yield [path, o];
  else if (typeof o == 'object')
    for (let key in o)
      yield *deepValues(o[key], filter, path.concat(key));
}

export function setAtPath(o: any, path: string[], value: any,
  createAt: (path: string[]) => any = path => { throw `nothing at ${path}`; }, start = 0) {
  if (path.length > start)
    if (path.length - start === 1)
      o[path[start]] = value; // no-op for primitives, throws for null/undefined
    else
      setAtPath(o[path[start]] ??= createAt(path.slice(0, start + 1)),
        path, value, createAt, start + 1);
  return value;
}

export function trimTail<T>(arr: T[]): T[] {
  while (arr[arr.length - 1] == null)
    arr.length--;
  return arr;
}
