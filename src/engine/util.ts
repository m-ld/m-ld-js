import {
  BehaviorSubject, concat, connect, defaultIfEmpty, EMPTY, firstValueFrom, from, NEVER, Observable,
  ObservableInput, ObservedValueOf, Observer, onErrorResumeNext, OperatorFunction, Subject,
  Subscription
} from 'rxjs';
import { mergeMap, switchAll } from 'rxjs/operators';

export const isArray = Array.isArray;

export function flatten<T>(bumpy: T[][]): T[] {
  return ([] as T[]).concat(...bumpy);
}

/**
 * Sugar for the common pattern of taking an observable input (typically a Promise) and 'inflating'
 * its result (or each of its results) into an observable output.
 * @param input a source of stuff to inflate
 * @param pump the map function that inflates the input to an observable output
 */
export function inflate<T extends ObservableInput<any>, O extends ObservableInput<any>>(
  input: O,
  pump: (p: ObservedValueOf<O>) => T
): Observable<ObservedValueOf<T>> {
  return from(input).pipe(mergeMap(pump));
}

export function inflateFrom<T>(inputOfInput: ObservableInput<ObservableInput<T>>): Observable<T> {
  return inflate(inputOfInput, from);
}

export function toJSON(thing: any): any {
  if (thing == null)
    return null;
  else if (typeof thing.toJSON == 'function')
    return thing.toJSON();
  else if (thing instanceof Error)
    return { ...thing, message: thing.message };
  else
    return thing;
}

export function settled(result: PromiseLike<unknown>): Promise<unknown> {
  return new Promise(done => result.then(done, done));
}

export function completed(observable: Observable<unknown>): Promise<void> {
  return new Promise((resolve, reject) =>
    observable.subscribe({ complete: resolve, error: reject }));
}

/**
 * Delays notifications from a source until a signal is received from a notifier.
 * @see https://ncjamieson.com/how-to-write-delayuntil/
 */
export function delayUntil<T>(notifier: ObservableInput<unknown>): OperatorFunction<T, T> {
  return source =>
    source.pipe(
      connect(published => {
        const delayed = new Observable<T>(subscriber => {
          let buffering = true;
          const buffer: T[] = [];
          const subscription = new Subscription();
          subscription.add(
            from(notifier).subscribe({
              next() {
                buffer.forEach(value => subscriber.next(value));
                subscriber.complete();
              },
              error(err) {
                subscriber.error(err);
              },
              complete() {
                buffering = false;
                buffer.length = 0;
              }
            }));
          subscription.add(
            published.subscribe({
              next(value) {
                if (buffering)
                  buffer.push(value);
              },
              error(err) {
                subscriber.error(err);
              }
            }));
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

export function first<T>(src: Observable<T>): Promise<T | undefined> {
  return firstValueFrom(src.pipe(defaultIfEmpty(undefined)));
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

  close() {
    this.switch(EMPTY);
    this.in.complete();
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

export function poisson(mean: number) {
  const threshold = Math.exp(-mean);
  let rtn = 0;
  for (let p = 1.0; p > threshold; p *= Math.random())
    rtn++;
  return rtn - 1;
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

export function binaryFold<T, R>(
  input: T[],
  map: (t: T) => R,
  fold: (r1: R, r2: R) => R
): R | null {
  return input.reduce<R | null>((r1, t) => {
    const r2 = map(t);
    return r1 == null ? r2 : fold(r1, r2);
  }, null);
}

export function mapObject(
  o: {}, fn: (k: string, v: any) => { [key: string]: any } | undefined): { [key: string]: any } {
  return Object.assign({}, ...Object.entries(o).map(([k, v]) => fn(k, v)));
}

export function *mapIter<T, R>(it: Iterable<T>, fn: (v: T) => R): Iterable<R> {
  for (let v of it)
    yield(fn(v));
}

export function *deepValues(
  o: any,
  filter: (o: any, path: string[]) => boolean = o => typeof o != 'object',
  path: string[] = []
): IterableIterator<[string[], any]> {
  if (filter(o, path))
    yield [path, o];
  else if (typeof o == 'object')
    for (let key of Object.keys(o))
      yield *deepValues(o[key], filter, path.concat(key));
}

export function setAtPath<T>(o: any, path: string[], value: T,
  createAt: (path: string[]) => any = path => {
    throw `nothing at ${path}`;
  },
  start = 0
): T {
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

export const isNaturalNumber = (n: any) =>
  typeof n == 'number' && Number.isSafeInteger(n) && n >= 0;