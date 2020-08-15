import { Observable, from, defer } from 'rxjs';
import { mergeMap, map } from 'rxjs/operators';

export type LiveValue<T> = Observable<T> & { readonly value: T; };

export function liveRollup<R extends { [key: string]: unknown }>(
  liveValues: { [K in keyof R]: LiveValue<R[K]> }): LiveValue<R> {
  function current(): R;
  function current(key: keyof R, value: R[typeof key]): R;
  function current(key?: keyof R, value?: any): R {
    const partial: Partial<R> = {}
    Object.keys(liveValues).forEach((k: keyof R) =>
      partial[k] = k === key ? value : liveValues[k].value);
    return partial as R;
  }
  const values = defer(() => from(Object.keys(liveValues)).pipe(mergeMap((key: keyof R) =>
    liveValues[key].pipe(map(value => current(key, value))))));
  return Object.defineProperties(values, { value: { get: current } });
}