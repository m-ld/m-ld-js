import { defer, Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { inflate } from './util';

export type LiveValue<T> = Observable<T> & { readonly value: T; };

export function liveRollup<R extends { [key: string]: unknown }>(
  liveValues: { [K in keyof R]: LiveValue<R[K]> }): LiveValue<R> {
  function get(): R;
  function get(key: keyof R, value: R[typeof key]): R;
  function get(key?: keyof R, value?: any): R {
    const partial: Partial<R> = {}
    Object.keys(liveValues).forEach((k: keyof R) =>
      partial[k] = k === key ? value : liveValues[k].value);
    return partial as R;
  }
  const values = defer(() => inflate(Object.keys(liveValues), (key: keyof R) =>
    liveValues[key].pipe(map(value => get(key, value)))));
  return Object.defineProperties(values, { value: { get } }) as LiveValue<R>;
}