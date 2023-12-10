import { defer, firstValueFrom, merge, Observable } from 'rxjs';
import { map, toArray } from 'rxjs/operators';
import { Consumable, each, flow } from 'rx-flowable';
import { SubjectGraph } from './SubjectGraph';
import { GraphSubject, GraphSubjects, ReadResult } from '../api';
import { Future, tapComplete } from './Future';

export interface LiveValue<T> extends Observable<T> {
  readonly value: T;
}

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
  const values = defer(() => merge(
    ...Object.keys(liveValues)
      .map(key => liveValues[key].pipe(map(value => get(key, value)))))
  );
  return Object.defineProperties(values, { value: { get } }) as LiveValue<R>;
}

export function readResult(result: Consumable<GraphSubject>): ReadResult {
  return new class extends Observable<GraphSubject> implements ReadResult {
    readonly completed = new Future;
    // Everything should flow through this consumable so that completed is fired
    readonly consume = result.pipe(tapComplete(this.completed));

    constructor() {
      super(subs => flow(this.consume, subs));
    }

    each(handle: (value: GraphSubject) => any) {
      return each(this.consume, handle);
    }

    then: PromiseLike<GraphSubjects>['then'] =
      (onFulfilled, onRejected) =>
        firstValueFrom(this.pipe(toArray<GraphSubject>())).then(onFulfilled == null ?
          null : graph => onFulfilled(new SubjectGraph(graph)), onRejected);
  };
}