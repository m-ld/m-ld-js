import { concatMap, OperatorFunction, pipe, reduce, windowCount } from 'rxjs';
import { ignoreIf } from './ignoreIf';
import { Bite } from '../index';

export function batch<T>(size: number): OperatorFunction<Bite<T>, Bite<T[]>> {
  return pipe(
    windowCount(size),
    concatMap(window => window.pipe(
      reduce((batchBite, { value, next }, index) => {
        batchBite.value.push(value);
        if (index < size - 1)
          next();
        else
          batchBite.next = next;
        return batchBite;
      }, { value: [], next: () => true } as Bite<T[]>),
      ignoreIf(batch => !batch.length))));
}