import { concatMap, OperatorFunction, tap } from 'rxjs';
import { Bite, Consumable } from '../index';

/**
 * Consumable flavour of merge/concat mapping – always concatenates.
 */
export function flatMap<T, R>(
  project: (value: T) => Consumable<R>): OperatorFunction<Bite<T>, Bite<R>> {
  return concatMap(({ value, next }) => project(value)
    .pipe(tap({ complete: next })));
}