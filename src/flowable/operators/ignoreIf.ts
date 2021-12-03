import { OperatorFunction } from 'rxjs';
import { filter } from 'rxjs/operators';
import { Bite } from '../index';

/**
 * Consumable (inverse) flavour of `filter` which calls {@link Bite.next()}
 * for ignored values to prevent consumption from stalling.
 */
export function ignoreIf<T>(
  predicate: (value: T) => boolean): OperatorFunction<Bite<T>, Bite<T>>;
export function ignoreIf<T>(
  predicate: null): OperatorFunction<Bite<T>, Bite<Exclude<T, null | undefined>>>;
export function ignoreIf<T>(
  predicate: ((value: T) => boolean) | null): OperatorFunction<Bite<T>, Bite<T>> {
  const p = predicate ?? (value => value == null);
  return filter(({ value, next }) => !p(value) || !next());
}