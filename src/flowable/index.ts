import { Observable, Subscriber, Subscription } from 'rxjs';

/**
 * A consumable is an observable that is
 * - always in pull/paused/consuming mode (cf. {@link Flowable})
 * - hot if there is at least one subscriber (not represented in the typing)
 *
 * This is an interface to ease the use of RxJS pipe operators and utilities
 * like `EMPTY`. However note that some (e.g. `from` and `merge`)
 * may create misbehaving consumables that flow with incorrect backpressure.
 *
 * @see Bite
 */
export interface Consumable<T> extends Observable<Bite<T>> {
  /** @deprecated use {@link each} */
  forEach(next: (value: Bite<T>) => void): Promise<void>;
}

/**
 * Replacement for {@link Observable.forEach}, which consumes each item one-by-one
 * @param consumable the consumable to consume
 * @param handle a handler for each item consumed
 */
export function each<T>(
  consumable: Consumable<T>, handle: (value: T) => any): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let subscription: Subscription;
    subscription = consumable.subscribe({
      next: async ({ value, next }) => {
        try {
          await handle(value);
          next();
        } catch (err) {
          reject(err);
          subscription?.unsubscribe();
        }
      },
      error: reject,
      complete: resolve
    });
  });
}

/**
 * One value being processed by a consumer. On receipt of a bite, every consumer
 * must either call {@link next} or unsubscribe, to allow the further items to
 * arrive (to other subscribers). Therefore, a consumable always flows at the
 * pace of he slowest consumer.
 *
 * When piping a consumable, it's essential to call done() for every item
 * which does not make it to the output, for example due to a filter or reduce.

 * @see Consumable
 */
export interface Bite<T> {
  value: T;
  /**
   * Call to release the next value for consumption.
   * @returns true for expressions like `next() && 'hello'`
   */
  next(): true;
}

/**
 * A flowable can be in pull/paused ("consuming") mode or push/flowing mode, like a stream.
 * The mode is initially decided by which `subscribe()` method is called first:
 * - If `Observable.subscribe()` is called first, the mode is flowing
 * - If `Flowable.consume.subscribe()` is called first, the mode is consuming
 *
 * A subscriber via `Observable.subscribe()` always receives all data, but it
 * may be delayed by any subscribed consumers (like 'data' events).
 *
 * A flowable is always multicast, and hot if there is at least one subscriber.
 *
 * @see https://nodejs.org/api/stream.html#two-reading-modes
 */
export interface Flowable<T> extends Observable<T> {
  readonly consume: Consumable<T>;
}

/**
 * Duck-typing an observable to see if it supports backpressure.
 */
export function isFlowable<T>(observable: Observable<T>): observable is Flowable<T> {
  return 'consume' in observable;
}

/**
 * Creates a flowable from some source consumable.
 */
export function flowable<T>(source: Consumable<T>): Flowable<T> {
  return new class extends Observable<T> implements Flowable<T> {
    readonly consume = source;
    constructor() {
      /* The observable (push) world just calls done for every item.
      Since the consumable flows at the pace of the slowest consumer,
      a subscriber to the source consumable will delay items. */
      super(subs => flow(source, subs));
    }
  }();
}

/**
 * Flows the given consumable to the subscriber with no back-pressure.
 * @see Flowable
 */
export function flow<T>(consumable: Consumable<T>, subs: Subscriber<T>) {
  return consumable.subscribe({
    next({ value, next }) {
      subs.next(value);
      next();
    },
    complete: () => subs.complete(),
    error: err => subs.error(err)
  });
}