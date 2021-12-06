import { Observable, Subscriber } from 'rxjs';
import { Bite, Consumable } from '../index';

export abstract class IteratingConsumable<T> extends Observable<Bite<T>> implements Consumable<T> {
  /** Can be sparse when subscribers unsubscribe */
  subscribers: Subscriber<Bite<T>>[] = [];
  /** Final state. `undefined` is not yet known, `true` is complete */
  final?: Error | true;

  protected constructor() {
    super(function (this: IteratingConsumable<T>, subs) {
      if (this.final === true) {
        subs.complete();
      } else if (this.final != null) {
        subs.error(this.final);
      } else {
        const index = this.addSubscriber(subs);
        return () => this.removeSubscriber(index);
      }
    });
  }

  /**
   * @param values from the source. If empty, this means no values are
   * available right now, not necessarily that the source is exhausted.
   */
  protected next = (values: Iterator<T>) => {
    // Check if we have been finalised
    if (this.final == null) {
      const { value, done } = values.next();
      if (!done)
        this.emitNext(value, values);
    }
  };

  private emitNext(value: any, values: Iterator<T>) {
    // RxJS is not happy with subscribers synchronously triggering the next item
    let sync = true, deferSyncNext = false;
    // Wait for every subscriber to process this item
    const waitingFor = new Array<Subscriber<Bite<T>>>(this.subscribers.length);
    const release = (i: number) => {
      if (waitingFor[i]) { // Just in case unsubscribe and next both called
        delete waitingFor[i];
        if (sparseEmpty(waitingFor)) {
          if (sync)
            deferSyncNext = true;
          else
            this.next(values); // Read another item
        }
      }
    };
    this.subscribers.forEach((subs, i) => {
      waitingFor[i] = subs;
      // A subscriber may close while we're processing this item, add a teardown
      subs.add(() => release(i));
    });
    waitingFor.forEach((subs, i) => {
      subs.next({
        value, next: () => {
          release(i);
          return true;
        }
      });
    });
    sync = false;
    if (deferSyncNext)
      this.next(values); // Read another item
  }

  /** Call from subclass when the source is exhausted */
  protected complete = () => {
    if (this.final == null) {
      this.final = true;
      this.subscribers.forEach(subs => subs.complete());
    }
  };

  /** Call from subclass when the source errors */
  protected error = (err: Error) => {
    if (this.final == null) {
      this.final = err;
      this.subscribers.forEach(subs => subs.error(err));
    }
  };

  private addSubscriber = (subs: Subscriber<Bite<T>>) => {
    const index = this.subscribers.push(subs) - 1;
    if (index === 0)
      this.start();
    return index;
  };

  private removeSubscriber = (index: number) => {
    delete this.subscribers[index];
    // If there are no subscribers left, remove our listeners
    if (sparseEmpty(this.subscribers)) {
      this.stop();
      // Safe to truncate
      this.subscribers.length = 0;
    }
  };

  /**
   * Called when no-one is consuming any more; tidy up state.
   */
  protected stop() {
  }

  /**
   * Called when the first consumer arrives; start iteration.
   * When values become available, the subclass should call {@link next()}.
   * When the source is exhausted, the subclass should call {@link complete()}.
   */
  protected abstract start(): void;
}

/**
 * @return true if the given array has only empty slots
 */
function sparseEmpty(array: Exclude<any, null | undefined>[]) {
  return array.every(value => value == null);
}