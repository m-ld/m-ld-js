import { IteratingConsumable } from './IteratingConsumable';
import { Consumable } from '../index';

export function consume<T>(promise: Promise<T>): Consumable<T> {
  return new PromiseConsumable(promise);
}

class PromiseConsumable<T> extends IteratingConsumable<T> {
  constructor(
    private readonly promise: Promise<T>) {
    super();
  }

  protected start() {
    this.promise.then(value => {
      this.next(this.yieldValue(value));
    }, this.error);
  }

  private *yieldValue(value: T) {
    try {
      yield value;
    } finally {
      // Important: we get here when the value has been consumed
      this.complete();
    }
  }
}