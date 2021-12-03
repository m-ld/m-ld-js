import { IteratingConsumable } from './IteratingConsumable';
import { Consumable } from '../index';

export function consume<T>(iterable: Iterable<T>): Consumable<T> {
  return new IterableConsumable(iterable);
}

class IterableConsumable<T> extends IteratingConsumable<T> {
  constructor(
    private readonly iterable: Iterable<T>) {
    super();
  }

  protected start() {
    this.next(this.read());
  }

  private *read() {
    try {
      for (let value of this.iterable)
        yield value;
    } finally {
      // Important: we get here when the last value has been consumed
      this.complete();
    }
  }
}