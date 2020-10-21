import AsyncLock = require('async-lock');
import { Future } from './util';

// TODO: Use this instead of AsyncLock
export class LockManager<K extends string> {
  private locks: { [key: string]: Promise<unknown> } = {};
  
  request<T = void>(key: K, fn: () => T | PromiseLike<T>): Promise<T> {
    const result = (this.locks[key] ?? Promise.resolve()).then(fn);
    this.locks[key] = new Promise(done => result.finally(done));
    return result;
  }
}

export class SharableLock<K extends string> {
  private asyncLock = new AsyncLock();
  private readonly active: {
    [key: string]: { count: number; done: Future; shared: boolean; };
  } = {};

  exclusive<T = void>(key: K, fn: () => T | PromiseLike<T>): Promise<T> {
    return this.enter(key, false).then(fn).finally(() => this.leave(key));
  }

  share<T = void>(key: K, fn: () => T | PromiseLike<T>): Promise<T> {
    return this.enter(key).then(fn).finally(() => this.leave(key));
  }

  extend(key: K, task: Promise<unknown>) {
    if (key in this.active) {
      this.active[key].count++;
      task.finally(() => this.leave(key));
    } else {
      throw new Error('Cannot extend if not locked');
    }
  }

  enter(key: K, shared: boolean = true): Promise<void> {
    if (key in this.active && shared && this.active[key].shared) {
      this.active[key].count++;
      return Promise.resolve();
    } else {
      return new Promise((resolve, reject) => {
        this.asyncLock.acquire(key, () => {
          const done = new Future;
          this.active[key] = { count: 1, done, shared };
          resolve(); // Return control to caller as soon as lock acquired
          return done;
        }).catch(reject); // Should only ever be due to lock acquisition
      });
    }
  }

  leave(key: K) {
    if (key in this.active && --this.active[key].count == 0) {
      const done = this.active[key].done;
      delete this.active[key];
      done.resolve(); // Exits the async lock
    }
  }
}
