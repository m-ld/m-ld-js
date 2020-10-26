import { Future } from './util';

interface SharedPromiseLike extends PromiseLike<unknown> {
  readonly pending: boolean;
  share<T>(fn: () => T | PromiseLike<T>): Promise<T>;
}

class SharedPromise extends Future implements SharedPromiseLike {
  private running: Promise<unknown> | null = null;
  private willRun: (() => Promise<unknown>)[] = [];

  async share<T>(fn: () => T | PromiseLike<T>): Promise<T> {
    if (!this.pending) {
      throw new Error('Promise not available for sharing');
    } else if (this.running != null) {
      const result = Promise.resolve(fn());
      this.setRunning([this.running, settled(result)]);
      return result;
    } else {
      return new Promise((resolve, reject) => {
        this.willRun.push(async () => {
          const result = Promise.resolve(fn());
          result.then(resolve, reject);
          return settled(result);
        });
      });
    }
  }

  start() {
    const all = this.willRun.splice(0); // Delete all
    this.setRunning(all.map(run => run()));
  }

  static resolve = (): SharedPromiseLike => ({
    pending: false,
    share: () => { throw new Error('Promise not available for sharing'); },
    then: (onfulfilled, onrejected) => Promise.resolve().then(onfulfilled, onrejected)
  })

  private setRunning(toRun: Promise<unknown>[]) {
    const run = Promise.all(toRun).then(() => {
      if (this.running === run)
        this.resolve();
    });
    this.running = run;
  }
}

export class LockManager<K extends string> {
  private head: { [key: string]: { task: SharedPromise, shared: boolean } } = {};

  async exclusive<T = void>(key: K, fn: () => T | PromiseLike<T>): Promise<T> {
    // Always wait for the current head to finish
    return this.next(key, fn, false);
  }

  async share<T = void>(key: K, fn: () => T | PromiseLike<T>): Promise<T> {
    const head = this.safeHead(key);
    // If the head is shared and not finished, share
    if (head.shared && head.task.pending) {
      return head.task.share(fn);
    } else {
      return this.next(key, fn, true);
    }
  }

  async extend<T = void>(key: K, task: Promise<T>): Promise<T> {
    // Force-share the head (will throw if not pending)
    return this.safeHead(key).task.share(() => task);
  }

  private async next<T = void>(key: K, fn: () => T | PromiseLike<T>, shared: boolean) {
    const prevTask = this.safeHead(key).task;
    const task = new SharedPromise;
    this.head[key] = { task, shared };
    const result = task.share(fn);
    await prevTask; // This await is the essence of the lock
    task.start();
    return result;
  }

  private safeHead(key: K) {
    return this.head[key] ?? SharedPromise.resolve();
  }
}

function settled(result: Promise<unknown>): Promise<unknown> {
  return new Promise(done => result.then(done, done));
}

