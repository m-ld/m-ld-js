import { Future, settled } from './util';

class SharedPromise extends Future {
  private running: Promise<unknown> | null = null;
  /** The promises herein should never reject */
  private willRun: (() => Promise<unknown>)[] = [];

  get isRunning() {
    return this.pending && this.running != null;
  }

  share<T>(proc: () => T | PromiseLike<T>): Promise<T> {
    if (!this.pending) {
      return Promise.reject(new Error('Promise not available for sharing'));
    } else if (this.isRunning) {
      return this.extend(SharedPromise.exec(proc));
    } else {
      return new Promise((resolve, reject) => {
        this.willRun.push(() => {
          const result = SharedPromise.exec(proc);
          result.then(resolve, reject);
          return settled(result);
        });
      });
    }
  }

  private static exec<T>(proc: () => PromiseLike<T> | T) {
    try {
      return Promise.resolve(proc());
    } catch (e) {
      return Promise.reject(e);
    }
  }

  extend<P extends PromiseLike<any>>(result: P): P {
    this.addRunning(settled(result));
    return result;
  }

  start() {
    const all = this.willRun.splice(0); // Delete all
    // This allows for a recursive synchronous share in one of the runs
    this.running = Promise.resolve();
    if (all.length === 0)
      this.resolve();
    else
      all.forEach(run => this.addRunning(run()));
  }

  private addRunning(toRun: Promise<unknown>) {
    const run = Promise.all([this.running, toRun]).finally(() => {
      if (this.running === run)
        this.resolve();
    });
    this.running = run;
  }
}

export class LockManager<K extends string = string> {
  private locks: {
    [key: string]: {
      /** Currently running task, may be extended */
      running: SharedPromise,
      /** Head task, may be shared if not exclusive */
      head: SharedPromise,
      /** Whether the head task is exclusive */
      exclusive: boolean
    }
  } = {};

  /**
   * Get the current state of the given lock.
   * - `'open'` means the lock is immediately available
   * - `'exclusive'` means newly scheduled tasks will execute when the lock opens
   * - `'shared'` means tasks will be shared when any scheduled exclusive tasks complete
   */
  state(key: K) {
    const lock = this.locks[key];
    return lock == null ? 'open' :
      lock.exclusive ? 'exclusive' : 'shared';
  }

  /**
   * Resolves when the lock is immediately available. Used for indication and
   * tests. In normal usage, {@link share} and {@link exclusive} are used for
   * scheduling.
   */
  async open(key: K) {
    if (this.locks[key] != null)
      await this.locks[key].head;
  }

  /**
   * Acquires the lock. Note that this method requires the caller to handle
   * errors to ensure the lock is not permanently stuck. If possible, prefer the
   * use of {@link share} or {@link exclusive}.
   *
   * @returns a promise that resolves when the lock is acquired,
   * providing a function to release it.
   */
  async acquire(key: K, mode: 'share' | 'exclusive'): Promise<() => void> {
    const running = new Future<() => void>();
    this[mode](key, () => {
      const done = new Future;
      running.resolve(done.resolve);
      return done;
    });
    return running;
  }

  /**
   * Schedules an exclusive task on the given lock. This task will execute when
   * any running task has completed.
   */
  exclusive<T = void>(key: K, proc: () => T | PromiseLike<T>): Promise<T> {
    // Always wait for the current head to finish
    return this.next(key, proc, true);
  }

  /**
   * Schedules a shared task on the given lock. Sharing will only occur with the
   * latest scheduled task, which may or may not be running, and only if that
   * task is shared; otherwise, this task will be appended to the queue.
   */
  share<T = void>(key: K, proc: () => T | PromiseLike<T>): Promise<T> {
    const lock = this.locks[key];
    // If the head is shared and not finished, share
    if (lock != null && !lock.exclusive && lock.head.pending) {
      return lock.head.share(proc);
    } else {
      return this.next(key, proc, false);
    }
  }

  /**
   * Extends a running task to encompass the given task. This should only be
   * called from code whose precondition is an existing lock.
   *
   * @returns a promise which settles when the task completes
   * @throws {RangeError} if the lock is not currently held
   */
  extend<P extends PromiseLike<any>>(key: K, task: P): P {
    const lock = this.locks[key];
    if (lock == null || lock.running == null)
      throw new RangeError(`${key} not available for sharing`);
    // Forcing a share even if the lock is exclusive
    return lock.running.extend(task);
  }

  private async next<T = void>(key: K, proc: () => T | PromiseLike<T>, exclusive: boolean) {
    const prevTask = this.locks[key]?.head;
    const task = new SharedPromise;
    // Don't change the running task
    this.locks[key] = { ...this.locks[key], head: task, exclusive };
    task.then(() => {
      // If we're the last in the queue, delete ourselves
      if (this.locks[key]?.head === task)
        delete this.locks[key];
    });
    const result = task.share(proc);
    await prevTask; // This wait is the essence of the lock
    this.locks[key].running = task;
    task.start();
    return result;
  }
}
