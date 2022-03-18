import { Future, settled } from './util';

interface Task {
  name: string;
  /** Must never reject */
  run: () => Promise<unknown>;
}

export type SharedProc<T, P extends SharedPromise<unknown>> = (this: P) => (PromiseLike<T> | T);

/**
 * TODO docs
 */
export class SharedPromise<R> extends Future {
  private running: Promise<unknown> | null = null;
  private readonly tasks: Task[] = [];
  readonly result: Promise<R>;

  constructor(name: string, proc: SharedProc<R, SharedPromise<R>>) {
    super();
    this.result = this.share(name, proc);
  }

  get isRunning() {
    return this.pending && this.running != null;
  }

  share<T>(name: string, proc: SharedProc<T, this>): Promise<T> {
    if (!this.pending)
      return Promise.reject(new RangeError('Promise not available for sharing'));
    else if (this.isRunning)
      return this.extend(name, this.exec(proc));
    else
      return this.willRun(name, proc);
  }

  extend<P extends PromiseLike<any>>(name: string, result: P): P {
    if (!this.isRunning)
      throw new RangeError('Promise not available for extending');
    this.addRunning(settled(result));
    return result;
  }

  run() {
    // This allows for a recursive synchronous share in one of the runs
    this.running = Promise.resolve();
    if (this.tasks.length === 0) {
      this.resolve();
    } else {
      this.tasks.slice().forEach(task =>
        this.addRunning(task.run()));
    }
    return this;
  }

  /** Not currently used; for debugging deadlocks */
  toString(): string {
    return `[${this.tasks.map(({ name }) => name).join(', ')}]${this.running ? '...' : ''}`;
  }

  private willRun<T>(name: string, proc: SharedProc<T, this>) {
    return new Promise<T>((resolve, reject) => {
      this.tasks.push({
        name, run: () => {
          const result = this.exec(proc);
          result.then(resolve, reject);
          return settled(result);
        }
      });
    });
  }

  private addRunning(done: Promise<unknown>) {
    const run = Promise.all([this.running, done]).finally(() => {
      if (this.running === run)
        this.resolve();
    });
    this.running = run;
  }

  private exec<T>(proc: SharedProc<T, this>) {
    try {
      // Use of try block catches both sync and async errors
      return Promise.resolve(proc.call(this));
    } catch (e) {
      return Promise.reject(e);
    }
  }
}

export class LockManager<K extends string = string> {
  private locks: {
    [key: string]: {
      /** Currently running task, may be extended */
      running?: SharedPromise<unknown>,
      /** Head task, may be shared if not exclusive */
      head: SharedPromise<unknown>,
      /** Whether the head task is exclusive */
      exclusive: boolean
    }
  } = {};

  /**
   * Get the current state of the given lock.
   * - `null` means the lock is immediately available
   * - `{ ... exclusive: true }` means newly scheduled tasks will execute when
   * the lock opens
   * - `{ ... exclusive: false }` means tasks will be shared when any scheduled
   * exclusive tasks complete
   */
  state(key: K) {
    const lock = this.locks[key];
    return lock == null ? null : {
      // Debugging possibilities:
      // running: lock.running?.toString(),
      // head: lock.head.toString(),
      exclusive: lock.exclusive
    };
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
   * errors to ensure the lock is not permanently closed. If possible, prefer
   * the use of {@link share} or {@link exclusive}.
   *
   * @returns a promise that resolves when the lock is acquired, providing a
   * function to release it. This function can safely be called multiple times.
   */
  async acquire(key: K, purpose: string, mode: 'share' | 'exclusive'): Promise<() => void> {
    const running = new Future<() => void>();
    this[mode](key, purpose, () => {
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
  exclusive<T = void>(key: K, purpose: string, proc: () => (PromiseLike<T> | T)): Promise<T> {
    // Always wait for the current head to finish
    return this.next(key, purpose, proc, true);
  }

  /**
   * Schedules a shared task on the given lock. Sharing will only occur with the
   * latest scheduled task, which may or may not be running, and only if that
   * task is shared; otherwise, this task will be appended to the queue.
   */
  share<T = void>(key: K, purpose: string, proc: () => (PromiseLike<T> | T)): Promise<T> {
    const lock = this.locks[key];
    // If the head is shared and not finished, extend
    if (lock != null && !lock.exclusive && lock.head.pending) {
      return lock.head.share(LockManager.taskName(key, purpose), proc);
    } else {
      return this.next(key, purpose, proc, false);
    }
  }

  /**
   * Extends a running task to encompass the given task. This should only be
   * called from code whose precondition is an existing lock.
   *
   * @returns a promise which settles when the task completes
   * @throws {RangeError} if the lock is not currently held
   */
  extend<P extends PromiseLike<any>>(key: K, purpose: string, task: P): P {
    const lock = this.locks[key];
    if (lock == null || lock.running == null)
      throw new RangeError(`${key} not available for sharing`);
    // Forcing a share even if the lock is exclusive
    return lock.running.extend(LockManager.taskName(key, purpose), task);
  }

  private async next<T = void>(
    key: K, purpose: string, proc: () => (PromiseLike<T> | T), exclusive: boolean) {
    const prevTask = this.locks[key]?.head;
    const task = new SharedPromise(LockManager.taskName(key, purpose), proc);
    // Don't change the running task
    this.locks[key] = { ...this.locks[key], head: task, exclusive };
    task.then(() => {
      // If we're the last in the queue, delete ourselves
      if (this.locks[key]?.head === task)
        delete this.locks[key];
    });
    await prevTask; // This wait is the essence of the lock
    this.locks[key].running = task;
    task.run();
    return task.result;
  }

  private static taskName(key: string, purpose: string) {
    return `${key}: ${purpose}`;
  }
}
