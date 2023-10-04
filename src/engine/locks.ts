import { settled } from './util';
import { Future } from './Future';
import { check } from './check';
import { MeldError } from '../api';

interface Task {
  /** For debugging */
  id: unknown;
  /** Must never reject */
  run?: () => Promise<unknown>;
}

export type SharedProc<T, P extends SharedPromise<unknown>> = (this: P) => (PromiseLike<T> | T);

/**
 * TODO docs
 */
export class SharedPromise<R> extends Future {
  private running: Promise<unknown> | null = null;
  private readonly tasks: Task[] = [];
  readonly result: Promise<R>;

  constructor(id: unknown, proc: SharedProc<R, SharedPromise<R>>) {
    super();
    this.result = this.share(id, proc);
  }

  get isRunning() {
    return this.pending && this.running != null;
  }

  share<T>(id: unknown, proc: SharedProc<T, this>): Promise<T> {
    if (!this.pending)
      return Promise.reject(new RangeError('Promise not available for sharing'));
    else if (this.isRunning)
      return this.extend(id, this.exec(proc));
    else
      return this.willRun(id, proc);
  }

  extend<P extends PromiseLike<unknown>>(id: unknown, result: P): P {
    if (!this.isRunning)
      throw new RangeError('Promise not available for extending');
    this.tasks.push({ id }); // For debugging
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
        this.addRunning(task.run?.() ?? Promise.resolve()));
    }
    return this;
  }

  /** Not currently used; for debugging deadlocks */
  toString(): string {
    return `[${this.tasks.map(({ id }) => id).join(', ')}]${this.running ? '...' : ''}`;
  }

  private willRun<T>(id: unknown, proc: SharedProc<T, this>) {
    return new Promise<T>((resolve, reject) => {
      this.tasks.push({
        id,
        run: () => {
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

export function checkLocked<Key extends string>(key: Key) {
  return check((m: { lock: LockManager<Key> }) => m.lock.state(key) != null,
    () => new MeldError('Unknown error', 'Clone state not locked'));
}

/** newly scheduled tasks will execute when the lock opens */
export const EXCLUSIVE = Symbol('exclusive');
/** new shared tasks will extend the most recent task */
export const SHARED = Symbol('shared');
/** new shared tasks will extend the running task */
export const SHARED_REENTRANT = Symbol('shared-reentrant');

type SharedLockMode = typeof SHARED | typeof SHARED_REENTRANT;
type LockMode = typeof EXCLUSIVE | SharedLockMode;

interface LockMeta {
  /**
   * The original reason for acquiring the lock. Additional purposes may have
   * been added by sharing or extension of the lock.
   */
  purpose: string | Symbol,
  /**
   * `true` means .
   * `false` means tasks will be shared when any scheduled exclusive tasks
   * complete.
   */
  mode: LockMode
}

class LockPeriod<T = unknown> extends SharedPromise<T> implements LockMeta {
  /**
   * @param key the lock identity
   * @param purpose the original reason for acquiring the lock
   * @param proc the first task to run
   * @param mode lock sharing mode
   */
  constructor(
    readonly key: string,
    readonly purpose: string | Symbol,
    proc: SharedProc<T, SharedPromise<T>>,
    readonly mode: LockMode
  ) {
    super({ key, purpose }, proc);
  }
}

export class LockManager<K extends string = string> {
  private locks: {
    [key: string]: {
      /** Currently running task, may be extended */
      running?: LockPeriod,
      /** Head task, may be shared if not exclusive */
      head: LockPeriod
    }
  } = {};

  /**
   * Get the current state of the given lock.
   * `undefined` means the lock is immediately available.
   */
  state(key: K): { running?: LockMeta; head: LockMeta } | undefined {
    return this.locks[key];
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
   * the use of {@link share}/{@link exclusive}/{@link extend}.
   *
   * @returns a promise that resolves when the lock is acquired, providing a
   * function to release it. This function can safely be called multiple times.
   */
  async acquire(
    key: K,
    purpose: string | Symbol,
    mode: LockMode
  ): Promise<() => void> {
    const running = new Future<() => void>();
    const proc = () => {
      const done = new Future;
      running.resolve(done.resolve);
      return done;
    };
    if (mode === EXCLUSIVE)
      // noinspection ES6MissingAwait
      this.exclusive(key, purpose, proc);
    else
      // noinspection ES6MissingAwait
      this.share(key, purpose, proc, mode);
    return running;
  }

  /**
   * Schedules an exclusive task on the given lock. This task will execute when
   * any running task has completed.
   */
  exclusive<T = void>(
    key: K,
    purpose: string | Symbol,
    proc: () => (PromiseLike<T> | T)
  ): Promise<T> {
    // Always wait for the current head to finish
    return this.next(key, purpose, proc, EXCLUSIVE);
  }

  /**
   * Schedules a shared task on the given lock.
   *
   * @remarks
   * If the currently running task allows reentry, the new task will extend it.
   * Otherwise, sharing will only occur with the latest scheduled task, which
   * may or may not be running, and only if that task allows sharing; otherwise,
   * this task will be appended to the queue.
   */
  share<T = void>(
    key: K,
    purpose: string | Symbol,
    proc: () => (PromiseLike<T> | T),
    mode: SharedLockMode = SHARED
  ): Promise<T> {
    const lock = this.locks[key];
    // If the running task is reentrant, extend it
    if (lock?.running?.mode === SHARED_REENTRANT)
      return lock.running.share({ key, purpose }, proc);
    // If the head is shared and not finished, extend it
    if (lock?.head.pending && lock.head.mode !== EXCLUSIVE)
      return lock.head.share({ key, purpose }, proc);
    // Otherwise, schedule after current head
    return this.next(key, purpose, proc, mode);
  }

  /**
   * Extends a running task to encompass the given task. This should only be
   * called from code whose precondition is an existing lock.
   *
   * @returns a promise which settles when the task completes
   * @throws {RangeError} if the lock is not currently held
   */
  extend<P extends PromiseLike<any>>(key: K, purpose: string | Symbol, task: P): P {
    const lock = this.locks[key];
    if (lock == null || lock.running == null)
      throw new RangeError(`${key} not available for sharing`);
    // Forcing a share even if the lock is exclusive
    return lock.running.extend({ key, purpose }, task);
  }

  private async next<T = void>(
    key: K,
    purpose: string | Symbol,
    proc: () => (PromiseLike<T> | T),
    mode: LockMode
  ) {
    const prevTask = this.locks[key]?.head;
    const task = new LockPeriod(key, purpose, proc, mode);
    // Don't change the running task
    this.locks[key] = { ...this.locks[key], head: task };
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
}
