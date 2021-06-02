import { EventEmitter } from 'events';
import * as ls from 'local-storage';
import { Observable } from 'rxjs';
import isNode = require('detect-node');

export interface LocalStorage {
  set(key: string, value: string | null): void;
  get(key: string): string | null;
  on(key: string, cb: (value: string) => void): void;
  off(key: string, cb: (value: string) => void): void;
}

class NoStorage extends EventEmitter implements LocalStorage {
  private storage: { [key: string]: string | null } = {};
  set = (key: string, value: string | null) => {
    this.storage[key] = value;
    this.emit(key, value);
  };
  get = (key: string) => this.storage[key];
}

export const local: LocalStorage = isNode ? new NoStorage : ls;

export class LocalLock {
  constructor(
    readonly id: string,
    readonly key: string) {
  }

  acquire(): Promise<void> {
    const owner = local.get(this.key);
    if (owner === this.id || owner == null) {
      // FIXME: If someone else is acquiring concurrently, we have contention
      this.enable();
      return Promise.resolve();
    } else {
      this.ping();
      // Give the owner a few millis to respond whether they are still there
      // FIXME: This won't work if that owner's event loop is blocked
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          if (this.hasPing()) {
            this.enable();
            resolve();
          } else {
            reject(new Error('Lock is busy'));
          }
        }, 20);
      });
    }
  }

  release() {
    local.off(this.key + '__ping', this.ping);
    local.set(this.key, null);
  }

  private ping = () => {
    local.set(this.key + '__ping', this.id);
  };

  private hasPing() {
    return local.get(this.key + '__ping') === this.id;
  }

  private enable() {
    local.on(this.key + '__ping', this.ping);
    local.set(this.key, this.id);
  }
}

export namespace Idle {
  type Handle = number | NodeJS.Immediate;
  const DEFAULT_IDLE_TIME = 50.0;

  const root: any = typeof window === 'undefined' ? global || {} : window;

  export const requestCallback: (callback: IdleRequestCallback) => Handle =
    root.requestIdleCallback?.bind(root) ?? ((callback: IdleRequestCallback) =>
      setImmediate((startTime: number) => callback({
        timeRemaining: () => Math.max(0, DEFAULT_IDLE_TIME - (Date.now() - startTime)),
        didTimeout: false // Not supporting timeout parameter to callback request
      }), Date.now()));

  export const cancelCallback: (handle: Handle) => void =
    root.cancelIdleCallback?.bind(root) ?? ((handle: Handle) => {
      if (typeof handle != 'number')
        clearImmediate(handle);
    });
}

export const idling = new Observable<IdleDeadline>(subs => {
  const handle = Idle.requestCallback(deadline => {
    subs.next(deadline);
    subs.complete();
  });
  return () => Idle.cancelCallback(handle);
});