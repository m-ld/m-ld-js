import { EventEmitter } from 'events';
import * as ls from 'local-storage';
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
  }

  private hasPing() {
    return local.get(this.key + '__ping') === this.id;
  }

  private enable() {
    local.on(this.key + '__ping', this.ping);
    local.set(this.key, this.id);
  }
}
