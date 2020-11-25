import { ErrorCallback } from 'abstract-leveldown';

declare module 'abstract-leveldown' {
  export interface AbstractLevelDOWN<K = any, V = any> {
    clear(cb: ErrorCallback): void;
    clear(options: AbstractIteratorOptions<K>, cb: ErrorCallback): void;
  }
}