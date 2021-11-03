import type { MemDownConstructor } from 'memdown';
import type { AbstractIteratorOptions, ErrorKeyValueCallback } from 'abstract-leveldown';
// Default import has gone away: https://github.com/Level/community/issues/87
const MemDown: MemDownConstructor = require('memdown');

type _Next<K> = (cb: ErrorKeyValueCallback<K, Buffer>) => void;

export class MeldMemDown extends MemDown<string, Buffer> {
  constructor() {
    super();
    this.supports.bufferKeys = false;
  }

  // noinspection JSUnusedGlobalSymbols
  protected _serializeKey(key: any): string {
    return key.toString();
  }

  // noinspection JSUnusedGlobalSymbols
  protected _iterator(options: AbstractIteratorOptions): any {
    const it = super._iterator(options);
    // Default is keys as buffers (whether or not we support them)
    if (options.keyAsBuffer == null || options.keyAsBuffer) {
      // Our iterator emits a string
      const superNext: _Next<string> = it._next.bind(it);
      const _next: _Next<Buffer | undefined> = cb =>
        superNext((err, key, value) =>
          cb(err, key != null ? Buffer.from(key) : undefined, value));
      return Object.assign(it, { _next });
    }
    return it;
  }
}