import BN = require('bn.js');
import { randomBytes, createHash, BinaryLike } from 'crypto';
const inspect = Symbol.for('nodejs.util.inspect.custom');

export class Hash {
  static readonly BYTE_WIDTH = 32;
  static readonly BIT_WIDTH = Hash.BYTE_WIDTH*8;

  private readonly value: BN;

  static random(): Hash {
    return new Hash(randomBytes(this.BYTE_WIDTH));
  }

  static digest(...data: BinaryLike[]): Hash {
    const hash = createHash('sha256');
    data.forEach(datum => hash.update(datum));
    return new Hash(hash.digest());
  }

  static decode(encoded: string): Hash {
    return new Hash(Buffer.from(encoded, 'base64'));
  }

  encode(): string {
    return Hash.toBuffer(this.value).toString('base64');
  }

  add(that: Hash): Hash {
    return new Hash(Hash.toBuffer(this.value.add(that.value)));
  }

  equals(that: Hash): boolean {
    return this.value.eq(that.value);
  }

  toString() {
    return `Hash: ${this.encode()}`;
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();

  private constructor(hash: Buffer) {
    this.value = new BN(Hash.toWidth(hash, Hash.BYTE_WIDTH), 'be').fromTwos(Hash.BIT_WIDTH);
  }

  static toWidth(src: Buffer, width: number): Buffer {
    if (src.length == width) {
      return src;
    } else if (src.length > width) {
      // We truncate the front of the array to allow use for integer overflow
      return src.slice(src.length - width);
    } else {
      const rtn = Buffer.alloc(width);
      // Pad left with the sign; (byte)-1 is 11111111
      if (src[0] >> 7)
        rtn.fill(-1, 0, width - src.length);
      rtn.set(src, width - src.length);
      return rtn;
    }
  }

  private static toBuffer(bi: BN) {
    return bi.toTwos(Hash.BIT_WIDTH).toArrayLike(Buffer, 'be', Hash.BYTE_WIDTH);
  }
}