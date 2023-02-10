import { TreeClock } from './clocks';
import * as MsgPack from './msgPack';
import { Attribution, AuditOperation, MeldError } from '../api';
import { levels } from 'loglevel';
import { MeldEncoder } from './MeldEncoding';
import { BufferEncoding, EncodedOperation, OperationMessage } from './index';
import { Future } from './Future';

const inspect = Symbol.for('nodejs.util.inspect.custom');

/**
 * Convenience implementation of an operation message, with static methods for
 * canonical serialisation
 */
export class MeldOperationMessage implements OperationMessage {
  static fromOperation(
    prev: number,
    data: EncodedOperation,
    attr: Attribution | null,
    time?: TreeClock
  ) {
    return new MeldOperationMessage(
      prev, EncodedOperation.toBuffer(data), attr, data, time);
  }

  static fromBuffer(payload: Uint8Array): MeldOperationMessage {
    const json = MsgPack.decode(payload);
    if (typeof json == 'object' && typeof json.prev == 'number' &&
      Buffer.isBuffer(json.enc) && typeof json.attr == 'object') {
      return new MeldOperationMessage(json.prev, json.enc, json.attr);
    } else {
      throw new MeldError('Bad update');
    }
  }

  static fromMessage(msg: OperationMessage): MeldOperationMessage {
    if (msg instanceof MeldOperationMessage)
      return msg;
    else
      return new MeldOperationMessage(msg.prev,
        EncodedOperation.toBuffer(msg.data),
        msg.attr, msg.data, msg.time);
  }

  /**
   * An internal signal to ensure correct ordering of revups
   */
  readonly delivered = new Future;

  /**
   * @param prev Previous public tick from the operation source
   * @param enc MessagePack-encoded enclosed update operation
   * @param attr Attribution of the operation to a security principal
   * @param [data] the actual update operation (decoded if not provided)
   * @param [time] the update time (decoded if not provided)
   */
  private constructor(
    readonly prev: number,
    readonly enc: Buffer,
    readonly attr: Attribution | null,
    readonly data: EncodedOperation = EncodedOperation.fromBuffer(enc),
    readonly time = TreeClock.fromJson(data[EncodedOperation.Key.time])
  ) {}

  /** Approximate length of serialised message, in bytes */
  get size() {
    const { pid, sig } = this.attr ?? {};
    return 8 + this.enc.length + (pid?.length ?? 0) + (sig?.length ?? 0);
  }

  toAuditOperation(): AuditOperation {
    return { attribution: this.attr, data: this.enc, operation: this.data }
  }

  static enc(msg: OperationMessage) {
    if (msg instanceof MeldOperationMessage)
      return msg.enc;
    else
      return EncodedOperation.toBuffer(msg.data);
  }

  static toBuffer(msg: OperationMessage) {
    const { prev, attr } = msg;
    const enc = MeldOperationMessage.enc(msg);
    return MsgPack.encode({ prev, enc, attr });
  }

  static toString(msg: OperationMessage, logLevel: number = levels.INFO) {
    const [v, from, time, updateData, encoding] = msg.data;
    const update = logLevel <= levels.DEBUG ?
      encoding.includes(BufferEncoding.SECURE) ? '---ENCRYPTED---' :
        MeldEncoder.jsonFromBuffer(updateData, encoding) :
      { length: updateData.length, encoding };
    return `${JSON.stringify({ v, from, time, update })}
    @ ${msg.time}, prev ${msg.prev}`;
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();
}