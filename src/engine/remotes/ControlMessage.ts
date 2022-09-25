import { GlobalClock, TreeClock } from '../clocks';
import { MeldError, MeldErrorStatus } from '../MeldError';
import { MeldRequestType, MeldResponseType } from '../../ns/m-ld';
import { Attribution } from '../../api';
import * as MsgPack from '../msgPack';
import { Buffer } from 'buffer';

const inspect = Symbol.for('nodejs.util.inspect.custom');

////////////////////////////////////////////////////////////////////////////////
// TODO: Protect all of this with json-schema

type ControlJson = { '@type': MeldRequestType | MeldResponseType, [key: string]: any };

export class ControlMessage {
  /**
   * @param json enclosed control JSON
   * @param attr attribution of `this.enc`. This field is mutable to allow
   * signing of the enclosed prior to sending.
   * @param enc the wire-encoded JSON, if available
   */
  protected constructor(
    json: ControlJson,
    public attr: Attribution | null = null,
    readonly enc = MsgPack.encode(json)
  ) {}

  protected static decodeBuffer(buffer: Buffer) {
    const payload = MsgPack.decode(buffer);
    if (typeof payload == 'object' &&
      Buffer.isBuffer(payload.enc) &&
      typeof payload.attr == 'object') {
      const attr: Attribution | null = payload.attr;
      const json: ControlJson = MsgPack.decode(payload.enc);
      if (json != null && typeof json == 'object' && typeof json['@type'] === 'string')
        return { attr, json, enc: payload.enc as Buffer };
    }
    throw new Error('Bad control JSON');
  }

  toBuffer() {
    const { enc, attr } = this;
    return MsgPack.encode({ enc, attr });
  }

  [inspect] = () => this.toString();
}

export abstract class Request extends ControlMessage {
  // If return type is Request the type system thinks it's always NewClock
  static fromBuffer(buffer: Buffer) {
    const { json, attr, enc } = ControlMessage.decodeBuffer(buffer);
    switch (json['@type']) {
      case MeldRequestType.clock:
        return new NewClockRequest(attr, enc);
      case MeldRequestType.snapshot:
        return new SnapshotRequest(attr, enc);
      case MeldRequestType.revup:
        return new RevupRequest(TreeClock.fromJson(json.time), attr, enc);
    }
    throw new Error('Bad request JSON');
  }
}

export class NewClockRequest extends Request {
  constructor(attr: Attribution | null = null, enc?: Buffer) {
    super({ '@type': MeldRequestType.clock }, attr, enc);
  }
  toString() {
    return 'New Clock';
  }
}

export class SnapshotRequest extends Request {
  constructor(attr: Attribution | null = null, enc?: Buffer) {
    super({ '@type': MeldRequestType.snapshot }, attr, enc);
  }
  toString() {
    return 'Snapshot';
  }
}

export class RevupRequest extends Request {
  constructor(
    readonly time: TreeClock,
    attr: Attribution | null = null,
    enc?: Buffer
  ) {
    super({ '@type': MeldRequestType.revup, time: time.toJSON() }, attr, enc);
  };
  toString() {
    return `Revup from ${this.time}`;
  }
}

export abstract class Response extends ControlMessage {
  static fromBuffer(buffer: Buffer) {
    const { json, attr, enc } = ControlMessage.decodeBuffer(buffer);
    switch (json['@type']) {
      case MeldResponseType.clock:
        return new NewClockResponse(TreeClock.fromJson(json.clock), attr, enc);
      case MeldResponseType.snapshot:
        return new SnapshotResponse(
          GlobalClock.fromJSON(json.gwc),
          TreeClock.fromJson(json.agreed),
          json.dataAddress,
          json.updatesAddress,
          attr, enc);
      case MeldResponseType.revup:
        return new RevupResponse(
          json.gwc != null ? GlobalClock.fromJSON(json.gwc) : null,
          json.updatesAddress,
          attr, enc);
      case MeldResponseType.rejected:
        return new RejectedResponse(<MeldErrorStatus><number>json.status, attr, enc);
    }
    throw new MeldError('Bad response', json);
  }
}

export class NewClockResponse extends Response {
  constructor(
    readonly clock: TreeClock,
    attr: Attribution | null = null,
    enc?: Buffer
  ) {
    super({
      '@type': MeldResponseType.clock,
      clock: clock.toJSON()
    }, attr, enc);
  };

  toString() {
    return `New Clock ${this.clock}`;
  }
}

export class SnapshotResponse extends Response {
  constructor(
    readonly gwc: GlobalClock,
    readonly agreed: TreeClock,
    readonly dataAddress: string,
    readonly updatesAddress: string,
    attr: Attribution | null = null,
    enc?: Buffer
  ) {
    super({
      '@type': MeldResponseType.snapshot,
      gwc: gwc.toJSON(),
      agreed: agreed.toJSON(),
      dataAddress,
      updatesAddress
    }, attr, enc);
  }

  toString() {
    return `Snapshot at ${this.gwc}`;
  }
}

export class RevupResponse extends Response {
  /**
   *
   * @param gwc `null` indicates this clone cannot collaborate on the rev-up request
   * @param updatesAddress If gwc == null this should be a stable identifier of the answering
   * clone, to allow detection of re-send.
   * @param attr attribution
   * @param enc the wire-encoded JSON, if available
   */
  constructor(
    readonly gwc: GlobalClock | null,
    readonly updatesAddress: string,
    attr: Attribution | null = null,
    enc?: Buffer
  ) {
    super({
      '@type': MeldResponseType.revup,
      gwc: gwc == null ? null : gwc.toJSON(),
      updatesAddress
    }, attr, enc);
  }

  toString() {
    return this.gwc != null ?
      `Can revup from ${this.updatesAddress} @ ${this.gwc}` :
      `${this.updatesAddress} can't provide revup`;
  }
}

export class RejectedResponse extends Response {
  constructor(readonly status: MeldErrorStatus, attr: Attribution | null = null, enc?: Buffer) {
    super({ '@type': MeldResponseType.rejected, status }, attr, enc);
  }

  toString() {
    return `Rejected with ${MeldErrorStatus[this.status]}`;
  }
}
