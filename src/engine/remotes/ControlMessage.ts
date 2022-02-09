import { GlobalClock, TreeClock } from '../clocks';
import { MeldError, MeldErrorStatus } from '../MeldError';
import { MeldRequestType, MeldResponseType } from '../../ns/m-ld';

const inspect = Symbol.for('nodejs.util.inspect.custom');

////////////////////////////////////////////////////////////////////////////////
// TODO: Protect all of this with json-schema

export abstract class ControlMessage {
  [inspect]() {
    return this.toString();
  }

  abstract toJSON(): { '@type': MeldRequestType | MeldResponseType, [key: string]: any };
}

export abstract class Request extends ControlMessage {
  // If return type is Request the type system thinks it's always NewClock
  static fromJson(json: any) {
    if (typeof json === 'object' && typeof json['@type'] === 'string') {
      switch (json['@type']) {
        case MeldRequestType.clock:
          return new NewClockRequest;
        case MeldRequestType.snapshot:
          return new SnapshotRequest;
        case MeldRequestType.revup:
          return new RevupRequest(TreeClock.fromJson(json.time));
      }
    }
    throw new Error('Bad request JSON');
  }
}

export class NewClockRequest extends Request {
  toJSON() {
    return { '@type': MeldRequestType.clock };
  }
  toString() {
    return 'New Clock';
  }
}

export class SnapshotRequest extends Request {
  toJSON() {
    return { '@type': MeldRequestType.snapshot };
  }
  toString() {
    return 'Snapshot';
  }
}

export class RevupRequest extends Request {
  constructor(
    readonly time: TreeClock) {
    super();
  };
  toString() {
    return `Revup from ${this.time}`;
  }
  toJSON() {
    return { '@type': MeldRequestType.revup, time: this.time.toJSON() };
  }
}

export abstract class Response extends ControlMessage {
  static fromJson(json: any) {
    if (typeof json === 'object' && typeof json['@type'] === 'string') {
      switch (json['@type']) {
        case MeldResponseType.clock:
          return new NewClockResponse(TreeClock.fromJson(json.clock));
        case MeldResponseType.snapshot:
          return new SnapshotResponse(
            GlobalClock.fromJSON(json.gwc),
            TreeClock.fromJson(json.agreed),
            json.dataAddress,
            json.updatesAddress);
        case MeldResponseType.revup:
          return new RevupResponse(
            json.gwc != null ? GlobalClock.fromJSON(json.gwc) : null,
            json.updatesAddress);
        case MeldResponseType.rejected:
          return new RejectedResponse(<MeldErrorStatus><number>json.status);
      }
    }
    throw new MeldError('Bad response', json);
  }
}

export class NewClockResponse extends Response {
  constructor(
    readonly clock: TreeClock) {
    super();
  };

  toJSON() {
    return {
      '@type': MeldResponseType.clock,
      clock: this.clock.toJSON()
    };
  }

  toString() {
    return `New Clock ${this.clock}`;
  }
}

export class SnapshotResponse extends Response {
  constructor(
    readonly gwc: GlobalClock,
    readonly agreed: TreeClock,
    readonly dataAddress: string,
    readonly updatesAddress: string) {
    super();
  }

  toJSON() {
    return {
      '@type': MeldResponseType.snapshot,
      gwc: this.gwc.toJSON(),
      agreed: this.agreed.toJSON(),
      dataAddress: this.dataAddress,
      updatesAddress: this.updatesAddress
    };
  }

  toString() {
    return `Snapshot at ${this.gwc}`;
  }
}

export class RevupResponse extends Response {
  constructor(
    /**
     * `null` indicates this clone cannot collaborate on the rev-up request
     */
    readonly gwc: GlobalClock | null,
    /**
     * If gwc == null this should be a stable identifier of the answering
     * clone, to allow detection of re-send.
     */
    readonly updatesAddress: string) {
    super();
  }

  toJSON() {
    return {
      '@type': MeldResponseType.revup,
      gwc: this.gwc == null ? null : this.gwc.toJSON(),
      updatesAddress: this.updatesAddress
    };
  }

  toString() {
    return this.gwc != null ?
      `Can revup from ${this.updatesAddress} @ ${this.gwc}` :
      `${this.updatesAddress} can't provide revup`;
  }
}

export class RejectedResponse extends Response {
  constructor(
    readonly status: MeldErrorStatus) {
    super();
  }

  toJSON() {
    return {
      '@type': MeldResponseType.rejected,
      status: this.status
    };
  }

  toString() {
    return `Rejected with ${MeldErrorStatus[this.status]}`;
  }
}
