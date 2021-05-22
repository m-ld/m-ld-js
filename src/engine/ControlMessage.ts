import { GlobalClock, TreeClock } from './clocks';
import { MeldError, MeldErrorStatus } from './MeldError';
const inspect = Symbol.for('nodejs.util.inspect.custom');

////////////////////////////////////////////////////////////////////////////////
// TODO: Protect all of this with json-schema

export interface Request {
  toJson(): object;
}

export namespace Request {
  export class NewClock implements Request {
    toJson = () => NewClock.JSON;
    toString = () => 'New Clock';
    [inspect] = () => this.toString();
    static readonly JSON = { '@type': 'http://control.m-ld.org/request/clock' };
  }

  export class Snapshot implements Request {
    toJson = () => Snapshot.JSON;
    toString = () => 'Snapshot';
    [inspect] = () => this.toString();
    static readonly JSON = { '@type': 'http://control.m-ld.org/request/snapshot' };
  }

  export class Revup implements Request {
    constructor(readonly time: TreeClock) { };
    toString = () => `Revup from ${this.time}`;
    [inspect] = () => this.toString();
    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/request/revup',
      time: this.time.toJson()
    });
  }

  // If return type is Request the type system thinks it's always NewClock
  export function fromJson(json: any): Request {
    if (typeof json === 'object' && typeof json['@type'] === 'string') {
      switch (json['@type']) {
        case 'http://control.m-ld.org/request/clock':
          return new NewClock;
        case 'http://control.m-ld.org/request/snapshot':
          return new Snapshot;
        case 'http://control.m-ld.org/request/revup':
          return new Revup(TreeClock.fromJson(json.time));
      }
    }
    throw new Error('Bad request JSON');
  }
}

export interface Response {
  toJson(): object;
};

export namespace Response {
  export class NewClock implements Response {
    constructor(
      readonly clock: TreeClock) {
    };

    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/clock',
      clock: this.clock.toJson()
    });

    toString = () => `New Clock ${this.clock}`;
    [inspect] = () => this.toString();
  }

  export class Snapshot implements Response {
    constructor(
      readonly gwc: GlobalClock,
      readonly dataAddress: string,
      readonly updatesAddress: string) {
    }

    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/snapshot',
      gwc: this.gwc.toJson(),
      dataAddress: this.dataAddress,
      updatesAddress: this.updatesAddress
    });

    toString = () => `Snapshot at ${this.gwc}`;
    [inspect] = () => this.toString();
  }

  export class Revup implements Response {
    constructor(
      /**
       * `null` indicates this clone cannot collaborate on the rev-up request
       */
      readonly gwc: GlobalClock | null,
      /**
       * If gwc == null this should be a stable identifier of the answering
       * clone, to allow detection of a re-send.
       */
      readonly updatesAddress: string) {
    }

    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/revup',
      gwc: this.gwc == null ? null : this.gwc.toJson(),
      updatesAddress: this.updatesAddress
    })

    toString = () => this.gwc != null ?
      `Can revup from ${this.updatesAddress} @ ${this.gwc}` :
      `${this.updatesAddress} can't provide revup`;
    [inspect] = () => this.toString();
  }

  export class Rejected implements Response {
    constructor(
      readonly status: MeldErrorStatus) {
    }

    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/rejected',
      status: this.status
    })

    toString = () => `Rejected with ${MeldErrorStatus[this.status]}`;
    [inspect] = () => this.toString();
  }

  export function fromJson(json: any): Response {
    if (typeof json === 'object' && typeof json['@type'] === 'string') {
      switch (json['@type']) {
        case 'http://control.m-ld.org/response/clock':
          return new NewClock(TreeClock.fromJson(json.clock));
        case 'http://control.m-ld.org/response/snapshot':
          return new Snapshot(
            GlobalClock.fromJson(json.gwc), json.dataAddress,
            json.updatesAddress);
        case 'http://control.m-ld.org/response/revup':
          return new Revup(
            json.gwc != null ? GlobalClock.fromJson(json.gwc) : null,
            json.updatesAddress);
        case 'http://control.m-ld.org/response/rejected':
          return new Rejected(<MeldErrorStatus><number>json.status);
      }
    }
    throw new MeldError('Bad response', JSON.stringify(json));
  }
}