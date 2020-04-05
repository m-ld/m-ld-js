import { Hash } from '../hash';
import { TreeClock } from '../clocks';

////////////////////////////////////////////////////////////////////////////////
// TODO: Protect all of this with json-schema

export interface Hello {
  id: string;
}

export namespace Request {
  export class NewClock {
    static readonly JSON = { '@type': 'http://control.m-ld.org/request/clock' };
  }

  export class Snapshot {
    static readonly JSON = { '@type': 'http://control.m-ld.org/request/snapshot' };
  }

  export class Revup {
    constructor(readonly lastHash: Hash) { };
    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/request/revup',
      lastHash: this.lastHash.encode()
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
          return new Revup(Hash.decode(json.lastHash));
      }
    }
    throw new Error('Bad request JSON');
  }
}
export type Request = Request.NewClock | Request.Snapshot | Request.Revup;

export namespace Response {
  export class NewClock {
    constructor(
      readonly clock: TreeClock) {
    };
    
    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/clock',
      clock: this.clock.toJson()
    });
  }

  export class Snapshot {
    constructor(
      readonly time: TreeClock,
      readonly dataAddress: string,
      readonly lastHash: Hash,
      readonly updatesAddress: string) {
    }
    
    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/snapshot',
      time: this.time.toJson(),
      dataAddress: this.dataAddress,
      lastHash: this.lastHash.encode(),
      updatesAddress: this.updatesAddress
    });
  }

  export class Revup {
    constructor(
      readonly hashFound: boolean,
      /**
       * If !hashFound this should be a stable identifier of the answering clone,
       * to allow detection of a re-send.
       */
      readonly updatesAddress: string) {
    }
    
    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/revup',
      hashFound: this.hashFound,
      updatesAddress: this.updatesAddress
    })
  }

  export function fromJson(json: any): Response {
    if (typeof json === 'object' && typeof json['@type'] === 'string') {
      switch (json['@type']) {
        case 'http://control.m-ld.org/response/clock':
          const clock = TreeClock.fromJson(json.clock);
          if (clock)
            return new NewClock(clock);
        case 'http://control.m-ld.org/response/snapshot':
          const time = TreeClock.fromJson(json.time);
          if (time)
            return new Snapshot(time, json.dataAddress, Hash.decode(json.lastHash), json.updatesAddress);
        case 'http://control.m-ld.org/response/revup':
          return new Revup(json.hashFound, json.updatesAddress);
      }
    }
    throw new Error('Bad response JSON');
  }
}
export type Response = Response.NewClock | Response.Revup | Response.Snapshot;
