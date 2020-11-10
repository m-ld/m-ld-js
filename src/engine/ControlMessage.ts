import { Hash } from './hash';
import { TreeClock } from './clocks';
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
          const time = TreeClock.fromJson(json.time);
          if (time)
            return new Revup(time);
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
      readonly lastTime: TreeClock,
      readonly quadsAddress: string,
      readonly tidsAddress: string,
      readonly lastHash: Hash,
      readonly updatesAddress: string) {
    }

    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/snapshot',
      lastTime: this.lastTime.toJson(),
      quadsAddress: this.quadsAddress,
      tidsAddress: this.tidsAddress,
      lastHash: this.lastHash.encode(),
      updatesAddress: this.updatesAddress
    });

    toString = () => `Snapshot at ${this.lastTime} with ${this.lastHash}`;
    [inspect] = () => this.toString();
  }

  export class Revup implements Response {
    constructor(
      /**
       * `null` indicates this clone cannot collaborate on the rev-up request
       */
      readonly lastTime: TreeClock | null,
      /**
       * If lastTime == null this should be a stable identifier of the answering
       * clone, to allow detection of a re-send.
       */
      readonly updatesAddress: string) {
    }

    readonly toJson = () => ({
      '@type': 'http://control.m-ld.org/response/revup',
      lastTime: this.lastTime == null ? null : this.lastTime.toJson(),
      updatesAddress: this.updatesAddress
    })

    toString = () => this.lastTime != null ?
      `Can revup from ${this.updatesAddress} @ ${this.lastTime}` :
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
      let lastTime: TreeClock | null;
      switch (json['@type']) {
        case 'http://control.m-ld.org/response/clock':
          const clock = TreeClock.fromJson(json.clock);
          if (clock)
            return new NewClock(clock);
          break;
        case 'http://control.m-ld.org/response/snapshot':
          lastTime = TreeClock.fromJson(json.lastTime);
          if (lastTime)
            return new Snapshot(
              lastTime, json.quadsAddress, json.tidsAddress,
              Hash.decode(json.lastHash), json.updatesAddress);
          break;
        case 'http://control.m-ld.org/response/revup':
          lastTime = TreeClock.fromJson(json.lastTime);
          return new Revup(lastTime, json.updatesAddress);
        case 'http://control.m-ld.org/response/rejected':
          return new Rejected(<MeldErrorStatus><number>json.status);
      }
    }
    throw new MeldError('Bad response', JSON.stringify(json));
  }
}