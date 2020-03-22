import { Hash } from '../hash';
import { TreeClock } from '../clocks';

export interface Hello {
  id: string;
}

export namespace Request {
  export interface NewClock { }

  export function isNewClock(req: Request): req is NewClock {
    return req === NewClock.INSTANCE; // See fromJson
  }

  export interface Snapshot { }

  export function isSnapshot(req: Request): req is Snapshot {
    return req === Snapshot.INSTANCE; // See fromJson
  }

  export interface Revup {
    lastHash: Hash;
  }

  export function isRevup(req: Request): req is Revup {
    return 'lastHash' in req;
  }

  export const NewClock = {
    INSTANCE: {} as NewClock,
    JSON: { '@type': 'http://control.m-ld.org/request/clock' }
  }

  export const Snapshot = {
    INSTANCE: {} as Snapshot,
    JSON: { '@type': 'http://control.m-ld.org/request/snapshot' }
  }

  export const Revup = {
    toJson: (res: Revup) => ({
      '@type': 'http://control.m-ld.org/request/revup', ...res
    })
  }

  // If return type is Request the type system thinks it's always NewClock
  export function fromJson(json: any): any {
    switch (json['@type']) {
      case 'http://control.m-ld.org/request/clock':
        return NewClock.INSTANCE; // See isNewClock
      case 'http://control.m-ld.org/request/snapshot':
        return Snapshot.INSTANCE; // See isSnapshot
      case 'http://control.m-ld.org/request/revup':
        return { lastHash: json.lastHash };
    }
    throw new Error('Bad request JSON');
  }
}
export type Request = Request.NewClock | Request.Snapshot | Request.Revup;

export namespace Response {
  export interface NewClock {
    clock: TreeClock;
  }

  export interface Snapshot {
    time: TreeClock;
    dataAddress: string;
    lastHash: Hash;
    updatesAddress: string;
  }

  export interface Revup {
    hashFound: boolean;
    /**
     * If !hashFound this should be a stable identifier of the answering clone,
     * to allow detection of a re-send.
     */
    updatesAddress: string;
  }

  export const NewClock = {
    toJson: (res: NewClock) => ({
      '@type': 'http://control.m-ld.org/response/clock', ...res,
      clock: res.clock.toJson()
    })
  }

  export const Snapshot = {
    toJson: (res: Snapshot) => ({
      '@type': 'http://control.m-ld.org/response/snapshot', ...res,
      time: res.time.toJson(),
      lastHash: res.lastHash.encode()
    })
  }

  export const Revup = {
    toJson: (res: Revup) => ({
      '@type': 'http://control.m-ld.org/response/revup', ...res
    })
  }

  export function fromJson(json: any): Response {
    switch (json['@type']) {
      case 'http://control.m-ld.org/response/clock':
        return { ...json, clock: TreeClock.fromJson(json.clock) };
      case 'http://control.m-ld.org/response/snapshot':
        return { ...json, time: TreeClock.fromJson(json.time), lastHash: Hash.decode(json.lastHash) };
      case 'http://control.m-ld.org/response/revup':
        return { ...json };
    }
    throw new Error('Bad response JSON');
  }
}
export type Response = Response.NewClock | Response.Revup | Response.Snapshot;
