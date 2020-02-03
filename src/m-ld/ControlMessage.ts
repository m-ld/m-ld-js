import { Hash } from '../hash';
import { TreeClock } from '../clocks';
import { toTimeString, fromTimeString } from './JsonDelta';

export namespace Request {
  export interface NewClock { }

  export function isNewClock(req: Request): req is NewClock {
    return req === NewClock; // See fromJson
  }

  export interface Snapshot { }

  export function isSnapshot(req: Request): req is Snapshot {
    return req === Snapshot; // See fromJson
  }

  export interface Revup {
    lastHash: Hash;
  }

  export function isRevup(req: Request): req is Revup {
    return 'lastHash' in req;
  }

  export const NewClock = {
    json: { '@type': 'http://control.m-ld.org/request/clock' }
  }

  export const Snapshot = {
    json: { '@type': 'http://control.m-ld.org/request/snapshot' }
  }

  export const Revup = {
    toJson: (res: Revup) => ({
      '@type': 'http://control.m-ld.org/request/revup', ...res
    })
  }

  export function fromJson(json: any): Request {
    switch (json['@type']) {
      case 'http://control.m-ld.org/request/clock':
        return NewClock; // See isNewClock
      case 'http://control.m-ld.org/request/snapshot':
        return Snapshot; // See isSnapshot
      case 'http://control.m-ld.org/request/revup':
        return { ...json };
    }
    throw new Error('Bad request JSON');
  }
}
export type Request = {} | Request.Revup;

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
    updatesAddress: string;
  }

  export const NewClock = {
    toJson: (res: NewClock) => ({
      '@type': 'http://control.m-ld.org/response/clock', ...res,
      clock: toTimeString(res.clock)
    })
  }

  export const Snapshot = {
    toJson: (res: Snapshot) => ({
      '@type': 'http://control.m-ld.org/response/snapshot', ...res,
      time: toTimeString(res.time),
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
        return { ...json, clock: fromTimeString(json.clock) };
      case 'http://control.m-ld.org/response/snapshot':
        return { ...json, time: fromTimeString(json.time), lastHash: Hash.decode(json.time) };
      case 'http://control.m-ld.org/response/revup':
        return { ...json };
    }
    throw new Error('Bad response JSON');
  }
}
export type Response = Response.NewClock | Response.Revup | Response.Snapshot;
