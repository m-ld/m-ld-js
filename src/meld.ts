/**
 * Primary interfaces involved in a m-ld implementation
 */
import { TreeClock } from './clocks';
import { Observable } from 'rxjs';
import { Message } from './messages';
import { Quad } from 'rdf-js';
import { Hash } from './hash';
import { Pattern, Subject, Update, GroupLike, asGroup } from './jsonrql';

export type DeltaMessage = Message<TreeClock, MeldDelta>;

export type UUID = string;

export interface Meld {
  updates(): Observable<DeltaMessage>;
  newClock(): Promise<TreeClock>;
  snapshot(): Promise<Snapshot>;
  revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage>>;
}

export interface MeldDelta {
  tid: UUID;
  insert: Quad[];
  delete: Quad[];
}

export interface Snapshot extends Message<TreeClock, Observable<Quad[]>> {
  lastHash: Hash
  updates(): Observable<DeltaMessage>;
}

export interface MeldRemotes extends Meld {
  connect(clone: Meld): void;
}

export interface MeldStore {
  transact(request: Pattern): Observable<Subject>;
  follow(after: Hash): Observable<Update>;
}

export type MeldClone = Meld & MeldStore;