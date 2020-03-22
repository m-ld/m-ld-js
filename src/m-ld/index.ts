/**
 * Primary interfaces involved in a m-ld implementation
 */
import { TreeClock } from '../clocks';
import { Observable } from 'rxjs';
import { Message } from '../messages';
import { Triple } from 'rdf-js';
import { Hash } from '../hash';
import { Pattern, Subject, Group, DeleteInsert } from './jsonrql';

export type DeltaMessage = Message<TreeClock, JsonDelta>;

export namespace DeltaMessage {
  export function toJson(msg: DeltaMessage): any {
    return { time: msg.time.toJson(), data: msg.data };
  }

  export function fromJson(json: any): DeltaMessage | undefined {
    const time = TreeClock.fromJson(json.time);
    if (time && json.data)
      return { time, data: json.data };
  }
}

export type UUID = string;

export interface Meld {
  readonly updates: Observable<DeltaMessage>;
  newClock(): Promise<TreeClock>;
  snapshot(): Promise<Snapshot>;
  revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined>;
}

export interface MeldDelta {
  readonly tid: UUID;
  readonly insert: Triple[];
  readonly delete: Triple[];
  /**
   * Serialisation output of triples is not required to be normalised.
   * For any m-ld delta, there are many possible serialisations.
   * A delta carries its serialisation with it, for journaling and hashing.
   */
  readonly json: JsonDelta
}

export type JsonDelta = {
  [key in Exclude<keyof MeldDelta, 'json'>]: string
}

export interface Snapshot extends Message<TreeClock, Observable<Triple[]>> {
  readonly lastHash: Hash;
  readonly updates: Observable<DeltaMessage>;
}

export interface MeldRemotes extends Meld {
  connect(clone: MeldLocal): void;
}

export interface MeldJournalEntry extends DeltaMessage {
  delivered(): void;
}

export interface MeldLocal extends Meld {
  readonly id: string;
  readonly updates: Observable<MeldJournalEntry>;
}

export interface MeldStore {
  transact(request: Pattern): Observable<Subject>;
  follow(after?: number): Observable<DeleteInsert<Group>>;
  close(err?: any): void;
}

export type MeldClone = MeldLocal & MeldStore;