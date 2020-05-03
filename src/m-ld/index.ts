/**
 * Primary interfaces involved in a m-ld implementation
 */
import { TreeClock } from '../clocks';
import { Observable } from 'rxjs';
import { Message } from '../messages';
import { Triple, Quad } from 'rdf-js';
import { Hash } from '../hash';
import { Pattern, Subject, Group, DeleteInsert } from './jsonrql';

export type DeltaMessage = Message<TreeClock, JsonDelta> & Object;

export namespace DeltaMessage {
  export function toJson(msg: DeltaMessage): any {
    return { time: msg.time.toJson(), data: msg.data };
  }

  export function fromJson(json: any): DeltaMessage | undefined {
    const time = TreeClock.fromJson(json.time);
    if (time && json.data)
      return { time, data: json.data, toString };
  }

  export function toString(this: DeltaMessage) {
    return `${JSON.stringify(this.data)}
    @ ${this.time}`;
  }
}

export type UUID = string;

export interface Meld {
  /**
   * Updates from this Meld. The stream is hot and continuous.
   * Completion or an error means that this Meld has closed.
   * @see online
   */
  readonly updates: Observable<DeltaMessage>;
  /**
   * Online-ness of this Meld. To be 'online' means that it is able
   * to collaborate with newly starting clones via snapshot & rev-up.
   * The stream is hot and continuous, but will also always emit
   * the current state to new subscribers (Rx BehaviourSubject).
   * Completion or an error means that this Meld has closed.
   * @see updates
   */
  readonly online: Observable<boolean>;

  newClock(): Promise<TreeClock>;
  snapshot(): Promise<Snapshot>;
  revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined>;
}

export interface MeldDelta extends Object {
  readonly tid: UUID;
  readonly insert: Triple[];
  readonly delete: Triple[];
  /**
   * Serialisation output of triples is not required to be normalised.
   * For any m-ld delta, there are many possible serialisations.
   * A delta carries its serialisation with it, for journaling and hashing.
   */
  readonly json: JsonDelta;
}

export type JsonDelta = {
  [key in 'tid' | 'insert' | 'delete']: string;
}

/**
 * An observable of quad arrays. Quads, because it must include the TID graph;
 * arrays for batching (sender decides array size).
 */
export interface Snapshot extends Message<TreeClock, Observable<Quad[]>> {
  readonly lastHash: Hash;
  readonly updates: Observable<DeltaMessage>;
}

export interface MeldRemotes extends Meld {
  setLocal(clone: MeldLocal): void;
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
  close(err?: any): Promise<void>;
}

export type MeldClone = MeldLocal & MeldStore;