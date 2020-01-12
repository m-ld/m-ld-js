/**
 * Primary interfaces involved in a m-ld implementation
 */
import { TreeClock } from './clocks';
import { Observable } from 'rxjs';
import { Message } from './messages';
import { Triple } from 'rdf-js';
import { Hash } from './hash';
import { Pattern, Subject, Update } from './jsonrql';
import { JsonDeltaBagBlock } from './JsonDelta';

export type DeltaMessage = Message<TreeClock, JsonDelta>;

export type UUID = string;

export interface Meld {
  readonly updates: Observable<DeltaMessage>;
  newClock(): Promise<TreeClock>;
  snapshot(): Promise<Snapshot>;
  revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined>;
}

export interface MeldDelta {
  tid: UUID;
  insert: Triple[];
  delete: Triple[];
  /**
   * Serialisation output of triples is not required to be normalised.
   * For any m-ld delta, there are many possible serialisations.
   * A delta carries its serialisation with it, for journaling and hashing.
   */
  json: JsonDelta
}

export type JsonDelta = {
  [key in Exclude<keyof MeldDelta, 'json'>]: string
}

export interface Snapshot extends Message<TreeClock, Observable<Triple[]>> {
  lastHash: Hash
  readonly updates: Observable<DeltaMessage>;
}

export interface MeldRemotes extends Meld {
  connect(clone: Meld): void;
}

export interface MeldJournalEntry extends DeltaMessage {
  delivered(): void;
}

export interface MeldLocal extends Meld {
  readonly updates: Observable<MeldJournalEntry>;
}

export interface MeldStore {
  transact(request: Pattern): Observable<Subject>;
  follow(after: Hash): Observable<Update>;
}

export type MeldClone = MeldLocal & MeldStore;