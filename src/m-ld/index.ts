/**
 * Primary interfaces involved in a m-ld engine
 */
import { TreeClock } from '../clocks';
import { Observable } from 'rxjs';
import { Message } from '../messages';
import { Quad } from 'rdf-js';
import { Hash } from '../hash';
import { Pattern, Subject } from '../dataset/jrql-support';
import { Future } from '../util';
import { LiveValue } from '../LiveValue';
import { LiveStatus, MeldUpdate as BaseUpdate, MeldStatus } from '@m-ld/m-ld-spec';
const inspect = Symbol.for('nodejs.util.inspect.custom');

// Unchanged from m-ld-spec
export { LiveStatus, MeldStatus };
  
/**
 * The graph is implicit in m-ld operations.
 */
export type Triple = Omit<Quad, 'graph'>;

export class DeltaMessage implements Message<TreeClock, JsonDelta> {
  readonly delivered = new Future;

  constructor(
    readonly time: TreeClock,
    readonly data: JsonDelta) {
  }

  toJson(): object {
    return { time: this.time.toJson(), data: this.data };
  }

  static fromJson(json: any): DeltaMessage | undefined {
    const time = TreeClock.fromJson(json.time);
    if (time && json.data)
      return new DeltaMessage(time, json.data);
  }

  toString() {
    return `${JSON.stringify(this.data)}
    @ ${this.time}`;
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();
}

export type UUID = string;

export interface Meld {
  /**
   * Updates from this Meld. The stream is hot, continuous and multicast.
   * Completion or an error means that this Meld has closed.
   * @see live
   */
  readonly updates: Observable<DeltaMessage>;
  /**
   * Liveness of this Meld. To be 'live' means that it is able to collaborate
   * with newly starting clones via snapshot & rev-up. A value of null indicates
   * unknown (e.g. starting or disconnected). The stream is hot, continuous and
   * multicast, but will also always emit the current state to new subscribers
   * (Rx BehaviorSubject). Completion or an error means that this Meld has
   * closed.
   * @see updates
   */
  readonly live: LiveValue<boolean | null>;

  newClock(): Promise<TreeClock>;
  snapshot(): Promise<Snapshot>;
  revupFrom(time: TreeClock): Promise<Observable<DeltaMessage> | undefined>;
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

export interface Snapshot {
  readonly lastTime: TreeClock;
  /**
   * An observable of reified quad arrays. Reified quads include their observed
   * TIDs. Arrays for batching (sender decides array size).
   */
  readonly quads: Observable<Triple[]>;
  /**
   * All observed TIDs, for detecting duplicates.
   */
  readonly tids: Observable<UUID[]>;
  readonly lastHash: Hash;
  readonly updates: Observable<DeltaMessage>;
}

export interface MeldRemotes extends Meld {
  setLocal(clone: MeldLocal | null): void;
}

export interface MeldLocal extends Meld {
  readonly id: string;
}

export interface MeldUpdate extends BaseUpdate {
  '@delete': Subject[];
  '@insert': Subject[];
}

export interface MeldClone {
  transact(request: Pattern): Observable<Subject>;
  follow(after?: number): Observable<MeldUpdate>;
  readonly status: Observable<MeldStatus> & LiveStatus;
  close(err?: any): Promise<void>;
}
