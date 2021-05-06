/**
 * Primary interfaces involved in a m-ld engine
 */
import { TreeClock, TreeClockJson } from './clocks';
import { Observable } from 'rxjs';
import { Message } from './messages';
import { MsgPack, Future, sha1Digest } from './util';
import { LiveValue } from './LiveValue';
import { MeldError } from './MeldError';
import { Triple } from './quads';
import { MeldEncoding } from './MeldEncoding';
const inspect = Symbol.for('nodejs.util.inspect.custom');

export class DeltaMessage implements Message<TreeClock, EncodedDelta> {
  readonly delivered = new Future;

  constructor(
    /** Previous public tick from the delta source */
    readonly prev: number,
    /** Encoded update delta */
    readonly data: EncodedDelta,
    /** Message time if you happen to have it, otherwise read from data */
    readonly time = TreeClock.fromJson(data[2]) as TreeClock) {
  }

  encode(): Buffer {
    const { prev, data } = this;
    return MsgPack.encode({ prev, data });
  }

  static decode(enc: Buffer): DeltaMessage {
    const json = MsgPack.decode(enc);
    if (typeof json.prev == 'number' && Array.isArray(json.data))
      return new DeltaMessage(json.prev, json.data);
    else
      throw new MeldError('Bad update');
  }

  get size() {
    return this.encode().length;
  }

  toString() {
    return `${JSON.stringify(this.data)}
    @ ${this.time}, prev ${this.prev}`;
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
  revupFrom(time: TreeClock): Promise<Revup | undefined>;
}

export interface MeldDelta extends Object {
  /**
   * First tick included in update (= time.ticks unless fused)
   */
  readonly from: number;
  /**
   * Update time at the delta source clone
   */
  readonly time: TreeClock;
  /**
   * Inserted triples, reified with transaction IDs iff the encoding is fused.
   */
  readonly inserts: [Triple, TID[]][];
  /**
   * Reified deleted triples, with transaction IDs.
   */
  readonly deletes: [Triple, TID[]][];
  /**
   * Serialisation of triples is not required to be normalised. For any m-ld
   * delta, there are many possible serialisations. A delta carries its
   * serialisation with it, for journaling and hashing.
   */
  readonly encoded: EncodedDelta;
}

/**
 * Transaction IDs are formally mapped to {@link TreeClock}s using the
 * {@link txnId} function.
 */
export type TID = ReturnType<typeof txnId>;

/**
 * Formal mapping from a clock time to a transaction ID. Used in the creation of
 * reified delta deletes and inserts.
 * @param time the clock time
 */
export function txnId(time: TreeClock) {
  return sha1Digest(MsgPack.encode(time.toJson()));
}

/**
 * A tuple containing encoding
 * - `0`: version,
 * - `1`: from,
 * - `2`: time as JSON,
 * - `3`: delete as gzip Buffer or JSON string, and
 * - `4`: insert as gzip Buffer or JSON string
 *
 * components of a {@link MeldDelta}. The delete and insert components are UTF-8
 * encoded JSON-LD strings, which may be GZIP compressed into a Buffer if bigger
 * than a threshold. Intended to be efficiently serialised with MessagePack.
 */
export type EncodedDelta = [2, number, TreeClockJson, string | Buffer, string | Buffer];

export interface Recovery {
  readonly lastTime: TreeClock;
  readonly updates: Observable<DeltaMessage>;
}

export interface Revup extends Recovery {
}

export interface Snapshot extends Recovery {
  /**
   * An observable of reified quad arrays. Reified quads include their observed
   * TIDs. Arrays for batching (sender decides array size).
   */
  readonly quads: Observable<Triple[]>;
}

export interface MeldRemotes extends Meld {
  setLocal(clone: MeldLocal | null): void;
}

export interface MeldLocal extends Meld {
  readonly id: string;
  readonly encoding: MeldEncoding;
}