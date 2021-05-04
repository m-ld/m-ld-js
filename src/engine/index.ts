/**
 * Primary interfaces involved in a m-ld engine
 */
import { TreeClock } from './clocks';
import { Observable } from 'rxjs';
import { Message } from './messages';
import { MsgPack, Future, sha1Digest } from './util';
import { LiveValue } from './LiveValue';
import { MeldError } from './MeldError';
import { gzip as gzipCb, gunzip as gunzipCb, InputType } from 'zlib';
import { Triple } from './quads';
import { MeldEncoding } from './MeldEncoding';
const gzip = (input: InputType) => new Promise<Buffer>((resolve, reject) =>
  gzipCb(input, (err, buf) => err ? reject(err) : resolve(buf)));
const gunzip = (input: InputType) => new Promise<Buffer>((resolve, reject) =>
  gunzipCb(input, (err, buf) => err ? reject(err) : resolve(buf)));
const inspect = Symbol.for('nodejs.util.inspect.custom');

const COMPRESS_THRESHOLD_BYTES = 1024;

export class DeltaMessage implements Message<TreeClock, EncodedDelta> {
  readonly delivered = new Future;

  constructor(
    /** Previous public tick from the delta source */
    readonly prev: number,
    /** Update time at the delta source */
    readonly time: TreeClock,
    /** Encoded update delta */
    readonly data: EncodedDelta,
    /** First tick included in update (= time.ticks unless fused) */
    readonly from = time.ticks) {
    const [, fused] = data;
    if (from > time.ticks || fused !== (from < time.ticks))
      throw new MeldError('Bad update');
  }

  encode(): Buffer {
    const { prev, from, time, data } = this;
    return MsgPack.encode({ prev, from, time: time.toJson(), data });
  }

  static decode(enc: Buffer): DeltaMessage {
    const json = MsgPack.decode(enc);
    const time = TreeClock.fromJson(json.time);
    if (time && json.data)
      return new DeltaMessage(json.prev, time, json.data, json.from ?? time.ticks);
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
   * Inserted triples, reified with transaction IDs iff the encoding is fused.
   */
  readonly inserts: [Triple, string[]][];
  /**
   * Reified deleted triples, with transaction IDs.
   */
  readonly deletes: [Triple, string[]][];
  /**
   * Serialisation output of triples is not required to be normalised.
   * For any m-ld delta, there are many possible serialisations.
   * A delta carries its serialisation with it, for journaling and hashing.
   */
  readonly encoded: EncodedDelta;
}

/**
 * Formal mapping from a clock time to a transaction ID. Used in the creation of
 * reified delta deletes and inserts.
 * @param time the clock time
 */
export function txnId(time: TreeClock): string {
  return sha1Digest(MsgPack.encode(time.toJson()));
}

/**
 * A tuple containing encoding
 * - `0`: version,
 * - `1`: fused flag (multiple transactions; has reified insert),
 * - `2`: delete, and
 * - `3`: insert
 *
 * components of a {@link MeldDelta}. The delete and insert components are UTF-8
 * encoded JSON-LD strings, which may be GZIP compressed into a Buffer if bigger
 * than a threshold. Intended to be efficiently serialised with MessagePack.
 */
export type EncodedDelta = [2, boolean, string | Buffer, string | Buffer];

export namespace EncodedDelta {
  export async function encode(json: any): Promise<Buffer | string> {
    const stringified = JSON.stringify(json);
    return stringified.length > COMPRESS_THRESHOLD_BYTES ?
      gzip(stringified) : stringified;
  }

  export async function decode(enc: string | Buffer): Promise<any> {
    if (typeof enc != 'string')
      enc = (await gunzip(enc)).toString();
    return JSON.parse(enc);
  }
}

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