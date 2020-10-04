/**
 * Primary interfaces involved in a m-ld engine
 */
import { TreeClock } from './clocks';
import { Observable } from 'rxjs';
import { Message } from './messages';
import { Quad } from 'rdf-js';
import { Hash } from './hash';
import { MsgPack, Future } from './util';
import { LiveValue } from './LiveValue';
import { MeldError } from './MeldError';
import { gzip as gzipCb, gunzip as gunzipCb, InputType } from 'zlib';
const gzip = (input: InputType) => new Promise<Buffer>((resolve, reject) =>
  gzipCb(input, (err, buf) => err ? reject(err) : resolve(buf)));
const gunzip = (input: InputType) => new Promise<Buffer>((resolve, reject) =>
  gunzipCb(input, (err, buf) => err ? reject(err) : resolve(buf)));
const inspect = Symbol.for('nodejs.util.inspect.custom');

const COMPRESS_THRESHOLD_BYTES = 1024;

/**
 * The graph is implicit in m-ld operations.
 */
export type Triple = Omit<Quad, 'graph'>;

export class DeltaMessage implements Message<TreeClock, EncodedDelta> {
  readonly delivered = new Future;

  constructor(
    readonly time: TreeClock,
    readonly data: EncodedDelta) {
  }

  encode(): Buffer {
    return MsgPack.encode({ time: this.time.toJson(), data: this.data });
  }

  static decode(enc: Buffer): DeltaMessage {
    const json = MsgPack.decode(enc);
    const time = TreeClock.fromJson(json.time);
    if (time && json.data)
      return new DeltaMessage(time, json.data);
    else
      throw new MeldError('Bad update');
  }

  size() {
    return this.encode().length;
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
  readonly encoded: EncodedDelta;
}

/**
 * A tuple containing version, tid, delete and insert components of a
 * {@link MeldDelta}. The delete and insert components are UTF-8 encoded JSON-LD
 * strings, which may be GZIP compressed into a Buffer if bigger than a
 * threshold. Intended to be efficiently serialised with MessagePack.
 */
export type EncodedDelta = [0, string, string | Buffer, string | Buffer];

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