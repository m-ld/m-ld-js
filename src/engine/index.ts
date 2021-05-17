/**
 * Primary interfaces involved in a m-ld engine
 */
import { TreeClock, TreeClockJson } from './clocks';
import { Observable } from 'rxjs';
import { Message } from './messages';
import { MsgPack, Future } from './util';
import { LiveValue } from './LiveValue';
import { MeldError } from './MeldError';
import { Triple } from './quads';
import { MeldEncoder } from './MeldEncoding';
const inspect = Symbol.for('nodejs.util.inspect.custom');

export class OperationMessage implements Message<TreeClock, EncodedOperation> {
  readonly delivered = new Future;

  constructor(
    /** Previous public tick from the operation source */
    readonly prev: number,
    /** Encoded update operation */
    readonly data: EncodedOperation,
    /** Message time if you happen to have it, otherwise read from data */
    readonly time = TreeClock.fromJson(data[2]) as TreeClock) {
  }

  encode(): Buffer {
    const { prev, data } = this;
    return MsgPack.encode({ prev, data });
  }

  static decode(enc: Buffer): OperationMessage {
    const json = MsgPack.decode(enc);
    if (typeof json.prev == 'number' && Array.isArray(json.data))
      return new OperationMessage(json.prev, json.data);
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
  readonly updates: Observable<OperationMessage>;
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

/**
 * A tuple containing encoding components of a {@link MeldOperation}. The delete
 * and insert components are UTF-8 encoded JSON-LD strings, which may be GZIP
 * compressed into a Buffer if bigger than a threshold. Intended to be
 * efficiently serialised with MessagePack.
 */
export type EncodedOperation = [
  version: 2,
  /** first tick of causal time range */
  from: number,
  /** time as JSON */
  time: TreeClockJson,
  /** delete as gzip Buffer or JSON string */
  deletes: string | Buffer,
  /** insert as gzip Buffer or JSON string */
  inserts: string | Buffer
];

export interface Recovery {
  readonly lastTime: TreeClock;
  readonly updates: Observable<OperationMessage>;
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
  readonly encoder: MeldEncoder;
}