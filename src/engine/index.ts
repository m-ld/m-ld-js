/**
 * Primary interfaces involved in a m-ld engine
 */
import { GlobalClock, TreeClock, TreeClockJson } from './clocks';
import { Observable } from 'rxjs';
import { Message } from './messages';
import { Future, MsgPack } from './util';
import { LiveValue } from './LiveValue';
import { MeldError } from './MeldError';
import { MeldReadState, StateProc } from '../api';
import { levels } from 'loglevel';
import { MeldEncoder } from './MeldEncoding';

const inspect = Symbol.for('nodejs.util.inspect.custom');

/**
 * An operation on domain data, expressed such that it can be causally-ordered using its
 * logical clock time, with a message service.
 */
export class OperationMessage implements Message<TreeClock, EncodedOperation> {
  readonly delivered = new Future;
  private _encoded: Buffer;

  constructor(
    /** Previous public tick from the operation source */
    readonly prev: number,
    /** Encoded update operation */
    readonly data: EncodedOperation,
    /** Message time if you happen to have it, otherwise read from data */
    readonly time = TreeClock.fromJson(data[2])
  ) {
  }

  get encoded(): Buffer {
    if (this._encoded == null) {
      const { prev, data } = this;
      this._encoded = MsgPack.encode({ prev, data });
    }
    return this._encoded;
  }

  static decode(enc: Buffer): OperationMessage {
    const json = MsgPack.decode(enc);
    if (typeof json.prev == 'number' && Array.isArray(json.data))
      return new OperationMessage(json.prev, json.data);
    else
      throw new MeldError('Bad update');
  }

  get size() {
    return this.encoded.length;
  }

  toString(logLevel: number = levels.INFO) {
    const [v, from, time, updateData, encoding] = this.data;
    const update = logLevel <= levels.DEBUG ?
      encoding.includes(BufferEncoding.SECURE) ? '---ENCRYPTED---' :
      MeldEncoder.jsonFromBuffer(updateData, encoding) :
      { length: updateData.length, encoding };
    return `${JSON.stringify({ v, from, time, update })}
    @ ${this.time}, prev ${this.prev}`;
  }

  // v8(chrome/nodejs) console
  [inspect] = () => this.toString();
}

/**
 * Primary internal m-ld engine-to-engine interface, used both as an interface of the local clone,
 * but also, symmetrically, to present remote clones to the local clone. Each member is similarly
 * used symmetrically: outgoing vs. incoming operations; liveness of the local clone vs.
 * is-anyone-out-there; and providing recovery to a peer vs. recovering from a peer.
 */
export interface Meld {
  /**
   * Operations from this Meld. The stream is hot, continuous and multicast.
   * Completion or an error means that this Meld has closed.
   * @see live
   */
  readonly operations: Observable<OperationMessage>;
  /**
   * Liveness of this Meld. To be 'live' means that it is able to collaborate
   * with recovering clones via snapshot & rev-up. A value of null indicates
   * unknown (e.g. starting or disconnected). The stream is hot, continuous and
   * multicast, but will also always emit the current state to new subscribers
   * (Rx BehaviorSubject). Completion or an error means that this Meld has
   * closed.
   * @see operations
   */
  readonly live: LiveValue<boolean | null>;
  /**
   * Mint a new clock, with a unique identity in the domain. For a local clone,
   * this method forks the clone's clock. For a set of remotes, the request is
   * forwarded to one remote clone (decided by the implementation) which will
   * call the method locally.
   */
  newClock(): Promise<TreeClock>;
  /**
   * Get a snapshot of all the data in the domain. For a local clone, this
   * method provides the local state. For a set of remotes, the request is
   * forwarded to one remote clone (decided by the implementation) which will
   * call the method locally.
   *
   * @param state readable prior state, used to inspect metadata
   */
  snapshot(state: MeldReadState): Promise<Snapshot>;
  /**
   * 'Rev-up' by obtaining recent operations for the domain. For a local clone,
   * this method provides the operations from the local journal. For a set of
   * remotes, the request is forwarded to one remote clone (decided by the
   * implementation) which will call the method locally.
   *
   * @param time the time of the most recent message seen by the requester; no older messages will
   *   be provided by the implementer.
   * @param state readable prior state, used to inspect metadata
   * @returns Revup containing the recent operations; or `undefined` if the implementer no longer
   *   has enough entries in its journal to ensure that all required operations are relayed. This
   *   can legitimately happen if the implementer has truncated its journal, to save resources.
   */
  revupFrom(time: TreeClock, state: MeldReadState): Promise<Revup | undefined>;
}

/**
 * Encodings that take object or buffer input and produce a buffer.
 */
export enum BufferEncoding {
  /** JSON string encoding, for testing only */
  JSON,
  /** MessagePack encoding from JSON object */
  MSGPACK,
  /** Gzip */
  GZIP,
  /** Processed by transport security */
  SECURE
}

/**
 * A tuple containing encoding components of a {@link MeldOperation}. The delete
 * and insert components JSON-LD objects, encoded as required, which may include
 * compression and encryption. Intended to be efficiently serialised with
 * MessagePack.
 */
export type EncodedOperation = [
  /**
   * @since 1
   */
  version: 4,
  /**
   * First tick of causal time range. If this is less than `time.ticks`, this
   * operation is a fusion of multiple operations.
   * @since 2
   */
  from: number,
  /**
   * Time as JSON. If this operation is a fusion, this is the _last_ time in the
   * fusion's range.
   * @since 1
   */
  time: TreeClockJson,
  /**
   * A tuple `[delete: object, insert: object]` encoded as per `encoding`
   * @since 3
   */
  update: Buffer,
  /**
   * Encodings applied to the update
   * @since 3
   */
  encoding: BufferEncoding[],
  /**
   * The _last_ tick in this operation's range that was an agreement, and any
   * proof required for applicable agreement conditions
   * @since 4
   */
  agreed?: [number, any]
];

/**
 * Common components of a clone snapshot and rev-up – both of which provide a
 * 'recovery' mechanism for clones from a 'collaborator' clone. Recovery is
 * required (non-exhaustively):
 * - For a brand new clone (always a snapshot)
 * - If a clone has been partitioned from the network and is now back online
 * - If a clone has detected that it has missed an operation message
 */
export interface Recovery {
  /**
   * 'Global Wall Clock' (or 'Great Westminster Clock'), containing the most
   * recent ticks and transaction IDs seen by the recovery collaborator.
   */
  readonly gwc: GlobalClock;
  /**
   * Operation messages seen by the collaborator. For a rev-up, these include
   * messages from the collaborator's journal. However for both rev-up and
   * snapshot, a collaborator also relays relevant operation messages it
   * observes during the recovery process.
   */
  readonly updates: Observable<OperationMessage>;
}

/**
 * A 'rev-up' is a recovery which includes the requested journal messages from the
 * collaborator in its {@link Recovery.updates}.
 * @see Meld.revupFrom
 */
export interface Revup extends Recovery {
}

/**
 * A snapshot is a recovery which includes all domain data from the collaborator.
 * @see Meld.snapshot
 */
export interface Snapshot extends Recovery {
  /**
   * All data in the snapshot.
   * @see Snapshot.Datum
   */
  readonly data: Observable<Snapshot.Datum>;
  /**
   * Time of the last agreement to contribute to the snapshot.
   */
  readonly agreed: TreeClock;
}

export namespace Snapshot {
  /**
   * Reified triples with their observed TIDs
   * (sender decides how many triples per emission)
   */
  export type Inserts = { inserts: Buffer, encoding: BufferEncoding[] };
  /**
   * A latest operation from a remote clone
   */
  export type Operation = { operation: EncodedOperation };
  /**
   * Data is either reified triples with their observed TIDs as JSON-LD, or
   * a latest operation from a remote clone.
   */
  export type Datum = Inserts | Operation;
}

/**
 * 'Remotes' variant of {@link Meld} representing remote domain clones to the
 * {@link MeldLocal local clone}.
 */
export interface MeldRemotes extends Meld {
  /**
   * Bootstrap method, setting the current local clone so that the remotes can
   * relay information to and from it, as necessary. For example, in the case
   * that a remote clone wishes to recover from the local clone.
   *
   * @param clone the local clone; `null` indicates that the local clone has gone away (probably
   *   closed).
   */
  setLocal(clone: MeldLocal | null): void;
}

/**
 * Local variant of {@link Meld} representing the local clone to remote clones on the same domain.
 */
export interface MeldLocal extends Meld {
  /**
   * Make a bounded read-only use of the state of the local clone. This is used
   * in the implementation of the m-ld protocol to inspect metadata, for example
   * for transport security, when external events happen.
   */
  withLocalState<T>(procedure: StateProc<MeldReadState, T>): Promise<T>;
}

export { CloneExtensions } from './CloneExtensions';