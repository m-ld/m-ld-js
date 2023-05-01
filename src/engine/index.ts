/**
 * Primary interfaces involved in a m-ld engine
 */
import { GlobalClock, TreeClock, TreeClockJson } from './clocks';
import { Observable } from 'rxjs';
import * as MsgPack from './msgPack';
import { LiveValue } from './api-support';
import { Attribution, MeldPreUpdate, MeldReadState, StateProc, UpdateProc } from '../api';
import { Message } from './messages';

/**
 * Primary internal m-ld engine-to-engine interface, used both as an interface
 * of the local clone, but also, symmetrically, to present remote clones to the
 * local clone. Each member is similarly used symmetrically: outgoing vs.
 * incoming operations; liveness of the local clone vs. is-anyone-out-there; and
 * providing recovery to a peer vs. recovering from a peer.
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
 * An operation on domain data, expressed such that it can be causally-ordered
 * using its logical clock time, with a message service.
 */
export interface OperationMessage extends Message<TreeClock, EncodedOperation> {
  /**
   * Previous public tick from the operation source. Used to detect FIFO
   * violations in message delivery.
   */
  readonly prev: number;
  /**
   * Attribution of the operation to a security principal
   */
  readonly attr: Attribution | null;
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
   * Encodings applied to the update, in the order of application
   * @since 3
   */
  encoding: BufferEncoding[],
  /**
   * The identity Iri compacted against the canonical domain context of the
   * principal originally responsible for this operation, if available; or
   * `null`. Note that this principal is not necessarily the same as the
   * attribution of a sent operation message if the operation has been processed
   * in some way, such as with a fusion.
   *
   * @see AppPrincipal
   * @since 4
   */
  principalId: string | null,
  /**
   * The _last_ tick in this operation's range that was an agreement, and any
   * proof required for applicable agreement conditions, or `null` if this
   * operation does not contain an agreement.
   * @see OperationAgreedSpec
   * @since 4
   */
  agreed: [number, any] | null
];

export namespace EncodedOperation {
  /** @internal utility to strongly key into EncodedOperation */
  export enum Key {
    // noinspection JSUnusedGlobalSymbols
    version,
    from,
    time,
    update,
    encoding,
    principalId,
    agreed
  }

  export const toBuffer: (op: EncodedOperation) => Buffer = MsgPack.encode;
  export const fromBuffer: (buffer: Buffer) => EncodedOperation = MsgPack.decode;
}

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
  /**
   * Cancel this recovery, potentially before any observables have been
   * subscribed. This will release any open queries or locks.
   * @param cause the reason for cancellation, used for logging
   */
  cancel(cause?: Error): void;
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
   * A latest operation from a remote clone, including any possible fused
   * history. This is necessary so that when receiving a rev-up fusion, it's
   * possible to 'cut' already-processed history from that fusion.
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
export interface MeldLocal extends Meld, ReadLatchable {
  /**
   * Make a bounded read-only use of the state of the local clone. This is used
   * in the implementation of the m-ld protocol to inspect metadata, for example
   * for transport security, when external events happen.
   */
  readonly latch: ReadLatchable['latch'];
}

export interface ReadLatchable {
  /**
   * Make a bounded read-only use of state. The state is guaranteed not to
   * change until the procedure's returned promise has settled.
   */
  latch<T>(procedure: StateProc<MeldReadState, T>): Promise<T>;
}

/**
 * A component that needs to be kept abreast of state changes
 * @internal
 */
export interface StateManaged {
  /**
   * Initialises the component against the given clone state. This method could
   * be used to read significant state into memory for the efficient
   * implementation of a component's function.
   */
  readonly initialise?: StateProc;
  /**
   * Called to inform the component of an update to the state, _after_ it has
   * been applied. If available, this procedure will be called for every state
   * after that passed to {@link initialise}.
   */
  readonly onUpdate?: UpdateProc<MeldPreUpdate>;
}