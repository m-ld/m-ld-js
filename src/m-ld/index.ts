/**
 * Primary interfaces involved in a m-ld engine
 */
import { TreeClock } from '../clocks';
import { Observable } from 'rxjs';
import { Message } from '../messages';
import { Quad } from 'rdf-js';
import { Hash } from '../hash';
import { Pattern, Subject, Read, Update } from '../dataset/jrql-support';
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

/**
 * A means to access the local clock tick of a transaction response.
 */
export interface HasExecTick {
  /**
   * The promise will be resolved with the local clone clock tick of the
   * transaction. For a read, this will be the tick from which the results were
   * obtained. For a write, this is the tick of the transaction completion. In
   * both cases, the promise resolution is a microtask in the event loop
   * iteration corresponding to the given clone clock tick. Therefore, `follow`
   * can be immediately called and the result subscribed, to be notified of
   * strictly subsequent updates.
   */
  readonly tick: Promise<number>;
}

/**
 * A **m-ld** clone represents domain data to an app.
 *
 * The Javascript clone engine uses a database engine, for which in-memory,
 * on-disk and in-browser persistence options are available (see
 * [Getting&nbsp;Started](/#getting-started)).
 *
 * The raw API methods of this class are augmented with convenience methods for
 * use by an app in the [`MeldApi`](/classes/meldapi.html) class.
 *
 * @see https://spec.m-ld.org/interfaces/meldclone.html
 */
export interface MeldClone {
  /**
   * Actively writes data to, or reads data from, the domain.
   *
   * The transaction executes once, asynchronously, and the results are notified
   * to subscribers of the returned stream.
   *
   * For write requests, the query executes as soon as possible. The result is
   * only completion or error of the returned observable stream – no Subjects
   * are signalled.
   *
   * For read requests, the query executes in response to the first subscription
   * to the returned stream, and subsequent subscribers will share the same
   * results stream.
   *
   * @param request the declarative transaction description
   * @returns an observable stream of subjects. For a write transaction, this is
   * empty, but indicates final completion or error of the transaction.
   */
  transact(request: Pattern): Observable<Subject> & HasExecTick;
  /**
   * Follow updates from the domain. All data changes are signalled through the
   * returned stream, strictly ordered according to the clone's logical clock.
   * The updates can therefore be correctly used to maintain some other view of
   * data, for example in a user interface or separate database.
   *
   * In this engine, the returned stream will signal all updates after the event
   * loop tick on which the stream is subscribed. To ensure that all updates are
   * observed, call this method immediately on receipt of the clone object, and
   * synchronously subscribe.
   *
   * This method will include the notification of 'rev-up' updates after a
   * connect to the domain. To change this behaviour, also subscribe to `status`
   * changes and ignore updates while the status is marked as `outdated`.
   *
   * @returns an observable stream of updates from the domain.
   */
  follow(): Observable<MeldUpdate>;
  /**
   * The current and future status of a clone. This stream is hot and
   * continuous, terminating when the clone closes (and can therefore be used to
   * detect closure).
   */
  readonly status: Observable<MeldStatus> & LiveStatus;
  /**
   * Closes this clone engine gracefully. Using this method ensures that data
   * has been fully flushed to storage and all transactions have been notified
   * to the domain (if this clone is online).
   * @param err used to notify a reason for the closure, for example an
   * application failure, for problem diagnosis.
   */
  close(err?: any): Promise<unknown>;
}

/**
 * Interface provided to a {@link MeldConstraint} to read data during checking
 * and application of a constraint.
 * @param request the Read request, e.g. a Select or Describe
 * @returns an observable stream of found Subjects
 */
export type MeldReader = <R extends Read>(request: R) => Observable<Subject>;

/**
 * A constraint asserts an invariant for data in a clone. When making
 * transactions against the clone, the constraint is 'checked', and violating
 * transactions fail.
 *
 * Constraints are also 'applied' for incoming updates from other clones. This
 * is because a constraint may be violated as a result of data changes in either
 * clone. In this case, the constraint must resolve the violation by application
 * of some rule.
 *
 * > 🚧 *Data constraints are currently an experimental feature. Please
 * > [contact&nbsp;us](mailto:info@m-ld.io) to discuss constraints required for
 * > your use-case.*
 *
 * In this clone engine, constraints are checked and applied for updates prior
 * to their application to the data (the updates are 'provisional'). If the
 * constraint requires to know the final state, it must infer it from the given
 * reader and the update.
 *
 * @see http://m-ld/org/doc/#concurrency
 */
export interface MeldConstraint {
  /**
   * Check the given update does not violate this constraint.
   * @param update the provisional update, prior to application to the data
   * @param read a way to read data from the clone at the time of the update
   * @returns a rejection if the constraint is violated (or fails)
   */
  check(update: MeldUpdate, read: MeldReader): Promise<unknown>;
  /**
   * Applies the constraint to an update being applied to the data. If the
   * update would cause a violation, this method must provide an Update which
   * resolves the violation.
   * @param update the provisional update, prior to application to the data
   * @param read a way to read data from the clone at the time of the update
   * @returns `null` if no violation is found. Otherwise, an Update the resolves
   * the violation so that the constraint invariant is upheld.
   */
  apply(update: MeldUpdate, read: MeldReader): Promise<Update | null>;
}