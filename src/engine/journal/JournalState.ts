import { GlobalClock, GlobalClockJson, TreeClock, TreeClockJson } from '../clocks';
import { Kvps } from '../dataset';
import { JournalEntry } from './JournalEntry';
import { EncodedOperation } from '../index';
import { Journal, TICK_KEY_GEN } from '.';
import { EntryReversion, MeldOperation } from '../MeldOperation';
import { Attribution } from '../../api';

interface JournalStateJson {
  /**
   * First known tick of the local clock. Journal entries will exist for
   * subsequent ticks. Operations may be retained for transactions prior to
   * this.
   */
  start: number;
  /**
   * Current local clock time, including internal ticks.
   */
  time: TreeClockJson;
  /**
   * JSON-encoded public clock time ('global wall clock' or 'Great Westminster
   * Clock'). This has latest public ticks seen for all processes (not internal
   * ticks), unlike an entry time, which may be causally related to older
   * messages from third parties, and the journal time, which has internal ticks
   * for the local clone identity.
   */
  gwc: GlobalClockJson;
  /**
   * Time of the last _agreement_. An incoming operation with any process tick
   * less than this time will be ignored by the dataset.
   */
  agreed: TreeClockJson;
}

/**
 * Marks a remote identity as blocked in the GWC. Note the use of characters
 * that do not appear in Base64 alphabet.
 */
const BLOCKED = '!blocked!';

export interface EntryBuilder {
  next(
    operation: MeldOperation,
    reversion: EntryReversion,
    localTime: TreeClock,
    attribution: Attribution | null
  ): this;
  void(entry: JournalEntry): this;
  block(remoteTime: TreeClock): this;
  deleteEntries: JournalEntry[];
  appendEntries: JournalEntry[];
  state: JournalState;
  commit: Kvps;
}

/** Immutable expansion of JournalJson */
export class JournalState {
  static fromJson(data: Journal, json: JournalStateJson) {
    const time = TreeClock.fromJson(json.time);
    const gwc = GlobalClock.fromJSON(json.gwc);
    const agreed = TreeClock.fromJson(json.agreed);
    return new JournalState(data, json.start, time, gwc, agreed);
  }

  constructor(
    private readonly journal: Journal,
    readonly start: number,
    readonly time: TreeClock,
    readonly gwc: GlobalClock,
    readonly agreed: TreeClock
  ) {
  }

  get json(): JournalStateJson {
    return {
      start: this.start,
      time: this.time.toJSON(),
      gwc: this.gwc.toJSON(),
      agreed: this.agreed.toJSON()
    };
  }

  commit: Kvps = this.journal.saveState(this);

  withTime(localTime: TreeClock, gwc?: GlobalClock, agreed?: TreeClock): JournalState {
    return new JournalState(
      this.journal,
      this.start,
      localTime,
      gwc ?? this.gwc,
      agreed ?? this.agreed);
  }

  builder(): EntryBuilder {
    return new (class implements EntryBuilder {
      deleteEntries: JournalEntry[] = [];
      appendEntries: JournalEntry[] = [];

      constructor(
        public state: JournalState
      ) {}

      next(
        operation: MeldOperation,
        reversion: EntryReversion,
        localTime: TreeClock,
        attribution: Attribution | null
      ) {
        const prevTicks = this.state.gwc.getTicks(operation.time);
        const prevTid = this.state.gwc.tid(operation.time);
        if (prevTid === BLOCKED)
          throw new RangeError('Trying to process operation from a blocked remote!');
        this.appendEntries.push(JournalEntry.fromOperation(
          this.state.journal,
          TICK_KEY_GEN.tickKey(localTime.ticks),
          [prevTicks, prevTid],
          operation,
          reversion,
          attribution
        ));
        this.state = this.state.withTime(
          localTime,
          this.state.gwc.set(operation.time),
          operation.agreed != null ?
            operation.time.ticked(operation.agreed.tick) : undefined
        );
        return this;
      }

      void(entry: JournalEntry) {
        if (entry.operation.agreed != null)
          throw new RangeError('Cannot void an agreement');
        // The entry's tick is now internal to its process, so the GWC must be
        // reset to the previous external tick and tid for the process.
        const [prevTick, prevTid] = entry.prev;
        const prevTime = entry.operation.time.ticked(prevTick);
        this.deleteEntries.push(entry);
        this.state = this.state.withTime(
          this.state.time.ticked(prevTime),
          this.state.gwc.set(prevTime, prevTid));
        return this;
      }

      block(remoteTime: TreeClock): this {
        this.state = this.state.withTime(
          this.state.time, this.state.gwc.set(remoteTime, BLOCKED));
        return this;
      }

      /** Commits the changed journal */
      commit: Kvps = async batch => {
        this.state.journal.spliceEntries(
          this.deleteEntries.map(entry => entry.index),
          this.appendEntries,
          { appending: true }
        )(batch);
        this.state.commit(batch);
      };
    })(this);
  }

  isBlocked(remoteTime: TreeClock) {
    return this.gwc.tid(remoteTime) === BLOCKED;
  }

  /**
   * Uses the journal to calculate the applicable operation based on the
   * incoming operation:
   * 1. If the incoming operation is a fusion for which we already have some
   * prefix, the prefix is cut away leaving only the part we have not seen.
   * 1. If the incoming operation pre-dates the last agreement we have seen, it
   * is ignored.
   *
   * @param op the incoming operation
   * @returns `op` if no changes are required, or an operation representing the
   * un-applied suffix, or `null` if `op` should be ignored.
   */
  applicableOperation(op: MeldOperation): Promise<MeldOperation> {
    return this.journal.withLockedHistory(async () => {
      // Cut away stale parts of an incoming fused operation.
      // Optimisation: no need to cut if incoming is not fused.
      if (op.from < op.time.ticks) {
        const seenTicks = this.gwc.getTicks(op.time);
        // Seen ticks >= op.from (otherwise we would not be here)
        const seenTid = op.time.ticked(seenTicks).hash;
        const seenOp = await this.journal.operation(seenTid);
        if (seenOp != null)
          op = await seenOp.cutSeen(op);
      }
      return { return: op };
    });
  }

  latestOperations(): Promise<EncodedOperation[]> {
    return this.journal.withLockedHistory(async () => {
      // For each latest op, emit a fusion of all contiguous ops up to it
      const ops = await Promise.all([...this.gwc.tids()].map(async tid => {
        // Every TID in the GWC except genesis and blocked should be represented.
        // Latest operation history for blocked clones is not necessary because
        // no further operations will be accepted anyway.
        if (tid !== TreeClock.GENESIS.hash && tid !== BLOCKED) {
          const op = await this.journal.operation(tid, 'require');
          return op.fusedPast();
        }
      }));
      return { return: ops.filter((op): op is EncodedOperation => op != null) };
    });
  }
}