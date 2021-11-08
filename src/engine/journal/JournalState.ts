import { GlobalClock, GlobalClockJson, TreeClock, TreeClockJson } from '../clocks';
import { Kvps } from '../dataset';
import { JournalEntry } from './JournalEntry';
import { MeldOperation } from '../MeldEncoding';
import { EncodedOperation } from '../index';
import { Journal, tickKey } from '.';

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
}

export interface EntryBuilder {
  next(operation: MeldOperation, localTime: TreeClock): this;
  entries: JournalEntry[];
  commit: Kvps;
}

/** Immutable expansion of JournalJson */
export class JournalState {
  static fromJson(data: Journal, json: JournalStateJson) {
    const time = TreeClock.fromJson(json.time);
    const gwc = GlobalClock.fromJSON(json.gwc);
    return new JournalState(data, json.start, time, gwc);
  }

  constructor(
    private readonly journal: Journal,
    readonly start: number,
    readonly time: TreeClock,
    readonly gwc: GlobalClock) {
  }

  get json(): JournalStateJson {
    return { start: this.start, time: this.time.toJSON(), gwc: this.gwc.toJSON() };
  }

  commit: Kvps = this.journal.saveState(this);

  withTime(localTime: TreeClock, gwc?: GlobalClock): JournalState {
    return new JournalState(this.journal, this.start, localTime, gwc ?? this.gwc);
  }

  builder(): EntryBuilder {
    const state = this;
    return new class {
      private localTime = state.time;
      private gwc = state.gwc;
      entries: JournalEntry[] = [];

      next(operation: MeldOperation, localTime: TreeClock) {
        const prevTicks = this.gwc.getTicks(operation.time);
        const prevTid = this.gwc.tid(operation.time);
        this.entries.push(JournalEntry.fromOperation(state.journal,
          tickKey(localTime.ticks), [prevTicks, prevTid], operation));
        this.gwc = this.gwc.update(operation.time);
        this.localTime = localTime;
        return this;
      }

      /** Commits the built journal entries to the journal */
      commit: Kvps = async batch => {
        for (let entry of this.entries)
          entry.commitTail(batch);

        state.withTime(this.localTime, this.gwc).commit(batch);
      };
    };
  }

  applicableOperation(op: MeldOperation): Promise<MeldOperation> {
    return this.journal.withLockedHistory(async () => {
      // Cut away stale parts of an incoming fused operation.
      // Optimisation: no need to cut if incoming is not fused.
      if (op.from < op.time.ticks) {
        const seenTicks = this.gwc.getTicks(op.time);
        // Seen ticks >= op.from (otherwise we would not be here)
        const seenTid = op.time.ticked(seenTicks).hash();
        const seenOp = await this.journal.operation(seenTid);
        if (seenOp != null)
          return { return: await seenOp.cutSeen(op) };
      }
      return { return: op };
    });
  }

  latestOperations(): Promise<EncodedOperation[]> {
    return this.journal.withLockedHistory(async () => {
      // For each latest op, emit a fusion of all contiguous ops up to it
      const ops = await Promise.all([...this.gwc.tids()].map(
        async tid => (await this.journal.operation(tid))?.fusedPast()));
      return { return: ops.filter((op): op is EncodedOperation => op != null) };
    });
  }
}