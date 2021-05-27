import { GlobalClock, GlobalClockJson, TreeClock, TreeClockJson } from '../clocks';
import { Kvps } from '../dataset';
import { JournalEntry } from './JournalEntry';
import { MeldOperation } from '../MeldEncoding';
import { from, Observable } from 'rxjs';
import { EncodedOperation } from '../index';
import { filter, mergeMap } from 'rxjs/operators';
import { JournalOperation } from './JournalOperation';
import { Journal } from '.';

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

interface EntryBuilder {
  next(operation: MeldOperation, localTime: TreeClock): this;
  commit: Kvps;
}

/** Immutable expansion of JournalJson */
export class JournalState {
  static fromJson(data: Journal, json: JournalStateJson) {
    const time = TreeClock.fromJson(json.time);
    const gwc = GlobalClock.fromJson(json.gwc);
    return new JournalState(data, json.start, time, gwc);
  }

  constructor(
    private readonly journal: Journal,
    readonly start: number,
    readonly time: TreeClock,
    readonly gwc: GlobalClock) {
  }

  get json(): JournalStateJson {
    return { start: this.start, time: this.time.toJson(), gwc: this.gwc.toJson() };
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
      private entries: JournalEntry[] = [];

      next(operation: MeldOperation, localTime: TreeClock) {
        const entry = JournalEntry.fromOperation(
          state.journal, operation, localTime, this.gwc);
        this.entries.push(entry);
        this.gwc = this.gwc.update(operation.time);
        this.localTime = localTime;
        return this;
      }

      /** Commits the built journal entries to the journal */
      commit: Kvps = async batch => {
        for (let entry of this.entries)
          entry.commit(batch);

        state.withTime(this.localTime, this.gwc).commit(batch);
      };
    };
  }

  latestOperations(): Observable<EncodedOperation> {
    return from(this.gwc.tids()).pipe(
      // From each op, emit a fusion of all contiguous ops up to the op
      mergeMap(tid => this.journal.operation(tid)),
      filter<JournalOperation>(op => op != null),
      mergeMap(op => op.fusedPast()));
  }
}