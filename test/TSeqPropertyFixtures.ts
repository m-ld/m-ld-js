////////////////////////////////////////////////////////////////////////////
// https://fast-check.dev/docs/advanced/model-based-testing/

import fc from 'fast-check';
import { TSeq, TSeqOperation, TSeqSplice } from '../src/tseq';
import { TSeqOperable } from '../src/tseq/TSeqOperable';

function arbitraryPercent() {
  return fc.integer({ min: 0, max: 99 });
}

/**
 * Fast-check has to generate its scenarios up-front so it can effectively
 * shrink them; this means it can't know the runtime length of anything. So
 * we use fractional splices and convert them to absolute splices in the
 * commands.
 */
export class PercentSplice {
  static arbitrary() {
    return fc.tuple(
      arbitraryPercent(), // Percent index
      arbitraryPercent(), // Percent deleteCount
      fc.option(fc.string(), { nil: undefined })
    ).map(([...splice]) => new PercentSplice(...splice));
  }

  constructor(
    readonly indexPercent: number,
    readonly deletePercent: number,
    readonly content: string | undefined
  ) {}

  toAbsoluteSplice(value: string): TSeqSplice {
    const index = toAbsolute(value.length, this.indexPercent);
    const deleteCount = toAbsolute(value.length - index, this.deletePercent);
    return [index, deleteCount, this.content];
  }
}

export function toAbsolute(length: number, percent: number) {
  return Math.floor(length * percent / 100);
}

export class SpliceStringModel {
  value = '';
  splice(index: number, deleteCount: number, content = '') {
    this.value =
      this.value.slice(0, index) + content
      + this.value.slice(index + deleteCount);
  }
  toString() { return this.value; }
}

export class TSeqSpliceCommand implements fc.Command<SpliceStringModel, TSeq> {
  constructor(readonly percentSplice: PercentSplice) {}
  check() { return true; }
  run(model: SpliceStringModel, tseq: TSeq) {
    const splice = this.percentSplice.toAbsoluteSplice(model.value);
    model.splice(...splice);
    tseq.splice(...splice);
    expect(tseq.toString()).toBe(model.toString());
  }
  static arbitrary() {
    return PercentSplice.arbitrary().map(splice => new TSeqSpliceCommand(splice));
  }
}

export type TSeqOutbox = { ops: TSeqOperation[], delivered: TSeqOperation[] };

export abstract class TSeqProcessGroupCommand implements fc.Command<TSeqOutbox[], TSeq[]> {
  abstract check(m: Readonly<TSeqOutbox[]>): boolean;
  run(model: TSeqOutbox[], real: TSeq[]) {
    // Uncomment to log the run
    // console.log(this.toString());
  }
  toString() {
    return this.constructor.name;
  }
  static runModel(
    cmds: Iterable<TSeqProcessGroupCommand>,
    processIds: string[]
  ) {
    fc.modelRun(() => ({
      model: processIds.map(() => ({ ops: [], delivered: [] })),
      real: processIds.map(pid => new TSeq(pid))
    }), cmds);
  }
}

export class TSeqProcessGroupSpliceCommand extends TSeqProcessGroupCommand {
  splice: TSeqSplice; // Stored for stringify
  constructor(
    readonly processIndex: number,
    readonly percentSplice: PercentSplice
  ) {
    super();
  }
  check() { return true; }
  run(outboxes: TSeqOutbox[], seqs: TSeq[]) {
    const outbox = outboxes[this.processIndex];
    const tseq = seqs[this.processIndex];
    this.splice = this.percentSplice.toAbsoluteSplice(tseq.toString());
    outbox.ops.push(tseq.splice(...this.splice));
    super.run(outboxes, seqs);
  }
  toString() {
    return super.toString() +
      ` processIndex ${this.processIndex},` +
      ` splice ${this.splice}`;
  }
  static arbitrary(numProcs: number) {
    return fc.tuple(
      fc.integer({ min: 0, max: numProcs - 1 }),
      PercentSplice.arbitrary()
    ).map(([pid, splice]) => new TSeqProcessGroupSpliceCommand(pid, splice));
  }
}

export class TSeqProcessGroupDeliverCommand extends TSeqProcessGroupCommand {
  last: { deliverCount: number, redeliverCount: number };
  constructor(
    readonly processIndex: number,
    readonly deliverPercent: number,
    readonly fuse: boolean,
    readonly redeliverPercent: number
  ) {
    super();
  }
  check(outboxes: TSeqOutbox[]) {
    return outboxes[this.processIndex].ops.length > 0;
  }
  run(outboxes: TSeqOutbox[], seqs: TSeq[]) {
    const outbox = outboxes[this.processIndex];
    // Note 0 <= deliverPercent <= 99; we always deliver at least one
    const deliverCount = toAbsolute(outbox.ops.length, this.deliverPercent) + 1;
    const redeliverCount = toAbsolute(outbox.delivered.length, this.redeliverPercent);
    const opsToDeliver = outbox.ops.splice(0, deliverCount);
    const opsToRedeliver = redeliverCount ? outbox.delivered.slice(-redeliverCount) : [];
    const prefix = redeliverCount ? opsToRedeliver.length == 1 ?
      opsToRedeliver[0] : TSeqOperable.concat(...opsToRedeliver) : undefined;
    // Deliver to all targets. This ensures causal delivery.
    for (let i = 0; i < seqs.length; i++) {
      if (this.processIndex !== i) {
        const tSeq = seqs[i];
        if (this.fuse) {
          const fusion = TSeqOperable.concat(...opsToRedeliver, ...opsToDeliver);
          this.deliver(tSeq, fusion, prefix);
        } else {
          opsToDeliver.forEach((op, i) =>
            i == 0 ? this.deliver(tSeq, op, prefix) : tSeq.apply(op))
        }
      }
    }
    outbox.delivered.push(...opsToDeliver);
    this.last = { deliverCount, redeliverCount };
    super.run(outboxes, seqs);
  }
  private deliver(tSeq: TSeq, op: TSeqOperation, prefix?: TSeqOperation) {
    tSeq.apply(prefix ? TSeqOperable.rmPrefix(prefix, op) : op);
  }
  toString() {
    return super.toString() +
      ` processIndex ${this.processIndex},` +
      ` delivered ${this.last.deliverCount},` +
      ` redelivered ${this.last.redeliverCount},`;
  }
  static arbitrary(numProcs: number, fuse: boolean, redeliver: boolean) {
    return fc.tuple(
      fc.integer({ min: 0, max: numProcs - 1 }),
      arbitraryPercent(),
      redeliver ? arbitraryPercent() : fc.constant(0)
    ).map(([pid, deliver, redeliverPercent]) =>
      new TSeqProcessGroupDeliverCommand(pid, deliver, fuse, redeliverPercent));
  }
}

export class TSeqProcessGroupCheckConvergedCommand extends TSeqProcessGroupCommand {
  check(outboxes: TSeqOutbox[]) {
    return outboxes.every(outbox => outbox.ops.length === 0);
  }
  run(outboxes: TSeqOutbox[], seqs: TSeq[]) {
    const values = seqs.map(tseq => tseq.toString());
    for (let value of values.slice(1))
      expect(value).toBe(values[0]);
    super.run(outboxes, seqs);
  }
  static arbitrary() { return fc.constant(new TSeqProcessGroupCheckConvergedCommand()); }
}

export class TSeqProcessGroupForceSyncCommand extends TSeqProcessGroupCheckConvergedCommand {
  check() { return true; }
  run(outboxes: TSeqOutbox[], seqs: TSeq[]) {
    for (let i = 0; i < seqs.length; i++) {
      const deliver =
        new TSeqProcessGroupDeliverCommand(i, 99, false, 0);
      if (deliver.check(outboxes))
        deliver.run(outboxes, seqs);
    }
    super.run(outboxes, seqs);
  }
  static arbitrary() { return fc.constant(new TSeqProcessGroupForceSyncCommand()); }
}

export function arbitraryProcessIds(numProcs: number) {
  return fc.uniqueArray(fc.uuid(), { maxLength: numProcs, minLength: numProcs });
}
