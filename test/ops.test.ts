import { CausalTimeRange, FusableCausalOperation } from '../src/engine/ops';
import { TreeClock } from '../src/engine/clocks';

// MutableOperations are tested (using quads) in dataset.test.ts
class CausalIntegerOp extends FusableCausalOperation<number, TreeClock> {
  protected sizeof(item: number): number {
    return 8;
  }
}

function tuple<Args extends any[]>(...args: Args): Args { return args; }

// Utilities for readability
const time = (ticks: number) => TreeClock.GENESIS.ticked(ticks);
const tid = (ticks: number) => time(ticks).hash;

describe('Fusable Causal Operations', () => {
  describe('preconditions', () => {
    test('not contiguous', () => {
      const one = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 2, time: time(2), deletes: [], inserts: [] });
      expect(CausalTimeRange.contiguous(one, two)).toBe(false);
    });

    test('reversed not contiguous', () => {
      const one = new CausalIntegerOp({ from: 1, time: time(1), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      expect(CausalTimeRange.contiguous(one, two)).toBe(false);
    });

    test('different IDs not contiguous', () => {
      const { left, right } = time(0).forked();
      const one = new CausalIntegerOp({
        from: 1, time: left.ticked(1), deletes: [], inserts: []
      });
      const two = new CausalIntegerOp({
        from: 2, time: right.ticked(2), deletes: [], inserts: []
      });
      expect(CausalTimeRange.contiguous(one, two)).toBe(false);
    });

    test('forked ID is not contiguous', () => {
      const one = new CausalIntegerOp({
        from: 1, time: time(0).ticked(1), deletes: [], inserts: []
      });
      const right = one.time.forked().right.ticked();
      const two = new CausalIntegerOp({
        from: right.ticks, time: right, deletes: [], inserts: []
      });
      expect(CausalTimeRange.contiguous(one, two)).toBe(false);
    });

    test('disjoint do not overlap', () => {
      const one = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 2, time: time(2), deletes: [], inserts: [] });
      expect(CausalTimeRange.overlaps(one, two)).toBe(false);
    });

    test('sequential do not overlap', () => {
      const one = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 1, time: time(1), deletes: [], inserts: [] });
      expect(CausalTimeRange.overlaps(one, two)).toBe(false);
    });
  });

  describe('fusing', () => {
    test('fuses empty', () => {
      const one = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 1, time: time(1), deletes: [], inserts: [] });
      expect(CausalTimeRange.contiguous(one, two)).toBe(true);
      const result = one.fusion().next(two).commit();
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([]);
    });

    test('fuses after fork', () => {
      const { left } = time(0).forked();
      const one = new CausalIntegerOp({ from: 0, time: left, deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 1, time: left.ticked(), deletes: [], inserts: [] });
      expect(CausalTimeRange.contiguous(one, two)).toBe(true);
      const result = one.fusion().next(two).commit();
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([]);
    });

    test('fuses disjoint inserts', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [tuple(0, [tid(0)])]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [], inserts: [tuple(1, [tid(1)])]
      });
      expect(CausalTimeRange.contiguous(one, two)).toBe(true);
      const result = one.fusion().next(two).commit();
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([tuple(0, [tid(0)]), tuple(1, [tid(1)])]);
    });

    test('fuses same value inserts', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [tuple(0, [tid(0)])]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [], inserts: [tuple(0, [tid(1)])]
      });
      expect(CausalTimeRange.contiguous(one, two)).toBe(true);
      const result = one.fusion().next(two).commit();
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toHaveLength(1);
      expect(result?.inserts).toEqual([[0, [tid(0), tid(1)]]]);
    });

    test('removes redundant insert', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [tuple(0, [tid(0)])]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [tuple(0, [tid(0)])], inserts: []
      });
      expect(CausalTimeRange.contiguous(one, two)).toBe(true);
      const result = one.fusion().next(two).commit();
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([]);
    });

    test('removes transitively redundant insert', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [tuple(0, [tid(0)])]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [], inserts: [tuple(1, [tid(1)])]
      });
      const thr = new CausalIntegerOp({
        from: 2, time: time(2), deletes: [tuple(0, [tid(0)])], inserts: []
      });
      expect(CausalTimeRange.contiguous(one, two)).toBe(true);
      const head = one.fusion().next(two).commit();
      expect(CausalTimeRange.contiguous(head, thr)).toBe(true);
      const result = new CausalIntegerOp(head).fusion().next(thr).commit();
      expect(result?.from).toBe(0);
      expect(result?.time.equals(thr.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([tuple(1, [tid(1)])]);
    });

    test('is associative', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [tuple(0, [tid(0)])]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [], inserts: [tuple(1, [tid(1)])]
      });
      const thr = new CausalIntegerOp({
        from: 2, time: time(2), deletes: [tuple(0, [tid(0)])], inserts: []
      });
      // Using redundant insert to show associativity
      expect(CausalTimeRange.contiguous(two, thr)).toBe(true);
      const tail = two.fusion().next(thr).commit();
      expect(CausalTimeRange.contiguous(one, tail)).toBe(true);
      const result = tail ? one.fusion().next(tail).commit() : undefined;
      expect(result?.from).toBe(0);
      expect(result?.time.equals(thr.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([tuple(1, [tid(1)])]);
    });
  });

  describe('cutting', () => {
    test('cuts empty', () => {
      const one = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 0, time: time(1), deletes: [], inserts: [] });
      expect(CausalTimeRange.overlaps(one, two)).toBe(true);
      const cut = two.cutting().next(one).commit();
      expect(cut.from).toBe(1);
      expect(cut.time.equals(two.time)).toBe(true);
      expect(cut.deletes).toEqual([]);
      expect(cut.inserts).toEqual([]);
    });

    test('cuts empty from non-empty', () => {
      const one = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({
        from: 0, time: time(1), deletes: [], inserts: [tuple(1, [tid(1)])]
      });
      expect(CausalTimeRange.overlaps(one, two)).toBe(true);
      const cut = two.cutting().next(one).commit();
      expect(cut.from).toBe(1);
      expect(cut.time.equals(two.time)).toBe(true);
      expect(cut.deletes).toEqual([]);
      expect(cut.inserts).toEqual([tuple(1, [tid(1)])]);
    });

    test('cut deletes unaccounted insert', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [tuple(0, [tid(0)])]
      });
      const two = new CausalIntegerOp({
        from: 0, time: time(1), deletes: [], inserts: []
      });
      expect(CausalTimeRange.overlaps(one, two)).toBe(true);
      const cut = two.cutting().next(one).commit();
      expect(cut.from).toBe(1);
      expect(cut.time.equals(two.time)).toBe(true);
      expect(cut.deletes).toEqual([tuple(0, [tid(0)])]);
      expect(cut.inserts).toEqual([]);
    });

    test('cut removes intersecting insert', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [tuple(0, [tid(0)])]
      });
      const two = new CausalIntegerOp({
        from: 0, time: time(1), deletes: [], inserts: [tuple(0, [tid(0)]), tuple(1, [tid(1)])]
      });
      expect(CausalTimeRange.overlaps(one, two)).toBe(true);
      const cut = two.cutting().next(one).commit();
      expect(cut.from).toBe(1);
      expect(cut.time.equals(two.time)).toBe(true);
      expect(cut.deletes).toEqual([]);
      expect(cut.inserts).toEqual([tuple(1, [tid(1)])]);
    });

    test('cut removes redundant delete', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(1), deletes: [], inserts: []
      });
      // Overlapping tick 1
      const two = new CausalIntegerOp({
        from: 1, time: time(2), deletes: [tuple(0, [tid(0)])], inserts: []
      });
      expect(CausalTimeRange.overlaps(one, two)).toBe(true);
      const cut = two.cutting().next(one).commit();
      expect(cut.from).toBe(2);
      expect(cut.time.equals(two.time)).toBe(true);
      expect(cut.deletes).toEqual([]);
      expect(cut.inserts).toEqual([]);
    });

    test('cut does not remove significant delete', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(1), deletes: [], inserts: [tuple(0, [tid(0)])]
      });
      // Overlapping tick 1
      const two = new CausalIntegerOp({
        from: 1, time: time(2), deletes: [tuple(0, [tid(0)])], inserts: []
      });
      expect(CausalTimeRange.overlaps(one, two)).toBe(true);
      const cut = two.cutting().next(one).commit();
      expect(cut.from).toBe(2);
      expect(cut.time.equals(two.time)).toBe(true);
      expect(cut.deletes).toEqual([tuple(0, [tid(0)])]);
      expect(cut.inserts).toEqual([]);
    });

    test('cut does everything', () => {
      const one = new CausalIntegerOp({
        from: 1,
        time: time(2),
        deletes: [
          tuple(0.0, [tid(0)]), // Deleted at 1
          tuple(0.1, [tid(0)])  // Deleted at 2
        ],
        inserts: [
          tuple(1.0, [tid(1)]),
          tuple(1.1, [tid(1)]),
          tuple(2.0, [tid(2)]),
          tuple(2.1, [tid(2)])
        ]
      });
      // Overlapping tick 2
      const two = new CausalIntegerOp({
        from: 2,
        time: time(3),
        deletes: [
          tuple(0.1, [tid(0)]), // Deleted at 2
          tuple(0.2, [tid(0)]), // Deleted at 3
          tuple(1.1, [tid(1)])  // Deleted at 3
          //tuple(2.1, [tid(2)])   Deleted at 3
        ],
        inserts: [
          tuple(2.0, [tid(2)]),
          tuple(3.0, [tid(3)])
        ]
      });
      expect(CausalTimeRange.overlaps(one, two)).toBe(true);
      const cut = two.cutting().next(one).commit();
      expect(cut.from).toBe(3);
      expect(cut.time.equals(two.time)).toBe(true);
      expect(cut.deletes).toEqual([
        tuple(0.2, [tid(0)]),
        tuple(1.1, [tid(1)]),
        tuple(2.1, [tid(2)])
      ]);
      expect(cut.inserts).toEqual([
        tuple(3, [tid(3)])
      ]);
    });
  });
});
