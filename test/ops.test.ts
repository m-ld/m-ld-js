import { FusableCausalOperation } from '../src/engine/ops';
import { TreeClock } from '../src/engine/clocks';

// MutableOperations are tested (using quads) in dataset.test.ts
class CausalIntegerOp extends FusableCausalOperation<number, TreeClock> { }
// Utilities for readability
const time = (ticks: number) => TreeClock.GENESIS.ticked(ticks)
// Not using TreeClock.hash to save test cycles
const tid = (ticks: number) => ticks.toFixed(0)

describe('Fusable Causal Operations', () => {
  describe('fusing', () => {
    test('do not fuse if not sequential', () => {
      const one = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 2, time: time(2), deletes: [], inserts: [] });
      expect(one.fuse(two)).toBeUndefined();
    });

    test('do not fuse if reversed', () => {
      const one = new CausalIntegerOp({ from: 1, time: time(1), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      expect(one.fuse(two)).toBeUndefined();
    });

    test('fuses empty', () => {
      const one = new CausalIntegerOp({ from: 0, time: time(0), deletes: [], inserts: [] });
      const two = new CausalIntegerOp({ from: 1, time: time(1), deletes: [], inserts: [] });
      const result = one.fuse(two);
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([]);
    });

    test('fuses disjoint inserts', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [[0, [tid(0)]]]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [], inserts: [[1, [tid(1)]]]
      });
      const result = one.fuse(two);
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts.length).toBe(2);
      expect(result?.inserts).toEqual(expect.arrayContaining([[0, [tid(0)]], [1, [tid(1)]]]));
    });

    test('fuses same value inserts', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [[0, [tid(0)]]]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [], inserts: [[0, [tid(1)]]]
      });
      const result = one.fuse(two);
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts.length).toBe(1);
      expect(result?.inserts).toEqual(
        expect.arrayContaining([[0, expect.arrayContaining([tid(0), tid(1)])]]));
    });

    test('removes redundant insert', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [[0, [tid(0)]]]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [[0, [tid(0)]]], inserts: []
      });
      const result = one.fuse(two);
      expect(result?.from).toBe(0);
      expect(result?.time.equals(two.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([]);
    });

    test('removes transitively redundant insert', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [[0, [tid(0)]]]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [], inserts: [[1, [tid(1)]]]
      });
      const thr = new CausalIntegerOp({
        from: 2, time: time(2), deletes: [[0, [tid(0)]]], inserts: []
      });
      const head = new CausalIntegerOp(one.fuse(two));
      const result = head.fuse(thr);
      expect(result?.from).toBe(0);
      expect(result?.time.equals(thr.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([[1, [tid(1)]]]);
    });

    test('is associative', () => {
      const one = new CausalIntegerOp({
        from: 0, time: time(0), deletes: [], inserts: [[0, [tid(0)]]]
      });
      const two = new CausalIntegerOp({
        from: 1, time: time(1), deletes: [], inserts: [[1, [tid(1)]]]
      });
      const thr = new CausalIntegerOp({
        from: 2, time: time(2), deletes: [[0, [tid(0)]]], inserts: []
      });
      // Using redundant insert to show associativity
      const tail = two.fuse(thr);
      const result = tail ? one.fuse(tail) : undefined;
      expect(result?.from).toBe(0);
      expect(result?.time.equals(thr.time)).toBe(true);
      expect(result?.deletes).toEqual([]);
      expect(result?.inserts).toEqual([[1, [tid(1)]]]);
    });
  });
});
