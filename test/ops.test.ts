import { CausalOperation } from '../src/engine/ops';
import { TreeClock } from '../src/engine/clocks';

describe('Causal Operations', () => {
  class CausalIntegerOp extends CausalOperation<number, TreeClock> {
    protected getIndex(i: number): string {
      return i.toFixed(0);
    }
  }

  function time(ticks: number) {
    return TreeClock.GENESIS.ticked(ticks);
  }

  function tid(ticks: number) {
    // not using TreeClock.hash to save test cycles
    return ticks.toFixed(0);
  }

  test('do not fuse if not sequential', () => {
    const one = new CausalIntegerOp(0, time(0), [], []);
    const two = new CausalIntegerOp(2, time(2), [], []);
    expect(one.fuse(two)).toBeNull();
  });

  test('do not fuse if reversed', () => {
    const one = new CausalIntegerOp(1, time(1), [], []);
    const two = new CausalIntegerOp(0, time(0), [], []);
    expect(one.fuse(two)).toBeNull();
  });

  test('fuses empty', () => {
    const one = new CausalIntegerOp(0, time(0), [], []);
    const two = new CausalIntegerOp(1, time(1), [], []);
    const result = one.fuse(two);
    expect(result?.from).toBe(0);
    expect(result?.time.equals(two.time)).toBe(true);
    expect(result?.deletes).toEqual([]);
    expect(result?.inserts).toEqual([]);
  });

  test('fuses disjoint inserts', () => {
    const one = new CausalIntegerOp(0, time(0), [], [[0, [tid(0)]]]);
    const two = new CausalIntegerOp(1, time(1), [], [[1, [tid(1)]]]);
    const result = one.fuse(two);
    expect(result?.from).toBe(0);
    expect(result?.time.equals(two.time)).toBe(true);
    expect(result?.deletes).toEqual([]);
    expect(result?.inserts.length).toBe(2);
    expect(result?.inserts).toEqual(expect.arrayContaining([[0, [tid(0)]], [1, [tid(1)]]]));
  });

  test('fuses same value inserts', () => {
    const one = new CausalIntegerOp(0, time(0), [], [[0, [tid(0)]]]);
    const two = new CausalIntegerOp(1, time(1), [], [[0, [tid(1)]]]);
    const result = one.fuse(two);
    expect(result?.from).toBe(0);
    expect(result?.time.equals(two.time)).toBe(true);
    expect(result?.deletes).toEqual([]);
    expect(result?.inserts.length).toBe(1);
    expect(result?.inserts).toEqual(
      expect.arrayContaining([[0, expect.arrayContaining([tid(0), tid(1)])]]));
  });

  test('removes redundant insert', () => {
    const one = new CausalIntegerOp(0, time(0), [], [[0, [tid(0)]]]);
    const two = new CausalIntegerOp(1, time(1), [[0, [tid(0)]]], []);
    const result = one.fuse(two);
    expect(result?.from).toBe(0);
    expect(result?.time.equals(two.time)).toBe(true);
    expect(result?.deletes).toEqual([]);
    expect(result?.inserts).toEqual([]);
  });

  test('removes transitively redundant insert', () => {
    const one = new CausalIntegerOp(0, time(0), [], [[0, [tid(0)]]]);
    const two = new CausalIntegerOp(1, time(1), [], [[1, [tid(1)]]]);
    const thr = new CausalIntegerOp(2, time(2), [[0, [tid(0)]]], []);
    const result = one.fuse(two).fuse(thr);
    expect(result?.from).toBe(0);
    expect(result?.time.equals(thr.time)).toBe(true);
    expect(result?.deletes).toEqual([]);
    expect(result?.inserts).toEqual([[1, [tid(1)]]]);
  });

  test('is associative', () => {
    const one = new CausalIntegerOp(0, time(0), [], [[0, [tid(0)]]]);
    const two = new CausalIntegerOp(1, time(1), [], [[1, [tid(1)]]]);
    const thr = new CausalIntegerOp(2, time(2), [[0, [tid(0)]]], []);
    // Using redundant insert to show associativity
    const result = one.fuse(two.fuse(thr));
    expect(result?.from).toBe(0);
    expect(result?.time.equals(thr.time)).toBe(true);
    expect(result?.deletes).toEqual([]);
    expect(result?.inserts).toEqual([[1, [tid(1)]]]);
  });
});