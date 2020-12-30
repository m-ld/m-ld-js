import { LseqDef } from '../src/engine/lseq';

describe('LSEQ', () => {
  const lseq = new LseqDef();
  // Using a radix of 16 for readability, default is 36
  lseq.radix = 16;
  // Using single-character sites x,y,z
  lseq.siteLength = 1;

  describe('Index', () => {
    test('rejects invalid index ids', () => {
      // Empty index
      expect(() => lseq.parse('x')).toThrowError();
      // Bad lengths
      expect(() => lseq.parse('axax')).toThrowError();
      expect(() => lseq.parse('axaaxa')).toThrowError();
      // Not valid for radix
      expect(() => lseq.parse('gx')).toThrowError();
      // Negative index
      expect(() => lseq.parse('ax-1x')).toThrowError();
      // Zero terminal index
      expect(() => lseq.parse('0x')).toThrowError();
      expect(() => lseq.parse('1x00x')).toThrowError();
    });

    test('constructs to have id', () => {
      expect(lseq.parse('1x').ids).toEqual([{ pos: 1, site: 'x' }]);
      expect(lseq.parse('0x01x').ids).toEqual([{ pos: 0, site: 'x' }, { pos: 1, site: 'x' }]);
      expect(lseq.parse('1x01y').ids).toEqual([{ pos: 1, site: 'x' }, { pos: 1, site: 'y' }]);
    });

    test('stringifies', () => {
      expect(lseq.parse('1x').toString()).toBe('1x');
      expect(lseq.parse('0x01x').toString()).toBe('0x01x');
      expect(lseq.parse('1x01y').toString()).toBe('1x01y');
    });

    test('generates head', () => {
      const headIndex = lseq.min.between(lseq.max, 'x');
      expect(headIndex.ids.length).toBe(1);
      expect(headIndex.ids[0].pos).toBeGreaterThan(0);
      expect(headIndex.ids[0].pos).toBeLessThan(16);
      expect(headIndex.ids[0].site).toBe('x');
    });

    test('generates next', () => {
      const headIndex = lseq.parse('1x').between(lseq.max, 'x');
      expect(headIndex.ids.length).toBe(1);
      expect(headIndex.ids[0].pos).toBeGreaterThan(1);
      expect(headIndex.ids[0].pos).toBeLessThan(16);
      expect(headIndex.ids[0].site).toBe('x');
    });

    test('generates between', () => {
      const headIndex = lseq.parse('1x').between(lseq.parse('fx'), 'x');
      expect(headIndex.ids.length).toBe(1);
      expect(headIndex.ids[0].pos).toBeGreaterThan(1);
      expect(headIndex.ids[0].pos).toBeLessThan(15);
    });

    test('overflows if high', () => {
      const headIndex = lseq.parse('fx').between(lseq.max, 'x');
      expect(headIndex.ids.length).toBe(2);
      expect(headIndex.ids[0].pos).toBe(15);
      expect(headIndex.ids[1].pos).toBeGreaterThan(0);
      expect(headIndex.ids[1].pos).toBeLessThan(256);
    });

    test('overflows if low', () => {
      const headIndex = lseq.min.between(lseq.parse('1x'), 'x');
      expect(headIndex.ids.length).toBe(2);
      expect(headIndex.ids[0].pos).toBe(0);
      expect(headIndex.ids[1].pos).toBeGreaterThan(0);
      expect(headIndex.ids[1].pos).toBeLessThan(256);
    });

    test('overflows if too close', () => {
      const headIndex = lseq.parse('1x').between(lseq.parse('2x'), 'x');
      expect(headIndex.ids.length).toBe(2);
      expect(headIndex.ids[0].pos).toBe(1);
      expect(headIndex.ids[1].pos).toBeGreaterThan(0);
      expect(headIndex.ids[1].pos).toBeLessThan(256);
    });

    test('prefers shorter between', () => {
      const headIndex = lseq.parse('1xffx001x').between(lseq.parse('2xffx'), 'x');
      // Could correctly generate 1xffx002x-1xffxfffx or 2x01x-2xfex, prefers latter
      expect(headIndex.ids.length).toBe(2);
      expect(headIndex.ids[0].pos).toBe(2);
      expect(headIndex.ids[1].pos).toBeGreaterThan(0);
      expect(headIndex.ids[1].pos).toBeLessThan(255);
    });
  });

  describe('CRDT', () => {
    // Naive example LSEQ class. Does not maintain items in order, so that
    // ordering is checked with every access.
    class Lseq<T> {
      constructor(private site: string) { }
      // Index-items
      private items: { [index: string]: T } = {};
      // Ordered index-items
      private ordered = () => Object.entries(this.items)
        .sort((e1, e2) => e1[0].localeCompare(e2[0]))
        .map(e => ({ index: e[0], item: e[1] }));
      // Ordered values
      get values() { return this.ordered().map(i => i.item); }
      // Insert into list at given numeric index >=0, <= values.length
      // Returns an operation suitable for applying to another replica
      insert(i: number, value: T): { insert: [string, T] } {
        const ordered = this.ordered();
        const lbound = ordered[i - 1]?.index, ubound = ordered[i]?.index;
        const index = (lbound ? lseq.parse(lbound) : lseq.min)
          .between(ubound ? lseq.parse(ubound) : lseq.max, this.site).toString();
        this.items[index] = value;
        return { insert: [index, value] };
      }
      // Remove at numeric index >=0, <= values.length
      // Returns an operation suitable for applying to another replica
      delete(i: number): { delete: string } {
        const index = this.ordered()[i].index;
        delete this.items[index];
        return { delete: index };
      }
      // Applies a message generated at another replica. Note that causal
      // delivery ordering is required for the CRDT.
      apply(op: ReturnType<this['insert']> | ReturnType<this['delete']>) {
        if ('insert' in op)
          this.items[op.insert[0]] = op.insert[1];
        else
          delete this.items[op.delete];
      }
    }

    let x: Lseq<string>;
    let y: Lseq<string>;

    beforeEach(() => {
      x = new Lseq<string>('x');
      y = new Lseq<string>('y');
    });

    test('insert head', () => {
      x.insert(0, 'a');
      expect(x.values).toEqual(['a']);
    });

    test('insert tail', () => {
      x.insert(0, 'a');
      x.insert(1, 'b');
      expect(x.values).toEqual(['a', 'b']);
    });

    test('insert body', () => {
      x.insert(0, 'a');
      x.insert(1, 'c');
      x.insert(1, 'b');
      expect(x.values).toEqual(['a', 'b', 'c']);
    });

    test('insert order preserved in operations', () => {
      x.insert(0, 'a');
      x.insert(1, 'b');
      x.insert(2, 'c');
      x.apply(y.insert(0, 'n'));
      x.apply(y.insert(1, 'o'));
      x.apply(y.insert(2, 'p'));
      expect(x.values.filter(v => v < 'm')).toEqual(['a', 'b', 'c']);
      expect(x.values.filter(v => v > 'm')).toEqual(['n', 'o', 'p']);
    });

    test('deletion operation after insert', () => {
      x.insert(0, 'a');
      x.insert(1, 'b');
      x.insert(2, 'c');
      x.apply(y.insert(0, 'n'));
      x.apply(y.insert(1, 'o'));
      x.apply(y.insert(2, 'p'));
      x.apply(y.delete(1));
      expect(x.values.filter(v => v < 'm')).toEqual(['a', 'b', 'c']);
      expect(x.values.filter(v => v > 'm')).toEqual(['n', 'p']);
    });
  });
});
