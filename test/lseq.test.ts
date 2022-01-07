import { mock } from 'jest-mock-extended';
import { LseqDef, LseqIndexNotify, LseqIndexRewriter } from '../src/engine/lseq';

describe('LSEQ', () => {
  const lseq = new LseqDef();
  // Using a radix of 16 for readability, default is 36
  lseq.radix = 16;
  // Using single-character sites x,y,z
  lseq.siteLength = 1;

  describe('LSEQ Definition', () => {
    test('rejects invalid position identifiers', () => {
      // Empty position identifier
      expect(() => lseq.parse('x')).toThrowError();
      // Bad lengths
      expect(() => lseq.parse('axax')).toThrowError();
      expect(() => lseq.parse('axaaxa')).toThrowError();
      // Not valid for radix
      expect(() => lseq.parse('gx')).toThrowError();
      // Negative position identifier
      expect(() => lseq.parse('ax-1x')).toThrowError();
      // Zero terminal position identifier
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
  });

  describe('Allocating single positions', () => {
    test('allocates head', () => {
      const head = lseq.min.between(lseq.max, 'x')[0];
      expect(head.ids.length).toBe(1);
      expect(head.ids[0].pos).toBeGreaterThan(0);
      expect(head.ids[0].pos).toBeLessThan(16);
      expect(head.ids[0].site).toBe('x');
    });

    test('allocates next', () => {
      const tail = lseq.parse('1x').between(lseq.max, 'x')[0];
      expect(tail.ids.length).toBe(1);
      expect(tail.ids[0].pos).toBeGreaterThan(1);
      expect(tail.ids[0].pos).toBeLessThan(16);
      expect(tail.ids[0].site).toBe('x');
    });

    test('allocates between', () => {
      const mid = lseq.parse('1x').between(lseq.parse('fx'), 'x')[0];
      expect(mid.ids.length).toBe(1);
      expect(mid.ids[0].pos).toBeGreaterThan(1);
      expect(mid.ids[0].pos).toBeLessThan(15);
    });

    test('overflows if high', () => {
      const posId = lseq.parse('fx').between(lseq.max, 'x')[0];
      expect(posId.ids.length).toBe(2);
      expect(posId.ids[0].pos).toBe(15);
      expect(posId.ids[1].pos).toBeGreaterThan(0);
      expect(posId.ids[1].pos).toBeLessThan(256);
    });

    test('overflows if low', () => {
      const posId = lseq.min.between(lseq.parse('1x'), 'x')[0];
      expect(posId.ids.length).toBe(2);
      expect(posId.ids[0].pos).toBe(0);
      expect(posId.ids[1].pos).toBeGreaterThan(0);
      expect(posId.ids[1].pos).toBeLessThan(256);
    });

    test('allocates between min and overflowed head', () => {
      const posId = lseq.min.between(lseq.parse('0x02x'), 'x')[0];
      expect(posId.ids.length).toBe(2);
      expect(posId.ids[0].pos).toBe(0);
      expect(posId.ids[1].pos).toBe(1);
    });

    test('allocates between min and double-overflowed head', () => {
      const posId = lseq.min.between(lseq.parse('0x00x002x'), 'x')[0];
      expect(posId.ids.length).toBe(3);
      expect(posId.ids[0].pos).toBe(0);
      expect(posId.ids[1].pos).toBe(0);
      expect(posId.ids[2].pos).toBe(1);
    });

    test('allocates between overflowed and max', () => {
      const posId = lseq.parse('fxfex').between(lseq.max, 'x')[0];
      expect(posId.ids.length).toBe(2);
      expect(posId.ids[0].pos).toBe(15);
      expect(posId.ids[1].pos).toBe(255);
    });

    test('overflows if too close', () => {
      const posId = lseq.parse('1x').between(lseq.parse('2x'), 'x')[0];
      expect(posId.ids.length).toBe(2);
      expect(posId.ids[0].pos).toBe(1);
      expect(posId.ids[1].pos).toBeGreaterThan(0);
      expect(posId.ids[1].pos).toBeLessThan(256);
    });

    test('overflows twice if no room', () => {
      const posId = lseq.parse('1x').between(lseq.parse('1x01x'), 'x')[0];
      expect(posId.ids.length).toBe(3);
      expect(posId.ids[0].pos).toBe(1);
      expect(posId.ids[1].pos).toBe(0);
      expect(posId.ids[2].pos).toBeGreaterThan(0);
      expect(posId.ids[2].pos).toBeLessThan(4096);
    });

    test('overflows if no headroom', () => {
      const posId = lseq.parse('1xffx').between(lseq.parse('2x01x'), 'x')[0];
      expect(posId.ids.length).toBe(3);
      expect(posId.ids[0].pos).toBe(1);
      expect(posId.ids[1].pos).toBe(255);
      expect(posId.ids[2].pos).toBeGreaterThan(0);
      expect(posId.ids[2].pos).toBeLessThan(4096);
    });

    test('prefers shorter between', () => {
      const posId = lseq.parse('1xffx001x').between(lseq.parse('2xffx'), 'x')[0];
      // Could correctly generate 1xffx002x-1xffxfffx, prefers or 2x01x-2xfex
      expect(posId.ids.length).toBe(2);
      expect(posId.ids[0].pos).toBe(2);
      expect(posId.ids[1].pos).toBeGreaterThan(0);
      expect(posId.ids[1].pos).toBeLessThan(256);
    });

    test('cannot position between same index if same site', () => {
      expect(() => lseq.parse('1x').between(lseq.parse('1x'), 'x')).toThrowError();
    });

    test('can position between same index if different site', () => {
      const posId = lseq.parse('1x').between(lseq.parse('1y'), 'x')[0];
      expect(posId.ids.length).toBe(2);
      expect(posId.ids[0].pos).toBe(1);
      expect(posId.ids[1].pos).toBeGreaterThan(0);
      expect(posId.ids[1].pos).toBeLessThan(256);
    });

    test('correctly orders different sites', () => {
      const posId = lseq.parse('1x01x').between(lseq.parse('1y'), 'x')[0];
      expect(posId.ids.length).toBe(2);
      expect(posId.ids[0].pos).toBe(1);
      expect(posId.ids[0].site).toBe('x');
      expect(posId.ids[1].pos).toBeGreaterThan(1);
      expect(posId.ids[1].pos).toBeLessThan(256);
    });
  });

  describe('Allocating multiple positions', () => {
    test('allocates head', () => {
      const heads = lseq.min.between(lseq.max, 'x', 3);
      expect(heads.length).toBe(3);
      let prev = 0;
      for (let head of heads) {
        expect(head.ids.length).toBe(1);
        expect(head.ids[0].pos).toBeGreaterThan(prev);
        expect(head.ids[0].pos).toBeLessThan(16);
        expect(head.ids[0].site).toBe('x');
        prev = head.ids[0].pos;
      }
    });

    test('allocates next', () => {
      const tails = lseq.parse('1x').between(lseq.max, 'x', 3);
      expect(tails.length).toBe(3);
      let prev = '1x';
      for (let tail of tails) {
        expect(tail.ids.length).toBe(1);
        expect(tail.toString() > prev).toBe(true);
        expect(tail.ids[0].pos).toBeLessThan(16);
        expect(tail.ids[0].site).toBe('x');
        prev = tail.toString();
      }
    });

    test('allocates between', () => {
      const mids = lseq.parse('1x').between(lseq.parse('fx'), 'x', 3);
      expect(mids.length).toBe(3);
      let prev = '1x';
      for (let mid of mids) {
        expect(mid.ids.length).toBe(1);
        expect(mid.toString() > prev).toBe(true);
        expect(mid.ids[0].pos).toBeLessThan(15);
        prev = mid.toString();
      }
    });

    test('overflows if not enough room', () => {
      // Using exactly double the space to ensure every position allocated at
      // level 1 has one allocation at level 2
      const mids = lseq.parse('1x').between(lseq.parse('4x'), 'x', 4);
      expect(mids.length).toBe(4);
      let prev = '1x';
      for (let mid of mids) {
        expect(mid.ids.length).toBeLessThan(3);
        expect(mid.toString() > prev).toBe(true);
        expect(mid.ids[0].pos).toBeLessThan(4);
        prev = mid.toString();
      }
    });

    test('allocates exactly count', () => {
      // Ask for enough to cause an overflow and an uneven second-level allocation
      const mids = lseq.min.between(lseq.max, 'x', 32);
      expect(mids.length).toBe(32);
    });

    test('double overflows if necessary', () => {
      // Enough to blow up the second level
      const count = 1 + 16 * 15 + 1;
      const loads = lseq.parse('1x').between(lseq.parse('3x'), 'x', count);
      expect(loads.length).toBe(count);
      let prev = '1x';
      for (let posId of loads) {
        expect(posId.ids.length).toBeLessThan(4);
        expect(posId.toString() > prev).toBe(true);
        expect(posId.ids[0].pos).toBeLessThan(3);
        prev = posId.toString();
      }
    });
  });

  describe('CRDT', () => {
    // Naive example LSEQ class. Does not maintain items in order, so that
    // ordering is checked with every access.
    // noinspection JSPotentiallyInvalidUsageOfThis
    class Lseq<T> {
      constructor(private site: string) { }
      // Position identifier-items
      private items: { [posId: string]: T } = {};
      // Ordered position identifier-items
      private ordered = () => Object.entries(this.items)
        .sort((e1, e2) => e1[0].localeCompare(e2[0]))
        .map(e => ({ posId: e[0], value: e[1] }));
      // Ordered values
      // @ts-ignore
      get values() { return this.ordered().map(i => i.value); }
      // Insert into list at given numeric index >=0, <= values.length
      // Returns an operation suitable for applying to another replica
      insert(i: number, value: T): { insert: [string, T] } {
        const ordered = this.ordered();
        const lbound = ordered[i - 1]?.posId, ubound = ordered[i]?.posId;
        const posId = (lbound ? lseq.parse(lbound) : lseq.min)
          .between(ubound ? lseq.parse(ubound) : lseq.max, this.site)[0].toString();
        this.items[posId] = value;
        return { insert: [posId, value] };
      }
      // Remove at numeric index >=0, <= values.length
      // Returns an operation suitable for applying to another replica
      delete(i: number): { delete: string } {
        const posId = this.ordered()[i].posId;
        delete this.items[posId];
        return { delete: posId };
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

  describe('Index rewriter', () => {
    test('does nothing with no requests on empty list', () => {
      const rw = new LseqIndexRewriter(lseq, 'x');
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(0);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('does nothing with no requests on a singleton list', () => {
      const rw = new LseqIndexRewriter(lseq, 'x');
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: lseq.min.between(lseq.max, 'x')[0].toString(), value: 'a' }],
        notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(0);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('inserts head by index on empty list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      rw.addInsert(['a'], 0);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('a');
      const head = lseq.parse(notify.inserted.mock.calls[0][1]);
      expect(head.ids.length).toBe(1);
      expect(head.ids[0].pos).toBeGreaterThan(0);
      expect(head.ids[0].pos).toBeLessThan(16);
      expect(head.ids[0].site).toBe('x');
      expect(notify.inserted.mock.calls[0][2]).toBe(0);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('inserts tail by index on singleton list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      rw.addInsert(['b'], 1);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('b');
      expect(notify.inserted.mock.calls[0][1] > head).toBe(true);
      expect(notify.inserted.mock.calls[0][2]).toBe(1);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('inserts beyond tail index on singleton list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      rw.addInsert(['b'], 2); // Beyond tail
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('b');
      expect(notify.inserted.mock.calls[0][1] > head).toBe(true);
      expect(notify.inserted.mock.calls[0][2]).toBe(1);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('inserts two heads on singleton list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      rw.addInsert(['a', 'b'], 0);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'c' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);

      expect(notify.inserted.mock.calls.length).toBe(2);

      expect(notify.inserted.mock.calls[0][0]).toBe('a');
      const aPos = notify.inserted.mock.calls[0][1];
      expect(aPos < head).toBe(true);
      expect(notify.inserted.mock.calls[0][2]).toBe(0);

      expect(notify.inserted.mock.calls[1][0]).toBe('b');
      const bPos = notify.inserted.mock.calls[1][1];
      expect(bPos > aPos).toBe(true);
      expect(bPos < head).toBe(true);
      expect(notify.inserted.mock.calls[1][2]).toBe(1);

      expect(notify.reindexed.mock.calls.length).toBe(1);
      expect(notify.reindexed.mock.calls[0][0]).toBe('c');
      expect(notify.reindexed.mock.calls[0][1] > bPos).toBe(true);
      expect(notify.reindexed.mock.calls[0][2]).toBe(2);
    });

    test('deletes head on a singleton list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      rw.addDelete(head);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(1);
      expect(notify.deleted.mock.calls[0][0]).toBe('a');
      expect(notify.deleted.mock.calls[0][1]).toBe(head);
      expect(notify.inserted.mock.calls.length).toBe(0);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('does not delete non-existent position', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      rw.addDelete('garbage');
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(0);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('does nothing if deleting an insert', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      const tail = lseq.parse(head).between(lseq.max, 'x')[0].toString();
      rw.addInsert('b', tail);
      rw.addDelete(tail);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(0);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('throws if inserting a delete', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      const tail = lseq.parse(head).between(lseq.max, 'x')[0].toString();
      rw.addDelete(tail);
      expect(() => rw.addInsert('b', tail)).toThrow();
    });

    test('replaces head by index on a singleton list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      rw.addDelete(head);
      rw.addInsert(['b'], 0);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(1);
      expect(notify.deleted.mock.calls[0][0]).toBe('a');
      expect(notify.deleted.mock.calls[0][1]).toBe(head);

      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('b');
      const bPos = notify.inserted.mock.calls[0][1];
      expect(bPos > lseq.min.toString()).toBe(true);
      expect(notify.inserted.mock.calls[0][2]).toBe(0);

      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('does not renumber tail after replacing head', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString(),
        tail = lseq.parse(head).between(lseq.max, 'x')[0].toString();
      rw.addDelete(head);
      rw.addInsert(['b'], 0);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }, { posId: tail, value: 'c' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(1);
      expect(notify.deleted.mock.calls[0][0]).toBe('a');
      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('b');

      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('inserts head by position ID on empty list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      rw.addInsert('a', head);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('a');
      expect(notify.inserted.mock.calls[0][1]).toBe(head);
      expect(notify.inserted.mock.calls[0][2]).toBe(0);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('inserts head by position ID on singleton list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      const newHead = lseq.min.between(lseq.parse(head), 'x')[0].toString();
      rw.addInsert('a', newHead);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'b' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);

      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('a');
      expect(notify.inserted.mock.calls[0][1]).toBe(newHead);
      expect(notify.inserted.mock.calls[0][2]).toBe(0);

      expect(notify.reindexed.mock.calls.length).toBe(1);
      expect(notify.reindexed.mock.calls[0][0]).toBe('b');
      expect(notify.reindexed.mock.calls[0][1]).toBe(head);
      expect(notify.reindexed.mock.calls[0][2]).toBe(1);
    });

    test('inserts tail by position ID on singleton list', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      const tail = lseq.parse(head).between(lseq.max, 'x')[0].toString();
      rw.addInsert('b', tail);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);
      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('b');
      expect(notify.inserted.mock.calls[0][1]).toBe(tail);
      expect(notify.inserted.mock.calls[0][2]).toBe(1);
      expect(notify.reindexed.mock.calls.length).toBe(0);
    });

    test('inserts middle by position ID', () => {
      const rw = new LseqIndexRewriter<string>(lseq, 'x');
      const head = lseq.min.between(lseq.max, 'x')[0].toString();
      const tail = lseq.parse(head).between(lseq.max, 'x')[0].toString();
      const mid = lseq.parse(head).between(lseq.parse(tail), 'x')[0].toString();
      rw.addInsert('b', mid);
      const notify = mock<LseqIndexNotify<string>>();
      rw.rewriteIndexes([{ posId: head, value: 'a' }, { posId: tail, value: 'c' }], notify);
      expect(notify.deleted.mock.calls.length).toBe(0);

      expect(notify.inserted.mock.calls.length).toBe(1);
      expect(notify.inserted.mock.calls[0][0]).toBe('b');
      expect(notify.inserted.mock.calls[0][1]).toBe(mid);
      expect(notify.inserted.mock.calls[0][2]).toBe(1);

      expect(notify.reindexed.mock.calls.length).toBe(1);
      expect(notify.reindexed.mock.calls[0][0]).toBe('c');
      expect(notify.reindexed.mock.calls[0][1]).toBe(tail);
      expect(notify.reindexed.mock.calls[0][2]).toBe(2);
    });
  });
});
