import { TSeq, TSeqOperation } from '../src/tseq';
import { jsonify } from './testUtil';

describe('TSeq CRDT', () => {
  describe('local mutations', () => {
    test('create with process ID and string', () => {
      const tSeq = new TSeq('p1');
      expect(tSeq.toString()).toBe('');
      const operation = tSeq.splice(0, 0, 'hello world');
      expect(tSeq.toString()).toBe('hello world');
      expect(operation).toEqual([
        { run: [[['p1', 0]], 'hello world'], tick: 1 }
      ]);
    });

    test('append content', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello');
      const operation = tSeq.splice(5, 0, ' world');
      expect(tSeq.toString()).toBe('hello world');
      expect(operation).toEqual([
        { run: [[['p1', 5]], ' world'], tick: 2 }
      ]);
    });

    test('prepend content', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, ' world');
      const operation = tSeq.splice(0, 0, 'hello');
      expect(tSeq.toString()).toBe('hello world');
      expect(operation).toEqual([
        { run: [[['p1', -5]], 'hello'], tick: 2 }
      ]);
    });

    test('inject content', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hell world');
      const operation = tSeq.splice(4, 0, 'o');
      expect(tSeq.toString()).toBe('hello world');
      expect(operation).toEqual([
        { run: [[['p1', 3], ['p1', 0]], 'o'], tick: 2 }
      ]);
    });

    test('remove content', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello world');
      const operation = tSeq.splice(0, 6);
      expect(tSeq.toString()).toBe('world');
      expect(operation).toEqual([
        { run: [[['p1', 0]], 6], tick: 1 }
      ]);
    });

    test('replace content', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello world');
      const operation = tSeq.splice(6, 5, 'bob');
      expect(tSeq.toString()).toBe('hello bob');
      expect(operation).toEqual([
        { run: [[['p1', 6]], 'bob\x00\x00'], tick: 2 }
      ]);
    });
  });

  describe('applying operations', () => {
    test('ignores replay insert', () => {
      const tSeq = new TSeq('p1');
      const operations = tSeq.splice(0, 0, 'hello world');
      expect(tSeq.apply(operations)).toBe(false);
    });

    test('ignores replay delete', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello world');
      const operations = tSeq.splice(5, 6);
      expect(tSeq.apply(operations)).toBe(false);
    });

    test('applies initial content', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      const operations = tSeq1.splice(0, 0, 'hello world');
      expect(tSeq2.apply(operations)).toBe(true);
      expect(tSeq2.toString()).toBe('hello world');
    });

    test('returned splices are sequential', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      tSeq2.apply(tSeq1.splice(0, 0, 'hell world'));
      tSeq2.apply(tSeq1.splice(4, 0, 'o'));
      tSeq2.apply(tSeq1.splice(0, 6));
      expect(tSeq1.toString()).toBe('world');
      expect(tSeq2.toString()).toBe('world');
    });

    test('ignores duplicate operation', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      const operations = tSeq1.splice(0, 0, 'hello world');
      expect(tSeq2.apply(operations)).toBe(true);
      expect(tSeq2.apply(operations)).toBe(false);
    });

    test('throws if missed message', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      tSeq1.splice(0, 0, 'hello world');
      const operations = tSeq1.splice(5, 6);
      expect(() => tSeq2.apply(operations)).toThrow(RangeError);
    });

    test('appends independent content', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      const op1 = tSeq1.splice(0, 0, 'hello');
      const op2 = tSeq2.splice(0, 0, ' world');
      tSeq2.apply(op1);
      tSeq1.apply(op2);
      expect(tSeq1.toString()).toBe('hello world');
      expect(tSeq2.toString()).toBe('hello world');
    });

    test('applies content change', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      const op1 = tSeq1.splice(0, 0, 'hello world');
      tSeq2.apply(op1);
      const op2 = tSeq2.splice(0, 5, 'hi');
      tSeq1.apply(op2);
      expect(tSeq1.toString()).toBe('hi world');
      expect(tSeq2.toString()).toBe('hi world');
    });
  });

  describe('jsonify', () => {
    test('jsonify plain string', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello world');
      expect(jsonify(tSeq.toJSON())).toEqual([
        { 'p1': 1 }, ['p1', 0, 'hello world']
      ]);
      tSeq.splice(11, 0, '!');
      const json = jsonify(tSeq.toJSON());
      expect(json).toEqual([
        { 'p1': 2 }, ['p1', 0, 'hello world!']
      ]);
      const clone = TSeq.fromJSON('p2', json);
      expect(clone.toString()).toBe('hello world!');
      expect(jsonify(clone.toJSON())).toEqual(json);
    });

    test('jsonify operation', () => {
      const ops = new TSeq('p1').splice(0, 0, 'hello world');
      expect(TSeqOperation.fromJSON(jsonify(ops[0]))).toEqual(ops[0]);
    });
  });
});