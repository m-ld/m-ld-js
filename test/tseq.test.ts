import { TSeq } from '../src/tseq';
import { jsonify } from './testUtil';
import fc from 'fast-check';
import {
  arbitraryProcessIds, SpliceStringModel, TSeqProcessGroupCheckConvergedCommand,
  TSeqProcessGroupCommand, TSeqProcessGroupDeliverCommand, TSeqProcessGroupForceSyncCommand,
  TSeqProcessGroupSpliceCommand, TSeqSpliceCommand
} from './TSeqPropertyFixtures';

describe('TSeq CRDT', () => {
  describe('local mutations', () => {
    let tSeq: TSeq;

    beforeEach(() => {
      tSeq = new TSeq('p1');
    });

    afterEach(() => {
      expect(() => tSeq.checkInvariants()).not.toThrow();
    });

    test('create with process ID and string', () => {
      expect(tSeq.toString()).toBe('');
      expect(tSeq.charLength).toBe(0);
      expect(() => tSeq.checkInvariants()).not.toThrow();
      const operation = tSeq.splice(0, 0, 'hello world');
      expect(tSeq.toString()).toBe('hello world');
      expect(operation).toEqual([[
        [['p1', 0]],
        [['h', 1], ['e', 1], ['l', 1], ['l', 1], ['o', 1],
          [' ', 1], ['w', 1], ['o', 1], ['r', 1], ['l', 1], ['d', 1]]
      ]]);
    });

    test('append content', () => {
      tSeq.splice(0, 0, 'hello');
      const operation = tSeq.splice(5, 0, ' world');
      expect(tSeq.toString()).toBe('hello world');
      expect(operation).toEqual([[
        [['p1', 5]], [[' ', 2], ['w', 2], ['o', 2], ['r', 2], ['l', 2], ['d', 2]]
      ]]);
    });

    test('prepend content', () => {
      tSeq.splice(0, 0, ' world');
      const operation = tSeq.splice(0, 0, 'hello');
      expect(tSeq.toString()).toBe('hello world');
      expect(operation).toEqual([[
        [['p1', -5]], [['h', 2], ['e', 2], ['l', 2], ['l', 2], ['o', 2]]
      ]]);
    });

    test('inject content', () => {
      tSeq.splice(0, 0, 'hell world');
      const operation = tSeq.splice(4, 0, 'o');
      expect(tSeq.toString()).toBe('hello world');
      expect(operation).toEqual([[
        [['p1', 3], ['p1', 0]], [['o', 2]]
      ]]);
    });

    test('remove content', () => {
      tSeq.splice(0, 0, 'hello world');
      const operation = tSeq.splice(0, 6);
      expect(tSeq.toString()).toBe('world');
      expect(operation).toEqual([[
        [['p1', 0]], [['', 1], ['', 1], ['', 1], ['', 1], ['', 1], ['', 1]]
      ]]);
    });

    test('remove all content', () => {
      tSeq.splice(0, 0, 'hello world');
      tSeq.splice(0, 11);
      expect(tSeq.toString()).toBe('');
    });

    test('replace content', () => {
      tSeq.splice(0, 0, 'hello world');
      const operation = tSeq.splice(6, 5, 'bob');
      expect(tSeq.toString()).toBe('hello bob');
      expect(operation).toEqual([[
        [['p1', 6]], [['b', 2], ['o', 2], ['b', 2], ['', 1], ['', 1]]
      ]]);
    });
  });

  describe('applying operations', () => {
    test('ignores replay insert', () => {
      const tSeq = new TSeq('p1');
      const operations = tSeq.splice(0, 0, 'hello world');
      expect(tSeq.apply(operations)).toEqual([]);
    });

    test('ignores replay delete', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello world');
      const operations = tSeq.splice(5, 6);
      expect(tSeq.apply(operations)).toEqual([]);
    });

    test('applies initial content', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      const operations = tSeq1.splice(0, 0, 'hello world');
      expect(tSeq2.apply(operations)).toEqual([[0, 0, 'hello world']]);
      expect(tSeq2.toString()).toBe('hello world');
    });

    test('returned splices are sequential', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      tSeq2.apply(tSeq1.splice(0, 0, 'hell world'));
      tSeq2.apply(tSeq1.splice(4, 0, 'o'));
      expect(tSeq2.apply(tSeq1.splice(0, 6))).toEqual([[0, 6, '']]);
      expect(tSeq1.toString()).toBe('world');
      expect(tSeq2.toString()).toBe('world');
    });

    test('ignores duplicate insert', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      const operations = tSeq1.splice(0, 0, 'hello world');
      expect(tSeq2.apply(operations)).toEqual([[0, 0, 'hello world']]);
      expect(tSeq2.apply(operations)).toEqual([]);
    });

    test('appends independent content', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      const op1 = tSeq1.splice(0, 0, 'hello');
      const op2 = tSeq2.splice(0, 0, ' world');
      expect(tSeq2.apply(op1)).toEqual([[0, 0, 'hello']]);
      expect(tSeq1.apply(op2)).toEqual([[5, 0, ' world']]);
      expect(tSeq1.toString()).toBe('hello world');
      expect(tSeq2.toString()).toBe('hello world');
    });

    test('applies content change', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      const op1 = tSeq1.splice(0, 0, 'hello world');
      tSeq2.apply(op1);
      const op2 = tSeq2.splice(0, 5, 'hi');
      expect(tSeq1.apply(op2)).toEqual([[0, 5, 'hi']]);
      expect(tSeq1.toString()).toBe('hi world');
      expect(tSeq2.toString()).toBe('hi world');
    });

    test('applies my own content change', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      tSeq2.apply(tSeq1.splice(0, 0, 'hello world'));
      tSeq2.apply(tSeq1.splice(11, 0, '!'));
      const op = tSeq1.splice(11, 1, '?');
      expect(tSeq2.apply(op)).toEqual([[11, 1, '?']]);
    });

    test('applies multi-splice operation', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      tSeq2.apply(tSeq1.splice(0, 0, 'hello world'));
      tSeq2.splice(5, 0, ' my'); // 'hello my world'
      const two = tSeq2.apply(tSeq1.splice(2, 7));
      expect(tSeq1.toString()).toBe('held');
      expect(tSeq2.toString()).toBe('he myld');
      expect(two).toEqual([[2, 3, ''], [8, 4, '']]);
    });

    test('applies historical delete', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      tSeq2.apply(tSeq1.splice(0, 0, 'abc'));
      const op1 = tSeq1.splice(3, 0, 'd');
      const op2 = tSeq2.splice(1, 1); // 'b' has tick 1
      tSeq2.apply(op1);
      tSeq1.apply(op2);
      expect(tSeq1.toString()).toBe('acd');
      expect(tSeq2.toString()).toBe('acd');
    });

    test('leaves local nodes', () => {
      const tSeq1 = new TSeq('p1'), tSeq2 = new TSeq('p2');
      tSeq2.apply(tSeq1.splice(0, 0, 'ab'));
      tSeq2.splice(2, 0, '1');
      tSeq2.apply(tSeq1.splice(0, 2));
      expect(tSeq2.toString()).toBe('1');
    });
  });

  describe('jsonify', () => {
    test('clone with JSON', () => {
      const tSeq1 = new TSeq('p1');
      tSeq1.splice(0, 0, 'abc');
      const tSeq2 = TSeq.fromJSON('p2', tSeq1.toJSON());
      expect(tSeq2.toString()).toBe('abc');
      tSeq2.splice(1, 1);
      expect(tSeq2.toString()).toBe('ac');
      expect(() => tSeq2.checkInvariants()).not.toThrow();
    });

    test('jsonify plain string', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello world');
      expect(jsonify(tSeq.toJSON())).toEqual({
        tick: 1,
        rest: [['p1', 0, ['h', 1], ['e', 1], ['l', 1], ['l', 1], ['o', 1],
          [' ', 1], ['w', 1], ['o', 1], ['r', 1], ['l', 1], ['d', 1]]]
      });
      tSeq.splice(11, 0, '!');
      const json = jsonify(tSeq.toJSON());
      expect(json).toEqual({
        tick: 2,
        rest: [['p1', 0, ['h', 1], ['e', 1], ['l', 1], ['l', 1], ['o', 1],
          [' ', 1], ['w', 1], ['o', 1], ['r', 1], ['l', 1], ['d', 1], ['!', 2]]]
      });
      const clone = TSeq.fromJSON('p2', json);
      expect(clone.toString()).toBe('hello world!');
      expect(jsonify(clone.toJSON())).toEqual(json);
      expect(() => clone.checkInvariants()).not.toThrow();
    });

    test('jsonify with garbage collected nodes', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello world');
      tSeq.splice(0, 6);
      tSeq.splice(3, 2);
      expect(jsonify(tSeq.toJSON())).toEqual({
        tick: 1,
        rest: [['p1', 6, ['w', 1], ['o', 1], ['r', 1]]]
      });
    });

    test('jsonify with mid-array empty nodes', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hello world');
      tSeq.splice(4, 1);
      expect(tSeq.toString()).toBe('hell world');
      const json = tSeq.toJSON();
      const tSeq2 = TSeq.fromJSON('p2', json);
      expect(tSeq2.toString()).toBe('hell world');
      expect(() => tSeq2.checkInvariants()).not.toThrow();
    });

    test('jsonify mixed string', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hell world');
      tSeq.splice(4, 0, 'o');
      expect(jsonify(tSeq.toJSON())).toEqual({
        'tick': 2,
        'rest': [['p1', 0, ['h', 1], ['e', 1], ['l', 1],
          ['l', 1, ['p1', 0, ['o', 2]]], [' ', 1],
          ['w', 1], ['o', 1], ['r', 1], ['l', 1], ['d', 1]]]
      });
    });

    test('jsonify with garbage collected array', () => {
      const tSeq = new TSeq('p1');
      tSeq.splice(0, 0, 'hell world');
      tSeq.splice(4, 0, 'o');
      tSeq.splice(4, 1);
      expect(jsonify(tSeq.toJSON())).toEqual({
        'tick': 2,
        'rest': [['p1', 0, ['h', 1], ['e', 1], ['l', 1], ['l', 1],
          [' ', 1], ['w', 1], ['o', 1], ['r', 1], ['l', 1], ['d', 1]]]
      });
    });
  });

  describe('property tests', () => {
    test('presents a starting state', () => {
      fc.assert(
        fc.property(fc.string(), value => {
          const tseq = new TSeq('p1');
          tseq.splice(0, 0, value);
          expect(tseq.toString()).toBe(value);
        })
      );
    });

    test('can splice', () => {
      fc.assert(
        fc.property(
          fc.commands([
            TSeqSpliceCommand.arbitrary()
          ]), cmds => {
            fc.modelRun(() => ({
              model: new SpliceStringModel(),
              real: new TSeq('p1')
            }), cmds);
          }
        )
      );
    });

    test('process group converges with forced sync', () => {
      const numProcs = 3;
      fc.assert(
        fc.property(
          fc.commands([
            TSeqProcessGroupSpliceCommand.arbitrary(numProcs),
            TSeqProcessGroupForceSyncCommand.arbitrary()
          ], {
            size: '+1' // good coverage, <1sec runtime
          }),
          arbitraryProcessIds(numProcs),
          TSeqProcessGroupCommand.runModel
        )
      );
    });

    test.each([
      [false, false],
      [true, false],
      [false, true],
      [true, true]
    ])('process group converges with partial deliveries', (fuse, redeliver) => {
      const numProcs = 3;
      fc.assert(
        fc.property(
          fc.commands([
            TSeqProcessGroupSpliceCommand.arbitrary(numProcs),
            TSeqProcessGroupDeliverCommand.arbitrary(numProcs, fuse, redeliver),
            TSeqProcessGroupCheckConvergedCommand.arbitrary()
          ], {
            size: '+1' // good coverage, <1sec runtime
          }),
          arbitraryProcessIds(numProcs),
          TSeqProcessGroupCommand.runModel
        )
      );
    });
  });
});