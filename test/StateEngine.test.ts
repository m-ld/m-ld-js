import { firstValueFrom, Subject as Source } from 'rxjs';
import { MeldUpdate } from '../src';
import { LockManager } from '../src/engine/locks';
import { CloneEngine, StateEngine } from '../src/engine/StateEngine';
import { SubjectGraph } from '../src/engine/SubjectGraph';
import { single } from 'asynciterator';
import { DataFactory as RdfDataFactory, Quad } from 'rdf-data-factory';
import { drain } from 'rx-flowable';
import { consume } from 'rx-flowable/consume';

describe('State Engine', () => {
  class MockCloneEngine implements CloneEngine {
    tick = 0;
    readonly lock = new LockManager<'state'>();
    readonly dataUpdates = new Source<MeldUpdate>();
    // noinspection JSUnusedGlobalSymbols
    match: CloneEngine['match'] = () => single(
      rdf.quad(rdf.namedNode('state'),
        rdf.namedNode('tick'),
        rdf.literal(this.tick.toString())));
    countQuads = async () => 1;
    query = () => {
      throw undefined;
    };
    read = () => consume([{ '@id': 'state', tick: this.tick }]);
    write = async () => {
      this.dataUpdates.next({
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([]),
        '@ticks': ++this.tick,
        trace: jest.fn()
      });
      return this;
    };
    ask = () => Promise.resolve(false);
  }

  let clone: MockCloneEngine;
  let states: StateEngine;
  let ops: number[]; // Used to assert concurrent operation ordering
  const pushOp = async (n: number) => ops.push(n);
  const rdf = new RdfDataFactory();

  beforeEach(() => {
    clone = new MockCloneEngine;
    states = new StateEngine(clone);
    ops = [];
  });

  test('can read initial state', done => {
    states.read(async state => {
      await expect(drain(state.read({}))).resolves.toMatchObject([{ tick: 0 }]);
      done();
    });
  });

  test('can ask initial state', done => {
    states.read(async state => {
      await expect(state.ask({})).resolves.toBe(false);
      done();
    });
  });

  test('can match initial state', done => {
    states.match().on('data', (quad: Quad) => {
      expect(quad.object.equals(rdf.literal('0'))).toBe(true);
      done();
    });
  });

  test('can follow state', done => {
    states.follow(async update => {
      expect(update).toMatchObject({ '@ticks': 1 });
      done();
    });
    states.write(state => state.write({}));
  });

  test('can read initial state and follow', done => {
    states.read(async state => {
      await expect(firstValueFrom(state.read({})))
        .resolves.toMatchObject({ value: { tick: 0 } })
        .catch(err => done(err));
    }, async update => {
      expect(update).toMatchObject({ '@ticks': 1 });
      done();
    });
    states.write(state => state.write({}));
  });

  test('can match in initial state', done => {
    states.read(async state => {
      state.match().on('data', (quad: Quad) => {
        expect(quad.object.equals(rdf.literal('0'))).toBe(true);
        done();
      });
    });
  });

  test('can match in follow', done => {
    states.follow(async (_, state) => {
      state.match().on('data', (quad: Quad) => {
        expect(quad.object.equals(rdf.literal('1'))).toBe(true);
        done();
      });
    });
    states.write(state => state.write({}));
  });

  test('can unsubscribe a follow handler', done => {
    states.follow(() => done.fail()).unsubscribe();
    states.write(state => state.write({})).then(() => done());
  });

  test('can unsubscribe a read follow handler', done => {
    states.read(() => {
    }, () => done.fail()).unsubscribe();
    states.write(state => state.write({})).then(() => done());
  });

  test('read procedures are concurrent', done => {
    states.read(async () => {
      await pushOp(1);
      await pushOp(2);
    });
    states.read(async () => {
      await pushOp(3);
      await pushOp(4);
      expect(ops).toEqual([1, 3, 2, 4]);
      done();
    });
  });

  test('write procedures are exclusive', done => {
    states.write(async () => {
      await pushOp(1);
      await pushOp(2);
    });
    states.write(async () => {
      await pushOp(3);
      await pushOp(4);
      expect(ops).toEqual([1, 2, 3, 4]);
      done();
    });
  });

  test('write procedures are exclusive with reads', done => {
    states.read(async () => {
      await pushOp(1);
      await pushOp(2);
    });
    states.read(async () => {
      await pushOp(3);
      await pushOp(4);
    });
    states.write(async () => {
      await pushOp(5);
      await pushOp(6);
      expect(ops).toEqual([1, 3, 2, 4, 5, 6]);
      done();
    });
  });

  test('handlers are concurrent', done => {
    states.follow(async () => {
      await pushOp(1);
      await pushOp(2);
    });
    states.follow(async () => {
      await pushOp(3);
      await pushOp(4);
      expect(ops).toEqual([1, 3, 2, 4]);
      done();
    });
    states.write(state => state.write({}));
  });

  test('handlers complete before a write', done => {
    states.follow(async update => {
      expect(update['@ticks']).toBe(clone.tick);
      for (let i = 0; i < 10; i++)
        await pushOp(i + update['@ticks'] * 10);
    });
    states.write(async state => {
      state = await state.write({}); // Kick handlers
      expect(clone.tick).toBe(1);
      await pushOp(100); // Concurrent with the handler
      await state.write({}); // Should await the handler
      expect(clone.tick).toBe(2);
      // Expect ten ops from the first tick, and one 100
      expect(ops.indexOf(20)).toBe(11);
      expect(ops.indexOf(100)).toBeLessThan(11);
      done();
    });
  });

  test('cannot write to de-scoped state', done => {
    states.write(async state => {
      await state.write({});
      try {
        await state.write({});
        done.fail('Expecting de-scoped state');
      } catch (err) {
        done();
      }
    });
  });

  test('cannot read from de-scoped state', done => {
    states.write(async state => {
      await state.write({});
      try {
        state.read({});
        done.fail('Expecting de-scoped state');
      } catch (err) {
        done();
      }
    });
  });
});