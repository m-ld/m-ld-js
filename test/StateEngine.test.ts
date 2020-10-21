import { of, Subject as Source } from 'rxjs';
import { MeldUpdate } from '../src/api';
import { SharableLock } from '../src/engine/locks';
import { CloneEngine, StateEngine } from '../src/engine/StateEngine';

describe('State Engine', () => {
  class MockCloneEngine implements CloneEngine {
    tick = 0;
    readonly lock = new SharableLock<'state'>();
    readonly dataUpdates = new Source<MeldUpdate>();
    read = () => of({ tick: this.tick });
    write = async () => {
      this.dataUpdates.next({ '@delete': [], '@insert': [], '@ticks': ++this.tick });
    }
  }
  let clone: MockCloneEngine;
  let states: StateEngine;
  let ops: number[]; // Used to assert concurrent operation ordering
  const pushOp = async (n: number) => ops.push(n);

  beforeEach(() => {
    clone = new MockCloneEngine;
    states = new StateEngine(clone);
    ops = [];
  });

  test('can read initial state', done => {
    states.read(async state => {
      await expect(state.read({}).toPromise()).resolves.toMatchObject({ tick: 0 });
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
      await expect(state.read({}).toPromise()).resolves.toMatchObject({ tick: 0 }).catch(fail);
    }, async update => {
      expect(update).toMatchObject({ '@ticks': 1 });
      done();
    });
    states.write(state => state.write({}));
  });

  test('can unsubscribe a follow handler', done => {
    states.follow(fail).unsubscribe();
    states.write(state => state.write({})).then(() => done());
  });

  test('can unsubscribe a read follow handler', done => {
    states.read(() => { }, fail).unsubscribe();
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
        fail('Expecting de-scoped state');
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
        fail('Expecting de-scoped state');
      } catch (err) {
        done();
      }
    });
  });
});