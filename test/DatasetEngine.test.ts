import { DatasetEngine } from '../src/engine/dataset/DatasetEngine';
import {
  decodeOpUpdate, hotLive, memStore, MockProcess, mockRemotes, testConfig
} from './testClones';
import {
  asapScheduler, BehaviorSubject, EMPTY, EmptyError, firstValueFrom, NEVER, of, Subject as Source,
  throwError
} from 'rxjs';
import { comesAlive } from '../src/engine/AbstractMeld';
import { count, map, observeOn, take, toArray } from 'rxjs/operators';
import { TreeClock } from '../src/engine/clocks';
import { Meld, MeldRemotes, Snapshot } from '../src/engine';
import {
  combinePlugins, Describe, GraphSubject, MeldError, MeldReadState, Read, Select, Update
} from '../src';
import { AbstractLevel } from 'abstract-level';
import { jsonify } from './testUtil';
import { MemoryLevel } from 'memory-level';
import { Consumable, drain, each } from 'rx-flowable';
import { inflateFrom } from '../src/engine/util';
import { MeldOperationMessage } from '../src/engine/MeldOperationMessage';
import { mockFn } from 'jest-mock-extended';
import { SuSetDataset } from '../src/engine/dataset/SuSetDataset';
import { JsonldContext } from '../src/engine/jsonld';
import { EngineWrite } from '../src/engine/StateEngine';

const fred = {
  '@id': 'http://test.m-ld.org/fred',
  'http://test.m-ld.org/#name': 'Fred'
}, wilma = {
  '@id': 'http://test.m-ld.org/wilma',
  'http://test.m-ld.org/#name': 'Wilma'
}, barney = {
  '@id': 'http://test.m-ld.org/barney',
  'http://test.m-ld.org/#name': 'Barney'
};
const doNotInit = 'doNotInit';

class TestDatasetEngine extends DatasetEngine {
  static async instance(params?: Partial<{
    backend: AbstractLevel<any>,
    remotes: MeldRemotes,
    genesis: boolean,
    doNotInit?: typeof doNotInit
  }>): Promise<TestDatasetEngine> {
    const fullConfig = testConfig({ genesis: params?.genesis ?? true });
    const suset = new SuSetDataset(
      await memStore({ backend: params?.backend }),
      new JsonldContext(), combinePlugins([]), {}, fullConfig
    );
    const clone = new TestDatasetEngine(
      suset, params?.remotes ?? mockRemotes(), fullConfig
    );
    if (!params?.doNotInit)
      await clone.initialise();
    return clone;
  }
  // Utility that snapshots the clone only to fork its clock
  async newClock() {
    return this.lock.share('state', 'test', async () =>
      (await this.snapshot(true)).clock!);
  }
  // These methods on a clone require the state lock.
  // These are normally put in place by a StateEngine.
  read(request: Read): Consumable<GraphSubject> {
    return inflateFrom(this.lock.share('state', 'test', () => super.read(request)));
  }
  async write(request: EngineWrite): Promise<this> {
    return this.lock.exclusive('state', 'test', () => super.write(request));
  }
}

describe('Dataset engine', () => {
  describe('as genesis', () => {
    test('starts offline with unknown remotes', async () => {
      const clone = await TestDatasetEngine.instance({
        remotes: mockRemotes(NEVER, [null])
      });
      await expect(comesAlive(clone, false)).resolves.toBe(false);
      expect(clone.status.value).toEqual({ online: false, outdated: false, silo: false, ticks: 0 });
    });

    test('comes alive if siloed', async () => {
      const clone = await TestDatasetEngine.instance({
        remotes: mockRemotes(NEVER, [null, false])
      });
      await expect(comesAlive(clone)).resolves.toBe(true);
      expect(clone.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('stays live without reconnect if siloed', async () => {
      const clone = await TestDatasetEngine.instance({
        remotes: mockRemotes(NEVER, [true, false])
      });
      await expect(comesAlive(clone)).resolves.toBe(true);
      expect(clone.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('non-genesis fails to initialise if siloed', async () => {
      await expect(TestDatasetEngine.instance({
        remotes: mockRemotes(NEVER, [false]),
        genesis: false
      })).rejects.toThrow();
    });
  });

  describe('as silo genesis', () => {
    let silo: DatasetEngine;

    beforeEach(async () => {
      silo = await TestDatasetEngine.instance();
    });

    test('not found is empty', async () => {
      await expect(firstValueFrom(silo.read({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe))).rejects.toBeInstanceOf(EmptyError);
    });

    test('stores a JSON-LD object', async () => {
      await expect(silo.write({ jrql: fred })).resolves.toBe(silo);
      expect(silo.status.value.ticks).toBe(1);
    });

    test('retrieves a JSON-LD object', async () => {
      await silo.write({ jrql: fred });
      const subject = (await firstValueFrom(silo.read({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe))).value;
      expect(subject['@id']).toBe('http://test.m-ld.org/fred');
      expect(subject['http://test.m-ld.org/#name']).toBe('Fred');
    });

    test('has no ticks from genesis', async () => {
      expect(silo.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('has ticks after update', async () => {
      // noinspection ES6MissingAwait
      silo.write({ jrql: fred });
      await firstValueFrom(silo.dataUpdates);
      expect(silo.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 1 });
    });

    test('follow after initial ticks', async () => {
      const firstUpdate = firstValueFrom(silo.dataUpdates);
      // noinspection ES6MissingAwait
      silo.write({ jrql: fred });
      await expect(firstUpdate).resolves.toHaveProperty('@ticks', 1);
    });

    test('follow after current tick', async () => {
      await silo.write({ jrql: fred });
      expect(silo.status.value.ticks).toBe(1);
      const firstUpdate = firstValueFrom(silo.dataUpdates);
      await silo.write({ jrql: wilma });
      await expect(firstUpdate).resolves.toHaveProperty('@ticks', 2);
    });
  });

  describe('as genesis with remote clone', () => {
    let clone: TestDatasetEngine;
    let remote: MockProcess;
    let remoteUpdates: Source<MeldOperationMessage>;

    beforeEach(async () => {
      remoteUpdates = new Source;
      const remotesLive = hotLive([false]);
      // Ensure that remote updates are async
      const remotes = mockRemotes(remoteUpdates.pipe(observeOn(asapScheduler)), remotesLive);
      clone = await TestDatasetEngine.instance({ remotes });
      await comesAlive(clone); // genesis is alive
      remote = new MockProcess(await clone.newClock()); // no longer genesis
      remotes.revupFrom = mockFn().mockImplementation(async () => remote.revup());
      remotesLive.next(true); // remotes come alive
      await clone.status.becomes({ outdated: false });
    });

    test('requires state lock for rev-up', async () => {
      await expect(clone.revupFrom(remote.time)).rejects.toBeInstanceOf(MeldError);
    });

    test('answers rev-up from the new clone', async () => {
      const revup = await clone.latch(() => clone.revupFrom(remote.time));
      expect(revup).toBeDefined();
      await expect(firstValueFrom(revup!.updates)).rejects.toBeInstanceOf(EmptyError);
    });

    test('comes online as not silo', async () => {
      await expect(clone.status.becomes({ online: true, silo: false })).resolves.toBeDefined();
    });

    test('ticks increase monotonically', async () => {
      // Edge case from compliance tests: a local transaction racing a remote
      // transaction could cause a clock reversal.
      const updates = firstValueFrom(clone.dataUpdates.pipe(map(next => next['@ticks']),
        take(2), toArray()));
      // noinspection ES6MissingAwait
      clone.write({ jrql: fred });
      remoteUpdates.next(remote.sentOperation({}, wilma));
      // Note extra tick for constraint application in remote update
      const received = await updates;
      expect(received.length).toBe(2);
      expect(received[0] < received[1]).toBe(true);
    });

    // Edge cases from system testing: newClock exposes the current clock state
    // even if it doesn't have a journaled entry. This can also happen due to:
    // 1. a remote transaction, because of the clock space made for a constraint
    test('answers rev-up from next new clone after apply', async () => {
      const updated = firstValueFrom(clone.dataUpdates);
      remoteUpdates.next(remote.sentOperation({}, wilma));
      await updated;
      const thirdTime = await clone.newClock();
      await expect(clone.latch(() => clone.revupFrom(thirdTime))).resolves.toBeDefined();
    });
    // 2. a failed transaction
    test('answers rev-up from next new clone after failure', async () => {
      // Insert with union is not valid
      await clone.write({ jrql: <Update>{ '@union': [] } })
        .then(() => { throw 'Expecting error'; }, () => {});
      const thirdTime = await clone.newClock();
      await expect(clone.latch(() => clone.revupFrom(thirdTime))).resolves.toBeDefined();
    });

    // Edge case from system testing: when answering a rev-up, received
    // operations may not be forwarded because they are only added to the
    // journal when they have been processed locally, by which time the rev-up
    // may have completed. This is a problem if we ourselves are also revving-up
    // – the operation may not be broadcast, but only sent to us directly.
    test('forwards incoming ops answering rev-up', async () => {
      // Create a third-party process which will provide an update
      const third = new MockProcess(await clone.newClock());
      // Create a local operation the third party has seen but the remote has not
      const operated = firstValueFrom(clone.operations);
      await clone.write({ jrql: fred });
      third.join((await operated).time);
      // Inject an operation from the third party
      remoteUpdates.next(third.sentOperation({}, wilma));
      // Immediately start answering a rev-up for the remote (above is processing)
      const { updates } = (await clone.latch(() => clone.revupFrom(remote.time)))!;
      const updatesArrayPromise = firstValueFrom(updates.pipe(toArray()));
      // Consume the rev-up & check the third-party message is included
      const arrived = await updatesArrayPromise;
      expect(arrived.length).toBe(2);
      expect(decodeOpUpdate(arrived[0])).toMatchObject([{}, { '@id': 'fred' }]);
      expect(decodeOpUpdate(arrived[1])).toMatchObject([{}, wilma]);
    });
  });

  describe('as new clone', () => {
    let remotes: MeldRemotes;
    let remoteUpdates: Source<MeldOperationMessage>;
    let snapshot: jest.Mock<Promise<Snapshot>, [boolean, MeldReadState]>;
    let collaborator: MockProcess;
    let collabPrevOp: MeldOperationMessage;
    let remotesLive: BehaviorSubject<boolean | null>;

    beforeEach(async () => {
      const { left, right } = TreeClock.GENESIS.forked();
      collaborator = new MockProcess(right);
      collabPrevOp = collaborator.sentOperation({}, wilma);
      remoteUpdates = new Source<MeldOperationMessage>();
      remotesLive = hotLive([true]);
      remotes = mockRemotes(remoteUpdates, remotesLive);
      snapshot = remotes.snapshot = mockFn<Meld['snapshot']>()
        // Cheating, snapshot should really contain Wilma (see op above)
        .mockImplementation(async () => collaborator.snapshot([], EMPTY, left));
    });

    test('initialises from snapshot', async () => {
      const clone = await TestDatasetEngine.instance({ remotes, genesis: false });
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(snapshot.mock.calls.length).toBe(1);
    });

    test('does not come live if snapshot fails', async () => {
      snapshot.mockImplementation(async () => {
        const snapshot = collaborator.snapshot(throwError(() =>
          new MeldError('Clone has closed')));
        // When the collaborator has closed, there are no more clones
        setImmediate(() => remotesLive.next(false));
        return snapshot;
      });
      await expect(TestDatasetEngine.instance({ remotes, genesis: false })).rejects.toThrow();
    });

    test('does not come live if remotes close', async () => {
      snapshot.mockImplementation(() => new Promise((_resolve, reject) => {
        // The remotes object is killed by e.g. an authentication failure
        // This causes undecided liveness and completion (see AbstractMeld)
        setImmediate(() => {
          remotesLive.next(null);
          remotesLive.complete();
          reject(new MeldError('Clone has closed'));
        });
      }));
      await expect(TestDatasetEngine.instance({ remotes, genesis: false })).rejects.toThrow();
    });

    test('does not come live if remotes never open', async () => {
      remotesLive = hotLive([null]);
      remotes = mockRemotes(remoteUpdates, remotesLive);
      setImmediate(() => remotesLive.complete());
      await expect(TestDatasetEngine.instance({ remotes, genesis: false })).rejects.toThrow();
    });

    test('retries if snapshot fails', async () => {
      snapshot.mockImplementationOnce(async () => collaborator.snapshot(
        throwError(() => new MeldError('Unknown error'))));
      const clone = await TestDatasetEngine.instance({ remotes, genesis: false });
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(snapshot.mock.calls.length).toBe(2);
    });

    test('can become a silo', async () => {
      const clone = await TestDatasetEngine.instance({ remotes, genesis: false });
      remotesLive.next(false);
      await expect(clone.status.becomes({ silo: true })).resolves.toBeDefined();
    });

    test('ignores operation from before snapshot', async () => {
      const clone = await TestDatasetEngine.instance({ remotes, genesis: false });
      const updates = firstValueFrom(clone.dataUpdates.pipe(count()));
      remoteUpdates.next(collabPrevOp);
      // Also enqueue a no-op write, which we can wait for - relying on queue ordering
      await clone.write({ jrql: { '@insert': [] } as Update });
      await clone.close(); // Will complete the updates
      await expect(updates).resolves.toBe(0);
    });

    test('recovers to snapshot with unbased operation', async () => {
      // We need an operation from the collaborator that is concurrent with the
      // agreement, and so is voidable
      const agreeing = collaborator.fork();
      collaborator.sentOperation({}, barney);
      const clone = await TestDatasetEngine.instance({ remotes, genesis: false });
      // Poke in an agreement concurrent with the snapshot (voids barney)
      remoteUpdates.next(agreeing.sentOperation({}, fred, { agree: true }));
      // Clone should ask for another snapshot
      await expect(clone.status.becomes({ outdated: true })).resolves.toBeDefined();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(snapshot.mock.calls.length).toBe(2);
    });
  });

  describe('as post-genesis clone', () => {
    let backend: AbstractLevel<any>;
    let remote: MockProcess;

    beforeEach(async () => {
      backend = new MemoryLevel();
      // Start a temporary genesis clone to initialise the store
      const clone = await TestDatasetEngine.instance({ backend });
      remote = new MockProcess(await clone.newClock()); // Forks the clock so no longer genesis
      await clone.close();
      // Now the ldb represents a former genesis clone
    });

    test('is outdated while revving-up', async () => {
      // Re-start on the same data, with a rev-up that never completes
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => remote.revup(NEVER);
      const clone = await TestDatasetEngine.instance({ backend, remotes, doNotInit });

      // Check that we are never not outdated
      const everNotOutdated = clone.status.becomes({ outdated: false });

      await clone.initialise();

      expect(clone.status.value).toEqual({ online: true, outdated: true, silo: false, ticks: 0 });
      await expect(Promise.race([everNotOutdated, Promise.resolve()])).resolves.toBeUndefined();
    });

    test('is not outdated when revved-up', async () => {
      // Re-start on the same data, with a rev-up that completes with no updates
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => remote.revup();
      const clone = await TestDatasetEngine.instance({ backend, remotes, doNotInit });

      // Check that we do transition through an outdated state
      const wasOutdated = clone.status.becomes({ outdated: true });

      await clone.initialise();

      await expect(wasOutdated).resolves.toMatchObject({ online: true, outdated: true });
      await expect(clone.status.becomes({ outdated: false }))
        .resolves.toEqual({ online: true, outdated: false, silo: false, ticks: 0 });
    });

    test('is not outdated if immediately siloed', async () => {
      const remotes = mockRemotes(NEVER, [null, false]);
      const clone = await TestDatasetEngine.instance({ backend, remotes });
      await expect(clone.status.becomes({ outdated: false }))
        .resolves.toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('immediately re-connects if rev-up fails', async () => {
      const remotes = mockRemotes(NEVER, [true]);
      const revupFrom = jest.fn()
        .mockReturnValueOnce(Promise.resolve({
          gwc: remote.gwc, updates: throwError(() => 'boom')
        }))
        .mockReturnValueOnce(Promise.resolve({
          gwc: remote.gwc, updates: EMPTY
        }));
      remotes.revupFrom = revupFrom;
      const clone = await TestDatasetEngine.instance({ backend, remotes });
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(revupFrom.mock.calls.length).toBe(2);
    });

    test('re-connects with disordered revup', async () => {
      const remotes = mockRemotes(NEVER, [true]);
      remote.tick(); // An operation we will never see
      const disorderedOp = remote.sentOperation({}, {});
      const revupFrom = jest.fn()
        .mockReturnValueOnce(Promise.resolve({
          gwc: remote.gwc, updates: of(disorderedOp)
        }))
        .mockReturnValueOnce(Promise.resolve({
          gwc: remote.gwc, updates: EMPTY
        }));
      remotes.revupFrom = revupFrom;
      const clone = await TestDatasetEngine.instance({ backend, remotes });
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(revupFrom.mock.calls.length).toBe(2);
    });

    test('maintains fifo during rev-up', async () => {
      // We need local siloed update
      let clone = await TestDatasetEngine.instance({ backend });
      await clone.write({ jrql: fred });
      await clone.close();
      // Need a remote with rev-ups to share
      const remotes = mockRemotes(NEVER, [true]);
      const revUps = new Source<MeldOperationMessage>();
      const revupCalled = new Promise<void>(resolve => {
        remotes.revupFrom = async () => {
          resolve();
          return {
            ...remote.revup(),
            gwc: remote.gwc.set(remote.time.ticked()),
            updates: revUps
          };
        };
      });
      // The clone will initialise into a revving-up state, waiting for a revUp
      clone = await TestDatasetEngine.instance({
        backend, remotes, doNotInit
      });
      const observedTicks = firstValueFrom(clone.operations.pipe(
        map(op => op.time.ticks), take(2), toArray()));
      await clone.initialise();
      await revupCalled;
      // Do a new update during the rev-up, this should be delayed
      await clone.write({
        jrql: {
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#name': 'Flintstone'
        }
      });
      // Provide a rev-up that pre-dates the local siloed update
      revUps.next(remote.sentOperation({}, wilma));
      revUps.complete();
      // Check that the updates are not out of order
      await expect(observedTicks).resolves.toEqual([1, 2]);
    });

    test('immediately re-connects after out of order operation', async () => {
      // Re-start on the same data
      const remoteUpdates = new Source<MeldOperationMessage>();
      const remotes = mockRemotes(remoteUpdates, [true]);
      remotes.revupFrom = async () => remote.revup();
      const clone = await TestDatasetEngine.instance({ backend, remotes });
      await clone.status.becomes({ outdated: false });

      // Push a operation claiming a missed public tick
      remote.tick();
      const outOfOrder = remote.sentOperation({}, wilma);
      remoteUpdates.next(outOfOrder);

      await expect(clone.status.becomes({ outdated: true })).resolves.toBeDefined();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      await expect(outOfOrder.delivered).rejects.toBeInstanceOf(MeldError);
    });

    test('ignores outdated operation', async () => {
      // Re-start on the same data
      const remoteUpdates = new Source<MeldOperationMessage>();
      const remotes = mockRemotes(remoteUpdates, [true]);
      remotes.revupFrom = async () => remote.revup();
      const clone = await TestDatasetEngine.instance({ backend, remotes });
      await clone.status.becomes({ outdated: false });

      const updates = firstValueFrom(clone.dataUpdates.pipe(toArray()));
      // Push a operation
      let op = remote.sentOperation({}, wilma);
      remoteUpdates.next(op);
      // Push the same operation again
      op = MeldOperationMessage.fromOperation(op.prev, op.data, null, op.time);
      remoteUpdates.next(op);
      // Delivered OK, but need additional check below to see ignored
      await expect(op.delivered).resolves.toBeUndefined();
      // Also enqueue a no-op write, which we can wait for - relying on queue ordering
      await clone.write({ jrql: { '@insert': [] } as Update });
      await clone.close(); // Will complete the updates
      const arrived = jsonify(await updates);
      expect(arrived.length).toBe(1);
      expect(arrived[0]).toMatchObject({ '@insert': [{ '@id': 'http://test.m-ld.org/wilma' }] });
    });

    test('recovers with snapshot if no rev-up available', async () => {
      // Re-start on the same data, with a rev-up that never completes
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => undefined;
      remotes.snapshot = mockFn().mockImplementation(
        async () => ({ ...remote.snapshot(), updates: EMPTY }));
      const clone = await TestDatasetEngine.instance({ backend, remotes });
      await expect(clone.status.becomes({ outdated: false })).resolves.not.toThrow();
      expect(remotes.snapshot).toHaveBeenCalled();
    });

    test('refuses snapshot if pre-agreement', async () => {
      // Start with offline remotes
      const remotesLive = hotLive([null]);
      const remotes = mockRemotes(NEVER, remotesLive);
      const clone = await TestDatasetEngine.instance({ backend, remotes });
      // Create a local agreement
      const willAgree = firstValueFrom(clone.operations);
      await clone.write({ jrql: { '@insert': fred, '@agree': true } });
      const agreeOp = await willAgree;
      const tooOldSnapshot = remote.snapshot();
      remotes.snapshot = mockFn()
        .mockImplementationOnce(async () => ({ ...tooOldSnapshot, updates: EMPTY }))
        .mockImplementationOnce(async () => {
          remote.join(agreeOp.time);
          return ({ ...remote.snapshot(), updates: EMPTY });
        });
      // Go live with forced snapshot
      remotes.revupFrom = async () => undefined;
      remotesLive.next(true);
      // Expect the missing agreement to be emitted again
      expect((await firstValueFrom(clone.operations)).time.equals(agreeOp.time)).toBe(true);
      // Expect the clone to eventually resolve to the newer snapshot
      await expect(clone.status.becomes({ outdated: false })).resolves.not.toThrow();
      expect(tooOldSnapshot.cancel).toHaveBeenCalled();
      expect(remotes.snapshot).toHaveBeenCalledTimes(2);
    });

    test('nested read is not deadlocked by connect', async () => {
      // Start with offline remotes
      const remotesLive = hotLive([null]);
      const remotes = mockRemotes(NEVER, remotesLive);
      const clone = await TestDatasetEngine.instance({ backend, remotes });
      await clone.write({ jrql: { '@graph': [fred, wilma, barney] } });
      let count = 0;
      await each(clone.read({ // Shares 'live' lock until all results consumed
        '@select': '?id', '@where': { '@id': '?id' }
      } as Select), async () => {
        // Connect wants exclusive 'live'
        if (count++ === 0) remotesLive.next(true);
        // Nested read wants shared 'live'
        await drain(clone.read({ '@describe': fred['@id'] } as Describe));
      });
      expect(count).toBe(3);
      await clone.close(); // Otherwise it continues trying to connect
    });
  });
});
