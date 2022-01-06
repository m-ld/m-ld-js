import { DatasetEngine } from '../src/engine/dataset/DatasetEngine';
import {
  hotLive, memStore, MockProcess, mockRemotes, testConfig, testExtensions
} from './testClones';
import {
  asapScheduler, BehaviorSubject, EMPTY, EmptyError, firstValueFrom, NEVER, Subject as Source,
  throwError
} from 'rxjs';
import { comesAlive } from '../src/engine/AbstractMeld';
import { count, map, observeOn, take, toArray } from 'rxjs/operators';
import { TreeClock } from '../src/engine/clocks';
import { MeldRemotes, OperationMessage, Snapshot } from '../src/engine';
import { Describe, GraphSubject, MeldConfig, MeldReadState, Read, Subject, Update } from '../src';
import { AbstractLevelDOWN } from 'abstract-leveldown';
import { jsonify } from './testUtil';
import { MeldMemDown } from '../src/memdown';
import { Write } from '../src/jrql-support';
import { Consumable } from '../src/flowable';
import { inflateFrom } from '../src/engine/util';
import { MeldError } from '../src/engine/MeldError';

describe('Dataset engine', () => {
  describe('as genesis', () => {
    async function genesis(
      remotes: MeldRemotes, config?: Partial<MeldConfig>): Promise<DatasetEngine> {
      let clone = new DatasetEngine({
        dataset: await memStore(), remotes, extensions: testExtensions(), config: testConfig(config)
      });
      await clone.initialise();
      return clone;
    }

    test('starts offline with unknown remotes', async () => {
      const clone = await genesis(mockRemotes(NEVER, [null]));
      await expect(comesAlive(clone, false)).resolves.toBe(false);
      expect(clone.status.value).toEqual({ online: false, outdated: false, silo: false, ticks: 0 });
    });

    test('comes alive if siloed', async () => {
      const clone = await genesis(mockRemotes(NEVER, [null, false]));
      await expect(comesAlive(clone)).resolves.toBe(true);
      expect(clone.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('stays live without reconnect if siloed', async () => {
      const clone = await genesis(mockRemotes(NEVER, [true, false]));
      await expect(comesAlive(clone)).resolves.toBe(true);
      expect(clone.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('non-genesis fails to initialise if siloed', async () => {
      await expect(genesis(mockRemotes(NEVER, [false],
        TreeClock.GENESIS.forked().left), { genesis: false })).rejects.toThrow();
    });
  });

  // Read and write methods on a clone require the state lock. These are
  // normally put in place by a StateEngine.
  class TestDatasetEngine extends DatasetEngine {
    read(request: Read): Consumable<GraphSubject> {
      return inflateFrom(this.lock.share('state', 'test', () => super.read(request)));
    }

    async write(request: Write): Promise<this> {
      return this.lock.exclusive('state', 'test', () => super.write(request));
    }
  }

  describe('as silo genesis', () => {
    let silo: DatasetEngine;

    beforeEach(async () => {
      silo = new TestDatasetEngine({
        dataset: await memStore(),
        remotes: mockRemotes(),
        extensions: testExtensions(),
        config: testConfig()
      });
      await silo.initialise();
    });

    test('not found is empty', async () => {
      await expect(firstValueFrom(silo.read({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe))).rejects.toBeInstanceOf(EmptyError);
    });

    test('stores a JSON-LD object', async () => {
      await expect(silo.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject)).resolves.toBe(silo);
      expect(silo.status.value.ticks).toBe(1);
    });

    test('retrieves a JSON-LD object', async () => {
      await silo.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      const fred = (await firstValueFrom(silo.read({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe))).value;
      expect(fred['@id']).toBe('http://test.m-ld.org/fred');
      expect(fred['http://test.m-ld.org/#name']).toBe('Fred');
    });

    test('has no ticks from genesis', async () => {
      expect(silo.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('has ticks after update', async () => {
      // noinspection ES6MissingAwait
      silo.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      await firstValueFrom(silo.dataUpdates);
      expect(silo.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 1 });
    });

    test('follow after initial ticks', async () => {
      const firstUpdate = firstValueFrom(silo.dataUpdates);
      // noinspection ES6MissingAwait
      silo.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      await expect(firstUpdate).resolves.toHaveProperty('@ticks', 1);
    });

    test('follow after current tick', async () => {
      await silo.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      expect(silo.status.value.ticks).toBe(1);
      const firstUpdate = firstValueFrom(silo.dataUpdates);
      await silo.write({
        '@id': 'http://test.m-ld.org/wilma',
        'http://test.m-ld.org/#name': 'Wilma'
      } as Subject);
      await expect(firstUpdate).resolves.toHaveProperty('@ticks', 2);
    });
  });

  describe('as genesis with remote clone', () => {
    let clone: DatasetEngine;
    let remote: MockProcess;
    let remoteUpdates: Source<OperationMessage> = new Source;

    beforeEach(async () => {
      const remotesLive = hotLive([false]);
      // Ensure that remote updates are async
      const remotes = mockRemotes(remoteUpdates.pipe(observeOn(asapScheduler)), remotesLive);
      clone = new TestDatasetEngine({
        dataset: await memStore(), remotes, extensions: testExtensions(), config: testConfig()
      });
      await clone.initialise();
      await comesAlive(clone); // genesis is alive
      remote = new MockProcess(await clone.newClock()); // no longer genesis
      remotes.revupFrom = async () => ({ gwc: remote.gwc, updates: EMPTY });
      remotesLive.next(true); // remotes come alive
      await clone.status.becomes({ outdated: false });
    });

    test('requires state lock for rev-up', async () => {
      await expect(clone.revupFrom(remote.time)).rejects.toBeInstanceOf(MeldError);
    });

    test('answers rev-up from the new clone', async () => {
      const revup = await clone.withLocalState(() => clone.revupFrom(remote.time));
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
      clone.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      remoteUpdates.next(remote.sentOperation(
        {}, { '@id': 'http://test.m-ld.org/wilma', 'http://test.m-ld.org/#name': 'Wilma' }));
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
      remoteUpdates.next(remote.sentOperation(
        {}, { '@id': 'http://test.m-ld.org/wilma', 'http://test.m-ld.org/#name': 'Wilma' }));
      await updated;
      const thirdTime = await clone.newClock();
      await expect(clone.withLocalState(() => clone.revupFrom(thirdTime))).resolves.toBeDefined();
    });
    // 2. a failed transaction
    test('answers rev-up from next new clone after failure', async () => {
      // Insert with union is not valid
      await clone.write(<Update>{ '@union': [] })
        .then(() => {
          throw 'Expecting error';
        }, () => {
        });
      const thirdTime = await clone.newClock();
      await expect(clone.withLocalState(() => clone.revupFrom(thirdTime))).resolves.toBeDefined();
    });
  });

  describe('as new clone', () => {
    let remotes: MeldRemotes;
    let remoteUpdates: Source<OperationMessage>;
    let snapshot: jest.Mock<Promise<Snapshot>, [MeldReadState]>;
    let collaborator: MockProcess;
    let collabPrevOp: OperationMessage;
    let remotesLive: BehaviorSubject<boolean | null>;

    beforeEach(async () => {
      const { left, right } = TreeClock.GENESIS.forked();
      collaborator = new MockProcess(right);
      collabPrevOp = collaborator.sentOperation(
        {}, { '@id': 'http://test.m-ld.org/wilma', 'http://test.m-ld.org/#name': 'Wilma' });
      remoteUpdates = new Source<OperationMessage>();
      remotesLive = hotLive([true]);
      remotes = mockRemotes(remoteUpdates, remotesLive, left);
      snapshot = jest.fn().mockReturnValueOnce(Promise.resolve<Snapshot>({
        gwc: collaborator.gwc,
        data: EMPTY, // Cheating, should really contain Wilma (see op above)
        updates: EMPTY
      }));
      remotes.snapshot = snapshot;
    });

    test('initialises from snapshot', async () => {
      const clone = new DatasetEngine({
        dataset: await memStore(), remotes, extensions: testExtensions(),
        config: testConfig({ genesis: false })
      });
      await clone.initialise();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(snapshot.mock.calls.length).toBe(1);
    });

    test('can become a silo', async () => {
      const clone = new DatasetEngine({
        dataset: await memStore(), remotes, extensions: testExtensions(),
        config: testConfig({ genesis: false })
      });
      await clone.initialise();
      remotesLive.next(false);
      await expect(clone.status.becomes({ silo: true })).resolves.toBeDefined();
    });

    test('ignores operation from before snapshot', async () => {
      const clone = new TestDatasetEngine({
        dataset: await memStore(), remotes, extensions: testExtensions(),
        config: testConfig({ genesis: false })
      });
      await clone.initialise();
      const updates = firstValueFrom(clone.dataUpdates.pipe(count()));
      remoteUpdates.next(collabPrevOp);
      // Also enqueue a no-op write, which we can wait for - relying on queue ordering
      await clone.write({ '@insert': [] } as Update);
      await clone.close(); // Will complete the updates
      await expect(updates).resolves.toBe(0);
    });
  });

  describe('as post-genesis clone', () => {
    let backend: AbstractLevelDOWN;
    let config: MeldConfig;
    let remote: MockProcess;

    beforeEach(async () => {
      backend = new MeldMemDown;
      config = testConfig();
      // Start a temporary genesis clone to initialise the store
      let clone = new DatasetEngine({
        dataset: await memStore({ backend }),
        remotes: mockRemotes(),
        extensions: testExtensions(),
        config
      });
      await clone.initialise();
      remote = new MockProcess(await clone.newClock()); // Forks the clock so no longer genesis
      await clone.close();
      // Now the ldb represents a former genesis clone
    });

    test('is outdated while revving-up', async () => {
      // Re-start on the same data, with a rev-up that never completes
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => ({ gwc: remote.gwc, updates: NEVER });
      const clone = new DatasetEngine({
        dataset: await memStore({ backend }),
        remotes,
        extensions: testExtensions(),
        config: testConfig()
      });

      // Check that we are never not outdated
      const everNotOutdated = clone.status.becomes({ outdated: false });

      await clone.initialise();

      expect(clone.status.value).toEqual({ online: true, outdated: true, silo: false, ticks: 0 });
      await expect(Promise.race([everNotOutdated, Promise.resolve()])).resolves.toBeUndefined();
    });

    test('is not outdated when revved-up', async () => {
      // Re-start on the same data, with a rev-up that completes with no updates
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => ({ gwc: remote.gwc, updates: EMPTY });
      const clone = new DatasetEngine({
        dataset: await memStore({ backend }),
        remotes,
        extensions: testExtensions(),
        config: testConfig()
      });

      // Check that we do transition through an outdated state
      const wasOutdated = clone.status.becomes({ outdated: true });

      await clone.initialise();

      await expect(wasOutdated).resolves.toMatchObject({ online: true, outdated: true });
      await expect(clone.status.becomes({ outdated: false }))
        .resolves.toEqual({ online: true, outdated: false, silo: false, ticks: 0 });
    });

    test('is not outdated if immediately siloed', async () => {
      const remotes = mockRemotes(NEVER, [null, false]);
      const clone = new DatasetEngine({
        dataset: await memStore({ backend }),
        remotes,
        extensions: testExtensions(),
        config: testConfig()
      });

      await clone.initialise();

      await expect(clone.status.becomes({ outdated: false }))
        .resolves.toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('immediately re-connects if rev-up fails', async () => {
      const remotes = mockRemotes(NEVER, [true]);
      const revupFrom = jest.fn()
        .mockReturnValueOnce(Promise.resolve({ gwc: remote.gwc, updates: throwError('boom') }))
        .mockReturnValueOnce(Promise.resolve({ gwc: remote.gwc, updates: EMPTY }));
      remotes.revupFrom = revupFrom;
      const clone = new DatasetEngine({
        dataset: await memStore({ backend }),
        remotes,
        extensions: testExtensions(),
        config: testConfig()
      });
      await clone.initialise();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(revupFrom.mock.calls.length).toBe(2);
    });

    test('maintains fifo during rev-up', async () => {
      // We need local siloed update
      let clone = new TestDatasetEngine({
        dataset: await memStore({ backend }),
        remotes: mockRemotes(),
        extensions: testExtensions(),
        config
      });
      await clone.initialise();
      await clone.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      });
      await clone.close();
      // Need a remote with rev-ups to share
      const remotes = mockRemotes(NEVER, [true]);
      const revUps = new Source<OperationMessage>();
      const revupCalled = new Promise<void>(resolve => {
        remotes.revupFrom = async () => {
          resolve();
          return { gwc: remote.gwc.update(remote.time.ticked()), updates: revUps };
        };
      });
      // The clone will initialise into a revving-up state, waiting for a revUp
      clone = new DatasetEngine({
        dataset: await memStore({ backend }),
        remotes,
        extensions: testExtensions(),
        config: testConfig()
      });
      const observedTicks = firstValueFrom(clone.operations.pipe(
        map(op => op.time.ticks), take(2), toArray()));
      await clone.initialise();
      await revupCalled;
      // Do a new update during the rev-up, this should be delayed
      await clone.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Flintstone'
      });
      // Provide a rev-up that pre-dates the local siloed update
      revUps.next(remote.sentOperation({}, {
        '@id': 'http://test.m-ld.org/wilma',
        'http://test.m-ld.org/#name': 'Wilma'
      }));
      revUps.complete();
      // Check that the updates are not out of order
      await expect(observedTicks).resolves.toEqual([1, 2]);
    });

    test('immediately re-connects after out of order operation', async () => {
      // Re-start on the same data
      const remoteUpdates = new Source<OperationMessage>();
      const remotes = mockRemotes(remoteUpdates, [true]);
      remotes.revupFrom = async () => ({ gwc: remote.gwc, updates: EMPTY });
      const clone = new DatasetEngine({
        dataset: await memStore({ backend }),
        remotes,
        extensions: testExtensions(),
        config: testConfig()
      });
      await clone.initialise();
      await clone.status.becomes({ outdated: false });

      // Push a operation claiming a missed public tick
      remote.tick();
      remoteUpdates.next(remote.sentOperation(
        {}, { '@id': 'http://test.m-ld.org/wilma', 'http://test.m-ld.org/#name': 'Wilma' }));

      await expect(clone.status.becomes({ outdated: true })).resolves.toBeDefined();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
    });

    test('ignores outdated operation', async () => {
      // Re-start on the same data
      const remoteUpdates = new Source<OperationMessage>();
      const remotes = mockRemotes(remoteUpdates, [true]);
      remotes.revupFrom = async () => ({ gwc: remote.gwc, updates: EMPTY });
      const clone = new TestDatasetEngine({
        dataset: await memStore({ backend }),
        remotes,
        extensions: testExtensions(),
        config: testConfig()
      });
      await clone.initialise();
      await clone.status.becomes({ outdated: false });

      const updates = firstValueFrom(clone.dataUpdates.pipe(toArray()));
      const op = remote.sentOperation(
        {}, { '@id': 'http://test.m-ld.org/wilma', 'http://test.m-ld.org/#name': 'Wilma' });
      // Push a operation
      remoteUpdates.next(op);
      // Push the same operation again
      remoteUpdates.next(op);
      // Also enqueue a no-op write, which we can wait for - relying on queue ordering
      await clone.write({ '@insert': [] } as Update);
      await clone.close(); // Will complete the updates
      const arrived = jsonify(await updates);
      expect(arrived.length).toBe(1);
      expect(arrived[0]).toMatchObject({ '@insert': [{ '@id': 'http://test.m-ld.org/wilma' }] });
    });
  });
});
