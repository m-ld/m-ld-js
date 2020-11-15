import { DatasetEngine } from '../src/engine/dataset/DatasetEngine';
import { memStore, mockRemotes, hotLive, testConfig } from './testClones';
import { NEVER, Subject as Source, asapScheduler, EMPTY, throwError, BehaviorSubject } from 'rxjs';
import { comesAlive } from '../src/engine/AbstractMeld';
import { first, take, toArray, map, observeOn, count } from 'rxjs/operators';
import { TreeClock } from '../src/engine/clocks';
import { DeltaMessage, MeldRemotes, Snapshot } from '../src/engine';
import { uuid } from 'short-uuid';
import { MeldConfig, Subject, Describe, Update } from '../src';
import MemDown from 'memdown';
import { AbstractLevelDOWN } from 'abstract-leveldown';
import { Hash } from '../src/engine/hash';

describe('Dataset engine', () => {
  describe('as genesis', () => {
    async function genesis(remotes: MeldRemotes, config?: Partial<MeldConfig>): Promise<DatasetEngine> {
      let clone = new DatasetEngine({ dataset: await memStore(), remotes, config: testConfig(config) });
      await clone.initialise();
      return clone;
    }

    test('starts offline with unknown remotes', async () => {
      const clone = await genesis(mockRemotes(NEVER, [null]));
      expect(clone.live.value).toBe(false);
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

  describe('as silo genesis', () => {
    let silo: DatasetEngine;

    beforeEach(async () => {
      silo = new DatasetEngine({ dataset: await memStore(), remotes: mockRemotes(), config: testConfig() });
      await silo.initialise();
    });

    test('not found is empty', async () => {
      await expect(silo.read({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe).toPromise()).resolves.toBeUndefined();
    });

    test('stores a JSON-LD object', async () => {
      await expect(silo.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject)).resolves.toBeUndefined();
      expect(silo.status.value.ticks).toBe(1);
    });

    test('retrieves a JSON-LD object', async () => {
      await silo.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      const result = silo.read({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe);
      const fred = await result.toPromise();
      expect(fred['@id']).toBe('http://test.m-ld.org/fred');
      expect(fred['http://test.m-ld.org/#name']).toBe('Fred');
    });

    test('has no ticks from genesis', async () => {
      expect(silo.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('has ticks after update', async () => {
      silo.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      await silo.dataUpdates.pipe(first()).toPromise();
      expect(silo.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 1 });
    });

    test('follow after initial ticks', async () => {
      const firstUpdate = silo.dataUpdates.pipe(first()).toPromise();
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
      const firstUpdate = silo.dataUpdates.pipe(first()).toPromise();
      await silo.write({
        '@id': 'http://test.m-ld.org/wilma',
        'http://test.m-ld.org/#name': 'Wilma'
      } as Subject);
      await expect(firstUpdate).resolves.toHaveProperty('@ticks', 2);
    });
  });

  describe('as genesis with remote clone', () => {
    let clone: DatasetEngine;
    let remoteTime: TreeClock;
    let remoteUpdates: Source<DeltaMessage> = new Source;

    beforeEach(async () => {
      const remotesLive = hotLive([false]);
      // Ensure that remote updates are async
      const remotes = mockRemotes(remoteUpdates.pipe(observeOn(asapScheduler)), remotesLive);
      clone = new DatasetEngine({ dataset: await memStore(), remotes, config: testConfig() });
      await clone.initialise();
      await comesAlive(clone); // genesis is alive
      remoteTime = await clone.newClock(); // no longer genesis
      remotes.revupFrom = async () => ({ lastTime: remoteTime, updates: EMPTY });
      remotesLive.next(true); // remotes come alive
      await clone.status.becomes({ outdated: false });
    });

    test('answers rev-up from the new clone', async () => {
      await expect(clone.revupFrom(remoteTime)).resolves.toBeDefined();
    });

    test('comes online as not silo', async () => {
      await expect(clone.status.becomes({ online: true, silo: false })).resolves.toBeDefined();
    });

    test('ticks increase monotonically', async () => {
      // Edge case from compliance tests: a local transaction racing a remote
      // transaction could cause a clock reversal.
      const updates = clone.dataUpdates.pipe(map(next => next['@ticks']), take(2), toArray()).toPromise();
      clone.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      remoteUpdates.next(new DeltaMessage(remoteTime.ticks, remoteTime.ticked(),
        [0, uuid(), '{}', '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}']));
      // Note extra tick for constraint application in remote update
      await expect(updates).resolves.toEqual([1, 3]);
    });

    // Edge cases from system testing: newClock exposes the current clock state
    // even if it doesn't have a journalled entry. This can also happen due to:
    // 1. a remote transaction, because of the clock space made for a constraint
    test('answers rev-up from next new clone after apply', async () => {
      const updated = clone.dataUpdates.pipe(take(1)).toPromise();
      remoteUpdates.next(new DeltaMessage(remoteTime.ticks, remoteTime.ticked(),
        [0, uuid(), '{}', '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}']));
      await updated;
      const thirdTime = await clone.newClock();
      await expect(clone.revupFrom(thirdTime)).resolves.toBeDefined();
    });
    // 2. a failed transaction
    test('answers rev-up from next new clone after failure', async () => {
      // Insert with variables is not valid
      await clone.write(<Update>{ '@insert': { '@id': '?s', '?p': '?o' } })
        .then(() => fail('Expecting error'), () => { });
      const thirdTime = await clone.newClock();
      await expect(clone.revupFrom(thirdTime)).resolves.toBeDefined();
    });
  });

  describe('as new clone', () => {
    let remotes: MeldRemotes;
    let remoteUpdates: Source<DeltaMessage>;
    let snapshot: jest.Mock<Promise<Snapshot>, void[]>;
    let collabClock: TreeClock;
    let remotesLive: BehaviorSubject<boolean | null>;

    beforeEach(async () => {
      const { left, right } = TreeClock.GENESIS.forked()
      collabClock = right;
      remoteUpdates = new Source<DeltaMessage>();
      remotesLive = hotLive([true]);
      remotes = mockRemotes(remoteUpdates, remotesLive, left);
      snapshot = jest.fn().mockReturnValueOnce(Promise.resolve<Snapshot>({
        lastHash: Hash.random(),
        lastTime: collabClock.ticked().scrubId(),
        quads: EMPTY,
        updates: EMPTY
      }));
      remotes.snapshot = snapshot;
    });

    test('initialises from snapshot', async () => {
      const clone = new DatasetEngine({ dataset: await memStore(), remotes, config: testConfig({ genesis: false }) });
      await clone.initialise();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(snapshot.mock.calls.length).toBe(1);
    });

    test('can become a silo', async () => {
      const clone = new DatasetEngine({ dataset: await memStore(), remotes, config: testConfig({ genesis: false }) });
      await clone.initialise();
      remotesLive.next(false);
      await expect(clone.status.becomes({ silo: true })).resolves.toBeDefined();
    });

    test('ignores delta from before snapshot', async () => {
      const clone = new DatasetEngine({ dataset: await memStore(), remotes, config: testConfig({ genesis: false }) });
      await clone.initialise();
      const updates = clone.dataUpdates.pipe(count()).toPromise();
      remoteUpdates.next(new DeltaMessage(collabClock.ticks, collabClock.ticked(),
        [0, uuid(), '{}', '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}']));
      // Also enqueue a no-op write, which we can wait for - relying on queue ordering
      await clone.write({ '@insert': [] } as Update);
      await clone.close(); // Will complete the updates
      await expect(updates).resolves.toBe(0);
    });
  });

  describe('as post-genesis clone', () => {
    let ldb: AbstractLevelDOWN;
    let config: MeldConfig;
    let remoteTime: TreeClock;

    beforeEach(async () => {
      ldb = new MemDown();
      config = testConfig();
      // Start a temporary genesis clone to initialise the store
      let clone = new DatasetEngine({ dataset: await memStore(ldb), remotes: mockRemotes(), config });
      await clone.initialise();
      remoteTime = await clone.newClock(); // Forks the clock so no longer genesis
      await clone.close();
      // Now the ldb represents a former genesis clone
    });

    test('is outdated while revving-up', async () => {
      // Re-start on the same data, with a rev-up that never completes
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => ({ lastTime: remoteTime, updates: NEVER });
      const clone = new DatasetEngine({ dataset: await memStore(ldb), remotes, config: testConfig() });

      // Check that we are never not outdated
      const everNotOutdated = clone.status.becomes({ outdated: false });

      await clone.initialise();

      expect(clone.status.value).toEqual({ online: true, outdated: true, silo: false, ticks: 0 });
      await expect(Promise.race([everNotOutdated, Promise.resolve()])).resolves.toBeUndefined();
    });

    test('is not outdated when revved-up', async () => {
      // Re-start on the same data, with a rev-up that completes with no updates
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => ({ lastTime: remoteTime, updates: EMPTY });
      const clone = new DatasetEngine({ dataset: await memStore(ldb), remotes, config: testConfig() });

      // Check that we do transition through an outdated state
      const wasOutdated = clone.status.becomes({ outdated: true });

      await clone.initialise();

      await expect(wasOutdated).resolves.toMatchObject({ online: true, outdated: true });
      await expect(clone.status.becomes({ outdated: false }))
        .resolves.toEqual({ online: true, outdated: false, silo: false, ticks: 0 });
    });

    test('is not outdated if immediately siloed', async () => {
      const remotes = mockRemotes(NEVER, [null, false]);
      const clone = new DatasetEngine({ dataset: await memStore(ldb), remotes, config: testConfig() });

      await clone.initialise();

      await expect(clone.status.becomes({ outdated: false }))
        .resolves.toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('immediately re-connects if rev-up fails', async () => {
      const remotes = mockRemotes(NEVER, [true]);
      const revupFrom = jest.fn()
        .mockReturnValueOnce(Promise.resolve({ lastTime: remoteTime, updates: throwError('boom') }))
        .mockReturnValueOnce(Promise.resolve({ lastTime: remoteTime, updates: EMPTY }));
      remotes.revupFrom = revupFrom;
      const clone = new DatasetEngine({ dataset: await memStore(ldb), remotes, config: testConfig() });
      await clone.initialise();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(revupFrom.mock.calls.length).toBe(2);
    });

    test('maintains fifo during rev-up', async () => {
      // We need local siloed update
      let clone = new DatasetEngine({ dataset: await memStore(ldb), remotes: mockRemotes(), config });
      await clone.initialise();
      await clone.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      });
      await clone.close();
      // Need a remote with rev-ups to share
      const remotes = mockRemotes(NEVER, [true]);
      const revUps = new Source<DeltaMessage>();
      remotes.revupFrom = async () => ({ lastTime: remoteTime.ticked(), updates: revUps });
      // The clone will initialise into a revving-up state, waiting for a revUp
      clone = new DatasetEngine({ dataset: await memStore(ldb), remotes, config: testConfig() });
      const observedTicks = clone.updates.pipe(map(next => next.time.ticks), take(2), toArray()).toPromise();
      await clone.initialise();
      // Do a new update during rev-up, this will immediately produce an update
      await clone.write({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Flintstone'
      });
      // Provide a rev-up that pre-dates the local siloed update
      revUps.next(new DeltaMessage(remoteTime.ticks, remoteTime.ticked(),
        [0, uuid(), '{}', '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}']));
      revUps.complete();
      // Check that the updates are not out of order
      await expect(observedTicks).resolves.toEqual([1, 2]);
    });

    test('immediately re-connects after out of order delta', async () => {
      // Re-start on the same data
      const remoteUpdates = new Source<DeltaMessage>();
      const remotes = mockRemotes(remoteUpdates, [true]);
      remotes.revupFrom = async () => ({ lastTime: remoteTime, updates: EMPTY });
      const clone = new DatasetEngine({ dataset: await memStore(ldb), remotes, config: testConfig() });
      await clone.initialise();
      await clone.status.becomes({ outdated: false });

      // Push a delta claiming a missed tick
      remoteUpdates.next(new DeltaMessage(remoteTime.ticks + 1, remoteTime.ticked().ticked(),
        [0, uuid(), '{}', '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}']));

      await expect(clone.status.becomes({ outdated: true })).resolves.toBeDefined();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
    });

    test('ignores outdated delta', async () => {
      // Re-start on the same data
      const remoteUpdates = new Source<DeltaMessage>();
      const remotes = mockRemotes(remoteUpdates, [true]);
      remotes.revupFrom = async () => ({ lastTime: remoteTime, updates: EMPTY });
      const clone = new DatasetEngine({ dataset: await memStore(ldb), remotes, config: testConfig() });
      await clone.initialise();
      await clone.status.becomes({ outdated: false });

      const updates = clone.dataUpdates.pipe(toArray()).toPromise();
      const delta = new DeltaMessage(remoteTime.ticks, remoteTime.ticked(),
        [0, uuid(), '{}', '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}']);
      // Push a delta
      remoteUpdates.next(delta);
      // Push the same delta again
      remoteUpdates.next(delta);
      // Also enqueue a no-op write, which we can wait for - relying on queue ordering
      await clone.write({ '@insert': [] } as Update);
      await clone.close(); // Will complete the updates
      const arrived = await updates;
      expect(arrived.length).toBe(1);
      expect(arrived[0]).toMatchObject({ '@insert': [{ "@id": "http://test.m-ld.org/wilma" }]})
    });
  });
});
