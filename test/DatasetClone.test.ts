import { DatasetClone } from '../src/engine/dataset/DatasetClone';
import { memStore, mockRemotes, hotLive, testConfig } from './testClones';
import { NEVER, Subject as Source, asapScheduler, EMPTY, throwError } from 'rxjs';
import { comesAlive } from '../src/engine/AbstractMeld';
import { first, take, toArray, map, observeOn } from 'rxjs/operators';
import { TreeClock } from '../src/engine/clocks';
import { DeltaMessage, MeldRemotes, Snapshot } from '../src/engine';
import { uuid } from 'short-uuid';
import { MeldConfig, Subject, Describe, Group, Update } from '../src';
import MemDown from 'memdown';
import { AbstractLevelDOWN } from 'abstract-leveldown';
import { Hash } from '../src/engine/hash';

describe('Dataset clone', () => {
  describe('as genesis', () => {
    async function genesis(remotes: MeldRemotes, config?: Partial<MeldConfig>): Promise<DatasetClone> {
      let clone = new DatasetClone({ dataset: await memStore(), remotes, config: testConfig(config) });
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
    let silo: DatasetClone;

    beforeEach(async () => {
      silo = new DatasetClone({ dataset: await memStore(), remotes: mockRemotes(), config: testConfig() });
      await silo.initialise();
    });

    test('not found is empty', async () => {
      await expect(silo.transact({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe).toPromise()).resolves.toBeUndefined();
    });

    test('stores a JSON-LD object', async () => {
      const result = silo.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      // Expecting nothing to be emitted for an insert
      await expect(result.toPromise()).resolves.toBeUndefined();
      await expect(result.tick).resolves.toBe(1);
    });

    test('retrieves a JSON-LD object', async () => {
      await silo.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject).toPromise();
      const result = silo.transact({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe);
      const fred = await result.toPromise();
      expect(fred['@id']).toBe('http://test.m-ld.org/fred');
      expect(fred['http://test.m-ld.org/#name']).toBe('Fred');
      await expect(result.tick).resolves.toBe(1);
    });

    test('has no ticks from genesis', async () => {
      expect(silo.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 0 });
    });

    test('has ticks after update', async () => {
      silo.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      await silo.follow().pipe(first()).toPromise();
      expect(silo.status.value).toEqual({ online: true, outdated: false, silo: true, ticks: 1 });
    });

    test('follow after initial ticks', async () => {
      const firstUpdate = silo.follow().pipe(first()).toPromise();
      silo.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      await expect(firstUpdate).resolves.toHaveProperty('@ticks', 1);
    });

    test('follow after current tick', async () => {
      const insertFred = silo.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      await insertFred.toPromise();
      await expect(insertFred.tick).resolves.toBe(1);
      expect(silo.status.value.ticks).toBe(1);
      const firstUpdate = silo.follow().pipe(first()).toPromise();
      await expect(silo.transact({
        '@id': 'http://test.m-ld.org/wilma',
        'http://test.m-ld.org/#name': 'Wilma'
      } as Subject).tick).resolves.toBe(2);
      await expect(firstUpdate).resolves.toHaveProperty('@ticks', 2);
    });
  });

  describe('as genesis with remote clone', () => {
    let clone: DatasetClone;
    let remoteTime: TreeClock;
    let remoteUpdates: Source<DeltaMessage> = new Source;

    beforeEach(async () => {
      const remotesLive = hotLive([false]);
      clone = new DatasetClone({
        dataset: await memStore(),
        // Ensure that remote updates are async
        remotes: mockRemotes(remoteUpdates.pipe(observeOn(asapScheduler)), remotesLive),
        config: testConfig()
      });
      await clone.initialise();
      await comesAlive(clone); // genesis is alive
      remoteTime = await clone.newClock(); // no longer genesis
      remotesLive.next(true); // remotes come alive
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
      const updates = clone.follow().pipe(map(next => next['@ticks']), take(2), toArray()).toPromise();
      clone.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      remoteUpdates.next(new DeltaMessage(remoteTime.ticked(),
        [0, uuid(), '{}', '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}']));
      await expect(updates).resolves.toEqual([1, 2]);
    });

    // Edge cases from system testing: newClock exposes the current clock state
    // even if it doesn't have a journalled entry. This can also happen due to:
    // 1. a remote transaction, because of the clock space made for a constraint
    test('answers rev-up from next new clone after apply', async () => {
      const updated = clone.follow().pipe(take(1)).toPromise();
      remoteUpdates.next(new DeltaMessage(remoteTime.ticked(),
        [0, uuid(), '{}', '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}']));
      await updated;
      const thirdTime = await clone.newClock();
      await expect(clone.revupFrom(thirdTime)).resolves.toBeDefined();
    });
    // 2. a failed transaction
    test('answers rev-up from next new clone after failure', async () => {
      // Insert with variables is not valid
      await clone.transact(<Update>{ '@insert': { '@id': '?s', '?p': '?o' } }).toPromise()
        .then(() => fail('Expecting error'), () => { });
      const thirdTime = await clone.newClock();
      await expect(clone.revupFrom(thirdTime)).resolves.toBeDefined();
    });
  });

  describe('as new clone', () => {
    let remotes: MeldRemotes;
    let snapshot: jest.Mock<Promise<Snapshot>, void[]>;
    const remotesLive = hotLive([true]);

    beforeEach(async () => {
      const { left: cloneClock, right: collabClock } = TreeClock.GENESIS.forked()
      remotes = mockRemotes(NEVER, remotesLive, cloneClock);
      snapshot = jest.fn().mockReturnValueOnce(Promise.resolve<Snapshot>({
        lastHash: Hash.random(),
        lastTime: collabClock.ticked(),
        quads: EMPTY,
        tids: EMPTY,
        updates: EMPTY
      }));
      remotes.snapshot = snapshot;
    });

    test('initialises from snapshot', async () => {
      const clone = new DatasetClone({ dataset: await memStore(), remotes, config: testConfig({ genesis: false }) });
      await clone.initialise();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(snapshot.mock.calls.length).toBe(1);
    });

    test('can become a silo', async () => {
      const clone = new DatasetClone({ dataset: await memStore(), remotes, config: testConfig({ genesis: false }) });
      await clone.initialise();
      remotesLive.next(false);
      await expect(clone.status.becomes({ silo: true })).resolves.toBeDefined();
    });
  });

  describe('as post-genesis clone', () => {
    let ldb: AbstractLevelDOWN;
    let config: MeldConfig;

    beforeEach(async () => {
      ldb = new MemDown();
      config = testConfig();
      // Start a temporary genesis clone to initialise the store
      let clone = new DatasetClone({ dataset: await memStore(ldb), remotes: mockRemotes(), config });
      await clone.initialise();
      await clone.newClock(); // Forks the clock so no longer genesis
      await clone.close();
      // Now the ldb represents a former genesis clone
    });

    test('is outdated while revving-up', async () => {
      // Re-start on the same data, with a rev-up that never completes
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => NEVER;
      const clone = new DatasetClone({ dataset: await memStore(ldb), remotes, config: testConfig() });

      // Check that we are never not outdated
      const everNotOutdated = clone.status.becomes({ outdated: false });

      await clone.initialise();

      expect(clone.status.value).toEqual({ online: true, outdated: true, silo: false, ticks: 0 });
      await expect(Promise.race([everNotOutdated, Promise.resolve()])).resolves.toBeUndefined();
    });

    test('is not outdated when revved-up', async () => {
      // Re-start on the same data, with a rev-up that never completes
      const remotes = mockRemotes(NEVER, [true]);
      remotes.revupFrom = async () => EMPTY;
      const clone = new DatasetClone({ dataset: await memStore(ldb), remotes, config: testConfig() });

      // Check that we do transition through an outdated state
      const wasOutdated = clone.status.becomes({ outdated: true });

      await clone.initialise();

      await expect(wasOutdated).resolves.toMatchObject({ online: true, outdated: true });
      await expect(clone.status.becomes({ outdated: false }))
        .resolves.toEqual({ online: true, outdated: false, silo: false, ticks: 0 });
    });

    test('immediately re-connects if rev-up fails', async () => {
      const remotes = mockRemotes(NEVER, [true]);
      const revupFrom = jest.fn()
        .mockReturnValueOnce(Promise.resolve(throwError('boom')))
        .mockReturnValueOnce(Promise.resolve(EMPTY));
      remotes.revupFrom = revupFrom;
      const clone = new DatasetClone({ dataset: await memStore(ldb), remotes, config: testConfig() });
      await clone.initialise();
      await expect(clone.status.becomes({ outdated: false })).resolves.toBeDefined();
      expect(revupFrom.mock.calls.length).toBe(2);
    });
  });
});
