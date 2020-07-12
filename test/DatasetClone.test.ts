import { DatasetClone } from '../src/dataset/DatasetClone';
import { Subject, Describe } from 'json-rql';
import { memStore, mockRemotes, testConfig } from './testClones';
import { NEVER, Subject as Source, asapScheduler, EMPTY } from 'rxjs';
import { comesAlive } from '../src/AbstractMeld';
import { first, take, toArray, map, observeOn } from 'rxjs/operators';
import { TreeClock } from '../src/clocks';
import { DeltaMessage, MeldRemotes } from '../src/m-ld';
import { uuid } from 'short-uuid';
import { MeldConfig } from '../src';
import MemDown from 'memdown';
import { AbstractLevelDOWN } from 'abstract-leveldown';

describe('Dataset clone', () => {
  describe('as genesis', () => {
    async function genesis(remotes: MeldRemotes, config?: Partial<MeldConfig>): Promise<DatasetClone> {
      let clone = new DatasetClone(await memStore(), remotes, testConfig(config));
      await clone.initialise();
      return clone;
    }

    test('starts offline with unknown remotes', async () => {
      const clone = await genesis(mockRemotes(NEVER, [null]));
      expect(clone.live.value).toBe(false);
      expect(clone.status.value).toEqual({ online: false, outdated: false, ticks: 0 });
    });

    test('connects if remotes live', async () => {
      const clone = await genesis(mockRemotes(NEVER, [true]));
      await expect(comesAlive(clone)).resolves.toBe(true);
      expect(clone.status.value).toEqual({ online: true, outdated: false, ticks: 0 });
    });

    test('comes alive if siloed', async () => {
      const clone = await genesis(mockRemotes(NEVER, [null, false]));
      await expect(comesAlive(clone)).resolves.toBe(true);
      expect(clone.status.value).toEqual({ online: true, outdated: false, ticks: 0 });
    });

    test('stays live without reconnect if siloed', async () => {
      const clone = await genesis(mockRemotes(NEVER, [true, false]));
      await expect(comesAlive(clone)).resolves.toBe(true);
      expect(clone.status.value).toEqual({ online: true, outdated: false, ticks: 0 });
    });

    test('non-genesis fails to initialise if siloed', async () => {
      await expect(genesis(mockRemotes(NEVER, [false],
        TreeClock.GENESIS.forked().left), { genesis: false })).rejects.toThrow();
    });
  });

  describe('as silo genesis', () => {
    let silo: DatasetClone;

    beforeEach(async () => {
      silo = new DatasetClone(await memStore(), mockRemotes(), testConfig());
      await silo.initialise();
    });

    test('not found is empty', async () => {
      await expect(silo.transact({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe).toPromise()).resolves.toBeUndefined();
    });

    test('stores a JSON-LD object', async () => {
      await expect(silo.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject).toPromise())
        // Expecting nothing to be emitted for an insert
        .resolves.toBeUndefined();
    });

    test('retrieves a JSON-LD object', async () => {
      await silo.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject).toPromise();
      const fred = await silo.transact({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe).toPromise();
      expect(fred['@id']).toBe('http://test.m-ld.org/fred');
      expect(fred['http://test.m-ld.org/#name']).toBe('Fred');
    });

    test('has no ticks from genesis', async () => {
      expect(silo.status.value).toEqual({ online: true, outdated: false, ticks: 0 });
    });

    test('has ticks after update', async () => {
      silo.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      await silo.follow().pipe(first()).toPromise();
      expect(silo.status.value).toEqual({ online: true, outdated: false, ticks: 1 });
    });
  });

  describe('as genesis with remote clone', () => {
    let clone: DatasetClone;
    let remoteTime: TreeClock;
    let remoteUpdates: Source<DeltaMessage> = new Source;

    beforeEach(async () => {
      // Ensure that remote updates are async
      clone = new DatasetClone(await memStore(),
        mockRemotes(remoteUpdates.pipe(observeOn(asapScheduler))), testConfig());
      await clone.initialise();
      await comesAlive(clone);
      remoteTime = await clone.newClock();
    });

    test('ticks increase monotonically', async () => {
      // Edge case from compliance tests: a local transaction racing a remote
      // transaction could cause a clock reversal.
      const updates = clone.follow().pipe(map(next => next['@ticks']), take(2), toArray()).toPromise();
      clone.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      remoteUpdates.next(new DeltaMessage(remoteTime.ticked(), {
        tid: uuid(),
        insert: '{"@id":"http://test.m-ld.org/wilma","http://test.m-ld.org/#name":"Wilma"}',
        delete: '{}'
      }));
      await expect(updates).resolves.toEqual([1, 2]);
    });
  });

  describe('as non-genesis clone', () => {
    let ldb: AbstractLevelDOWN;
    let config: MeldConfig;

    beforeEach(async () => {
      ldb = new MemDown();
      config = testConfig();
      // Start a temporary genesis clone to initialise the store
      let clone = new DatasetClone(await memStore(ldb), mockRemotes(), config);
      await clone.initialise();
      await clone.newClock(); // Forks the clock so no longer genesis
      await clone.close();
    });

    test('is outdated while revving-up', async () => {
      // Re-start on the same data, with a rev-up that never completes
      const remotes = mockRemotes();
      remotes.revupFrom = async () => NEVER;
      const clone = new DatasetClone(await memStore(ldb), remotes, testConfig());

      // Check that we are never not outdated
      const everNotOutdated = clone.status.becomes({ outdated: false });

      await clone.initialise();

      expect(clone.status.value).toEqual({ online: true, outdated: true, ticks: 0 });
      await expect(Promise.race([everNotOutdated, Promise.resolve()])).resolves.toBeUndefined();
    });

    test('is not outdated when revved-up', async () => {
      // Re-start on the same data, with a rev-up that never completes
      const remotes = mockRemotes();
      remotes.revupFrom = async () => EMPTY;
      const clone = new DatasetClone(await memStore(ldb), remotes, testConfig());

      // Check that we do transition through an outdated state
      const wasOutdated = clone.status.becomes({ outdated: true });

      await clone.initialise();

      await expect(wasOutdated).resolves.toMatchObject({ online: true, outdated: true });
      expect(clone.status.value).toEqual({ online: true, outdated: false, ticks: 0 });
    });
  });
});
