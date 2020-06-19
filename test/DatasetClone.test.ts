import { DatasetClone } from '../src/dataset/DatasetClone';
import { Subject, Describe } from 'json-rql';
import { genesisClone, mockRemotes } from './testClones';
import { NEVER } from 'rxjs';
import { isOnline, comesOnline } from '../src/AbstractMeld';
import {  first } from 'rxjs/operators';
import { TreeClock } from '../src/clocks';

describe('Dataset clone', () => {
  describe('initialisation', () => {
    test('starts offline with unknown remotes', async () => {
      const clone = await genesisClone(mockRemotes(NEVER, [null]));
      await expect(isOnline(clone)).resolves.toBe(false);
    });

    test('connects if remotes online', async () => {
      const clone = await genesisClone(mockRemotes(NEVER, [true]));
      await expect(comesOnline(clone)).resolves.toBe(true);
    });

    test('comes online if siloed', async () => {
      const clone = await genesisClone(mockRemotes(NEVER, [null, false]));
      await expect(comesOnline(clone)).resolves.toBe(true);
    });

    test('stays online without reconnect if siloed', async () => {
      const clone = await genesisClone(mockRemotes(NEVER, [true, false]));
      await expect(comesOnline(clone)).resolves.toBe(true);
    });

    test('non-genesis fails to initialise if siloed', async () => {
      // This is a bit of a con: how did we get a clock if we're offline?
      await expect(genesisClone(mockRemotes(NEVER, [false],
        TreeClock.GENESIS.forked().left))).rejects.toThrow();
    });
  });

  describe('as a m-ld store', () => {
    let store: DatasetClone;

    beforeEach(async () => {
      store = await genesisClone();
    });

    test('not found is empty', async () => {
      await expect(store.transact({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe).toPromise()).resolves.toBeUndefined();
    });

    test('stores a JSON-LD object', async () => {
      await expect(store.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject).toPromise())
        // Expecting nothing to be emitted for an insert
        .resolves.toBeUndefined();
    });

    test('retrieves a JSON-LD object', async () => {
      await store.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject).toPromise();
      const fred = await store.transact({
        '@describe': 'http://test.m-ld.org/fred'
      } as Describe).toPromise();
      expect(fred['@id']).toBe('http://test.m-ld.org/fred');
      expect(fred['http://test.m-ld.org/#name']).toBe('Fred');
    });

    test('has no ticks from genesis', async () => {
      await expect(store.latest()).resolves.toBe(0);
    });

    test('has ticks after update', async () => {
      store.transact({
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': 'Fred'
      } as Subject);
      await store.follow().pipe(first()).toPromise();
      await expect(store.latest()).resolves.toBe(1);
    });
  });
});
