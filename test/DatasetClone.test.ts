import { DatasetClone } from '../src/DatasetClone';
import { Subject, Describe } from '../src/jsonrql';
import MemDown from 'memdown';
import { QuadStoreDataset } from '../src/Dataset';
import { MeldStore, MeldRemotes } from '../src/meld';
import { mock } from 'jest-mock-extended';

describe('Meld store implementation', () => {
  let store: DatasetClone;

  beforeEach(async () => {
    store = new DatasetClone(new QuadStoreDataset(new MemDown, { id: 'test' }), mock<MeldRemotes>());
    store.genesis = true;
    await store.initialise(); 
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
});