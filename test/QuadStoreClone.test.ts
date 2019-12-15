import { QuadStoreClone } from '../src/QuadStoreClone';
import { Subject, Describe } from '../src/jsonrql';
import MemDown from 'memdown';

describe('Meld store implementation', () => {
  test('stores a JSON-LD object', async () => {
    await expect(new QuadStoreClone(new MemDown).transact({
      '@id': 'http://test.m-ld.org/fred',
      'http://test.m-ld.org/#name': 'Fred'
    } as Subject).toPromise())
      // Expecting nothing to be emitted for an insert
      .resolves.toBeUndefined();
  });

  test('retrieves a JSON-LD object', async () => {
    const clone = new QuadStoreClone(new MemDown);
    await clone.transact({
      '@id': 'http://test.m-ld.org/fred',
      'http://test.m-ld.org/#name': 'Fred'
    } as Subject).toPromise();
    const fred = await clone.transact({
      '@describe': 'http://test.m-ld.org/fred'
    } as Describe).toPromise();
    expect(fred['@id']).toBe('http://test.m-ld.org/fred');
    expect(fred['http://test.m-ld.org/#name']).toBe('Fred');
  });
});