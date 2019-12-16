import { QuadStoreClone } from '../src/QuadStoreClone';
import { MeldApi } from '../src/MeldApi';
import { Subject, Describe } from '../src/jsonrql';
import MemDown from 'memdown';

describe('Meld API', () => {
  let api: MeldApi;

  beforeEach(() => {
    api = new MeldApi('test.m-ld.org', {}, new QuadStoreClone(new MemDown));
  });

  test('retrieves a JSON-LD object', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.get('fred').toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred' });
  });
});