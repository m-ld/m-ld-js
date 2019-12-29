import { DatasetClone } from '../src/DatasetClone';
import { MeldApi } from '../src/MeldApi';
import { Subject, Describe } from '../src/jsonrql';
import MemDown from 'memdown';
import { QuadStoreDataset } from '../src/Dataset';
import { RdfStore } from 'quadstore';

describe('Meld API', () => {
  let api: MeldApi;

  beforeEach(() => {
    const clone = new DatasetClone(new QuadStoreDataset(new MemDown, { id: 'test' }));
    api = new MeldApi('test.m-ld.org', null, clone);
  });

  test('retrieves a JSON-LD object', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.get('fred').toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred' });
  });
});