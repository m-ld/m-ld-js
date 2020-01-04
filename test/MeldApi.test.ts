import { DatasetClone } from '../src/DatasetClone';
import { MeldApi } from '../src/MeldApi';
import { Subject } from '../src/jsonrql';
import MemDown from 'memdown';
import { QuadStoreDataset } from '../src/Dataset';
import { MeldRemotes } from '../src/meld';
import { mock } from 'jest-mock-extended';


describe('Meld API', () => {
  let api: MeldApi;

  beforeEach(async () => {
    const clone = new DatasetClone(new QuadStoreDataset(new MemDown, { id: 'test' }), mock<MeldRemotes>());
    clone.genesis = true;
    await clone.initialise();
    api = new MeldApi('test.m-ld.org', null, clone);
  });

  test('retrieves a JSON-LD object', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.get('fred').toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred' });
  });
});