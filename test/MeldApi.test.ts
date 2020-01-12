import { MeldApi } from '../src/MeldApi';
import { Subject } from '../src/jsonrql';
import { genesisClone } from './testClones';

describe('Meld API', () => {
  let api: MeldApi;

  beforeEach(async () => {
    api = new MeldApi('test.m-ld.org', null, await genesisClone());
  });

  test('retrieves a JSON-LD object', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.get('fred').toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred' });
  });
});
