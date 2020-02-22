import { MeldApi } from '../src/m-ld/MeldApi';
import { Subject, Select } from '../src/m-ld/jsonrql';
import { genesisClone } from './testClones';

describe('Meld API', () => {
  let api: MeldApi;

  beforeEach(async () => {
    api = new MeldApi('test.m-ld.org', null, await genesisClone());
  });

  test('retrieves a JSON-LD subject', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.get('fred').toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred' });
  });

  test('deletes a subject by path', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await api.delete('fred').toPromise();
    await expect(api.get('fred').toPromise()).resolves.toBeUndefined();
  });

  test('deletes an object by path', async () => {
    await api.transact({ '@id': 'fred', wife: { '@id': 'wilma' } } as Subject).toPromise();
    await api.delete('wilma').toPromise();
    await expect(api.get('fred').toPromise()).resolves.toBeUndefined();
  });

  test('selects where', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.transact({
      '@select': '?f', '@where': { '@id': '?f', name: 'Fred' }
    } as Select).toPromise())
      .resolves.toMatchObject({ '?f': { '@id': 'fred' } });
  });
});
