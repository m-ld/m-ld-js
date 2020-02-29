import { MeldApi } from '../src/m-ld/MeldApi';
import { Subject, Select, Describe } from '../src/m-ld/jsonrql';
import { genesisClone } from './testClones';
import { first } from 'rxjs/operators';

describe('Meld API', () => {
  let api: MeldApi;

  beforeEach(async () => {
    api = new MeldApi('test.m-ld.org', null, await genesisClone());
  });

  test('retrieves a JSON-LD subject', async () => {
    const captureUpdate = api.follow().pipe(first()).toPromise();
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(captureUpdate).resolves.toEqual({
      '@insert': { '@graph': [{ '@id': 'fred', name: 'Fred' }] },
      '@delete': { '@graph': [] }
    });
    await expect(api.get('fred').toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred' });
  });

  test('deletes a subject by path', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    const captureUpdate = api.follow().pipe(first()).toPromise();
    await api.delete('fred').toPromise();
    await expect(api.get('fred').toPromise()).resolves.toBeUndefined();
    await expect(captureUpdate).resolves.toEqual({
      '@delete': { '@graph': [{ '@id': 'fred', name: 'Fred' }] },
      '@insert': { '@graph': [] }
    });
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

  test('describes where', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.transact({
      '@describe': '?f', '@where': { '@id': '?f', name: 'Fred' }
    } as Describe).toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred' });
  });
});
