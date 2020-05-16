import { MeldApi } from '../src/m-ld/MeldApi';
import { Subject, Select, Describe, Reference } from '../src/m-ld/jsonrql';
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
      '@ticks': 1,
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
      '@ticks': 2,
      '@delete': { '@graph': [{ '@id': 'fred', name: 'Fred' }] },
      '@insert': { '@graph': [] }
    });
  });

  test('deletes a property by path', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred', wife: { '@id': 'wilma' } } as Subject).toPromise();
    await api.delete('wilma').toPromise();
    await expect(api.get('fred').toPromise()).resolves.toEqual({ '@id': 'fred', name: 'Fred' });
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

  test('selects not found', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.transact({
      '@select': '?w', '@where': { '@id': '?w', name: 'Wilma' }
    } as Select).toPromise())
      .resolves.toBeUndefined(); // toPromise undefined for empty Observable
  });

  test('describes where', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject).toPromise();
    await expect(api.transact({
      '@describe': '?f', '@where': { '@id': '?f', name: 'Fred' }
    } as Describe).toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred' });
  });

  test('describes with boolean value', async () => {
    await api.transact({ '@id': 'fred', married: true } as Subject).toPromise();
    await expect(api.transact({ '@describe': 'fred' } as Describe).toPromise())
      .resolves.toMatchObject({ '@id': 'fred', married: true });
  });

  test('describes with double value', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred', age: 40.5 } as Subject).toPromise();
    await expect(api.transact({ '@describe': 'fred' } as Describe).toPromise())
      .resolves.toMatchObject({ '@id': 'fred', name: 'Fred', age: 40.5 });
  });
});

describe('Node utility', () => {
  test('converts simple group update to subject updates', () => {
    expect(MeldApi.asSubjectUpdates({
      '@delete': { '@graph': { '@id': 'foo', size: 10 } },
      '@insert': { '@graph': { '@id': 'foo', size: 20 } }
    })).toEqual({
      'foo': {
        '@delete': { '@id': 'foo', size: 10 },
        '@insert': { '@id': 'foo', size: 20 }
      }
    });
  });

  test('converts array group update to subject updates', () => {
    expect(MeldApi.asSubjectUpdates({
      '@delete': { '@graph': [{ '@id': 'foo', size: 10 }, { '@id': 'bar', size: 30 }] },
      '@insert': { '@graph': [{ '@id': 'foo', size: 20 }, { '@id': 'bar', size: 40 }] }
    })).toEqual({
      'foo': {
        '@delete': { '@id': 'foo', size: 10 },
        '@insert': { '@id': 'foo', size: 20 }
      },
      'bar': {
        '@delete': { '@id': 'bar', size: 30 },
        '@insert': { '@id': 'bar', size: 40 }
      }
    });
  });

  interface Box {
    size: number;
    label?: string;
    contents?: Reference[];
  }

  test('does not update mismatching ids', () => {
    const box: MeldApi.Node<Box> = { '@id': 'bar', size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': {} });
    expect(box).toEqual({ '@id': 'bar', size: 10, label: 'My box' });
  });

  test('adds a missing value', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10 };
    MeldApi.update(box, { '@insert': { '@id': 'foo', label: 'My box' }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, label: 'My box' });
  });

  test('adds an array value', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10 };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: [20, 30] }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: [10, 20, 30] });
  });

  test('does not add an empty array value', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10 };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: [] }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10 });
  });

  test('adds an inserted value', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: [10, 20], label: 'My box' });
  });

  test('removes a deleted value', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    MeldApi.update(box, { '@delete': { '@id': 'foo', size: 10 }, '@insert': {} });
    expect(box).toEqual({ '@id': 'foo', size: [], label: 'My box' });
  });

  test('updates a value', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': { '@id': 'foo', size: 10 } });
    expect(box).toEqual({ '@id': 'foo', size: 20, label: 'My box' });
  });

  test('updates a value on anonymous node', () => {
    const box: MeldApi.Node<Box> = { size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { size: 20 }, '@delete': { size: 10 } });
    expect(box).toEqual({ size: 20, label: 'My box' });
  });

  // FIXME This breaks the Node type, but not possible to prevent at runtime
  test('adds a singleton reference as a singleton if array property undefined', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10 };
    MeldApi.update(box, { '@insert': { '@id': 'foo', contents: { '@id': 'bar' } }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: { '@id': 'bar' } });
  });

  test('adds a singleton reference into array if array property defined', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10, contents: [] };
    MeldApi.update(box, { '@insert': { '@id': 'foo', contents: { '@id': 'bar' } }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: [{ '@id': 'bar' }] });
  });

  test('updates a reference', () => {
    const box: MeldApi.Node<Box> = { '@id': 'foo', size: 10, contents: [{ '@id': 'bar' }] };
    MeldApi.update(box, {
      '@insert': { '@id': 'foo', contents: { '@id': 'baz' } },
      '@delete': { '@id': 'foo', contents: { '@id': 'bar' } }
    });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: [{ '@id': 'baz' }] });
  });
});
