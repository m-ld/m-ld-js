import { MeldApi, Resource, any } from '../src/m-ld/MeldApi';
import { memStore, mockRemotes, testConfig } from './testClones';
import { first } from 'rxjs/operators';
import { DatasetClone } from '../src/dataset/DatasetClone';
import { Group, Subject, Select, Describe, Reference, Update } from '../src/dataset/jrql-support';
import { DomainContext } from '../src/m-ld/MeldJson';

describe('Meld API', () => {
  let api: MeldApi;

  beforeEach(async () => {
    let clone = new DatasetClone({ dataset: await memStore(), remotes: mockRemotes(), config: testConfig() });
    await clone.initialise();
    api = new MeldApi(new DomainContext('test.m-ld.org'), clone);
  });

  test('retrieves a JSON-LD subject', async () => {
    const captureUpdate = api.follow().pipe(first()).toPromise();
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred' });
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 1,
      '@insert': [{ '@id': 'fred', name: 'Fred' }],
      '@delete': []
    });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
  });

  test('deletes a subject by update', async () => {
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred' });
    const captureUpdate = api.follow().pipe(first()).toPromise();
    await api.transact<Update>({ '@delete': { '@id': 'fred' } });
    await expect(api.get('fred')).resolves.toBeUndefined();
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 2,
      '@delete': [{ '@id': 'fred', name: 'Fred' }],
      '@insert': []
    });
  });

  test('deletes a property by update', async () => {
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
    const captureUpdate = api.follow().pipe(first()).toPromise();
    await api.transact<Update>({ '@delete': { '@id': 'fred', height: 5 } });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 2,
      '@delete': [{ '@id': 'fred', height: 5 }],
      '@insert': []
    });
  });

  test('deletes where any', async () => {
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
    await api.transact<Update>({ '@delete': { '@id': 'fred', height: any() } });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
  });

  test('updates a property', async () => {
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
    const captureUpdate = api.follow().pipe(first()).toPromise();
    await api.transact<Update>({
      '@delete': { '@id': 'fred', height: 5 },
      '@insert': { '@id': 'fred', height: 6 }
    });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred', height: 6 });
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 2,
      '@delete': [{ '@id': 'fred', height: 5 }],
      '@insert': [{ '@id': 'fred', height: 6 }]
    });
  });

  test('delete where must match all', async () => {
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
    await api.transact<Update>({
      '@delete': [{ '@id': 'fred', height: 5 }, { '@id': 'bambam' }]
    });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred', height: 5 });
  });

  test('inserts a subject by update', async () => {
    const captureUpdate = api.follow().pipe(first()).toPromise();
    await api.transact<Update>({ '@insert': { '@id': 'fred', name: 'Fred' } });
    await expect(api.get('fred')).resolves.toBeDefined();
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 1,
      '@delete': [],
      '@insert': [{ '@id': 'fred', name: 'Fred' }]
    });
  });

  test('deletes a subject by path', async () => {
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred' });
    const captureUpdate = api.follow().pipe(first()).toPromise();
    await api.delete('fred');
    await expect(api.get('fred')).resolves.toBeUndefined();
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 2,
      '@delete': [{ '@id': 'fred', name: 'Fred' }],
      '@insert': []
    });
  });

  test('deletes a property by path', async () => {
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred', wife: { '@id': 'wilma' } });
    await api.delete('wilma');
    await expect(api.get('fred')).resolves.toEqual({ '@id': 'fred', name: 'Fred' });
  });

  test('deletes an object by path', async () => {
    await api.transact<Subject>({ '@id': 'fred', wife: { '@id': 'wilma' } });
    await api.delete('wilma');
    await expect(api.get('fred')).resolves.toBeUndefined();
  });

  test('selects where', async () => {
    await api.transact<Subject>({ '@id': 'fred', name: 'Fred' });
    await expect(api.transact({
      '@select': '?f', '@where': { '@id': '?f', name: 'Fred' }
    } as Select))
      .resolves.toMatchObject([{ '?f': { '@id': 'fred' } }]);
  });

  test('selects where union', async () => {
    await api.transact<Group>({
      '@graph': [
        { '@id': 'fred', name: 'Fred' },
        { '@id': 'wilma', name: 'Wilma' }
      ]
    });
    await expect(api.transact<Select>({
      '@select': '?s', '@where': {
        '@union': [
          { '@id': '?s', name: 'Wilma' },
          { '@id': '?s', name: 'Fred' }
        ]
      }
    })).resolves.toEqual(expect.arrayContaining([
      expect.objectContaining({ '?s': { '@id': 'fred' } }),
      expect.objectContaining({ '?s': { '@id': 'wilma' } })
    ]));
  });

  test('selects not found', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject);
    await expect(api.transact<Select>({
      '@select': '?w', '@where': { '@id': '?w', name: 'Wilma' }
    })).resolves.toEqual([]);
  });

  test('describes where', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred' } as Subject);
    await expect(api.transact({
      '@describe': '?f', '@where': { '@id': '?f', name: 'Fred' }
    } as Describe))
      .resolves.toEqual([{ '@id': 'fred', name: 'Fred' }]);
  });

  test('describes with boolean value', async () => {
    await api.transact({ '@id': 'fred', married: true } as Subject);
    await expect(api.transact({ '@describe': 'fred' } as Describe))
      .resolves.toEqual([{ '@id': 'fred', married: true }]);
  });

  test('describes with double value', async () => {
    await api.transact({ '@id': 'fred', name: 'Fred', age: 40.5 } as Subject);
    await expect(api.transact({ '@describe': 'fred' } as Describe))
      .resolves.toMatchObject([{ '@id': 'fred', name: 'Fred', age: 40.5 }]);
  });
});

describe('Node utility', () => {
  test('converts simple group update to subject updates', () => {
    expect(MeldApi.asSubjectUpdates({
      '@delete': [{ '@id': 'foo', size: 10 }],
      '@insert': [{ '@id': 'foo', size: 20 }]
    })).toEqual({
      'foo': {
        '@delete': { '@id': 'foo', size: 10 },
        '@insert': { '@id': 'foo', size: 20 }
      }
    });
  });

  test('converts array group update to subject updates', () => {
    expect(MeldApi.asSubjectUpdates({
      '@delete': [{ '@id': 'foo', size: 10 }, { '@id': 'bar', size: 30 }],
      '@insert': [{ '@id': 'foo', size: 20 }, { '@id': 'bar', size: 40 }]
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
    const box: Resource<Box> = { '@id': 'bar', size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': {} });
    expect(box).toEqual({ '@id': 'bar', size: 10, label: 'My box' });
  });

  test('adds a missing value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10 };
    MeldApi.update(box, { '@insert': { '@id': 'foo', label: 'My box' }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, label: 'My box' });
  });

  test('adds an array value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10 };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: [20, 30] }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: [10, 20, 30] });
  });

  test('does not add an empty array value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10 };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: [] }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10 });
  });

  test('adds an inserted value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: [10, 20], label: 'My box' });
  });

  test('does not insert a duplicate value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: 10 }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, label: 'My box' });
  });

  test('removes a deleted value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    MeldApi.update(box, { '@delete': { '@id': 'foo', size: 10 }, '@insert': {} });
    expect(box).toEqual({ '@id': 'foo', size: [], label: 'My box' });
  });

  test('updates a value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': { '@id': 'foo', size: 10 } });
    expect(box).toEqual({ '@id': 'foo', size: 20, label: 'My box' });
  });

  test('updates unchanged value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    MeldApi.update(box, { '@insert': { '@id': 'foo', size: 10 }, '@delete': { '@id': 'foo', size: 10 } });
    expect(box).toEqual({ '@id': 'foo', size: 10, label: 'My box' });
  });

  // FIXME This breaks the Node type, but not possible to prevent at runtime
  test('adds a singleton reference as a singleton if array property undefined', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10 };
    MeldApi.update(box, { '@insert': { '@id': 'foo', contents: { '@id': 'bar' } }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: { '@id': 'bar' } });
  });

  test('adds a singleton reference into array if array property defined', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, contents: [] };
    MeldApi.update(box, { '@insert': { '@id': 'foo', contents: { '@id': 'bar' } }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: [{ '@id': 'bar' }] });
  });

  test('updates a reference', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, contents: [{ '@id': 'bar' }] };
    MeldApi.update(box, {
      '@insert': { '@id': 'foo', contents: { '@id': 'baz' } },
      '@delete': { '@id': 'foo', contents: { '@id': 'bar' } }
    });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: [{ '@id': 'baz' }] });
  });
});
