import { any, MeldUpdate } from '../src/api';
import { ApiStateMachine } from '../src/engine/MeldState';
import { memStore, mockRemotes, testConfig } from './testClones';
import { DatasetEngine } from '../src/engine/dataset/DatasetEngine';
import { Group, Subject, Select, Describe, Update } from '../src/jrql-support';
import { DomainContext } from '../src/engine/MeldEncoding';
import { Future } from '../src/engine/util';

describe('Meld State API', () => {
  let api: ApiStateMachine;

  beforeEach(async () => {
    let clone = new DatasetEngine({ dataset: await memStore(), remotes: mockRemotes(), config: testConfig() });
    await clone.initialise();
    api = new ApiStateMachine(new DomainContext('test.m-ld.org'), clone);
  });

  test('retrieves a JSON-LD subject', async () => {
    const captureUpdate = new Future<MeldUpdate>();
    api.follow(captureUpdate.resolve);
    await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 1,
      '@insert': [{ '@id': 'fred', name: 'Fred' }],
      '@delete': []
    });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
  });

  test('deletes a subject by update', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
    const captureUpdate = new Future<MeldUpdate>();
    api.follow(captureUpdate.resolve);
    await api.write<Update>({ '@delete': { '@id': 'fred' } });
    await expect(api.get('fred')).resolves.toBeUndefined();
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 2,
      '@delete': [{ '@id': 'fred', name: 'Fred' }],
      '@insert': []
    });
  });

  test('deletes a property by update', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
    const captureUpdate = new Future<MeldUpdate>();
    api.follow(captureUpdate.resolve);
    await api.write<Update>({ '@delete': { '@id': 'fred', height: 5 } });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 2,
      '@delete': [{ '@id': 'fred', height: 5 }],
      '@insert': []
    });
  });

  test('deletes where any', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
    await api.write<Update>({ '@delete': { '@id': 'fred', height: any() } });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
  });

  test('updates a property', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
    const captureUpdate = new Future<MeldUpdate>();
    api.follow(captureUpdate.resolve);
    await api.write<Update>({
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
    await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
    await api.write<Update>({
      '@delete': [{ '@id': 'fred', height: 5 }, { '@id': 'bambam' }]
    });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred', height: 5 });
  });

  test('inserts a subject by update', async () => {
    const captureUpdate = new Future<MeldUpdate>();
    api.follow(captureUpdate.resolve);
    await api.write<Update>({ '@insert': { '@id': 'fred', name: 'Fred' } });
    await expect(api.get('fred')).resolves.toBeDefined();
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 1,
      '@delete': [],
      '@insert': [{ '@id': 'fred', name: 'Fred' }]
    });
  });

  test('deletes a subject by path', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
    const captureUpdate = new Future<MeldUpdate>();
    api.follow(captureUpdate.resolve);
    await api.delete('fred');
    await expect(api.get('fred')).resolves.toBeUndefined();
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 2,
      '@delete': [{ '@id': 'fred', name: 'Fred' }],
      '@insert': []
    });
  });

  test('deletes a property by path', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred', wife: { '@id': 'wilma' } });
    await api.delete('wilma');
    await expect(api.get('fred')).resolves.toEqual({ '@id': 'fred', name: 'Fred' });
  });

  test('deletes an object by path', async () => {
    await api.write<Subject>({ '@id': 'fred', wife: { '@id': 'wilma' } });
    await api.delete('wilma');
    await expect(api.get('fred')).resolves.toBeUndefined();
  });

  test('selects where', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
    await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
    await expect(api.read<Select>({
      '@select': '?f', '@where': { '@id': '?f', name: 'Fred' }
    })).resolves.toMatchObject([{ '?f': { '@id': 'fred' } }]);
  });

  test('selects where union', async () => {
    await api.write<Group>({
      '@graph': [
        { '@id': 'fred', name: 'Fred' },
        { '@id': 'wilma', name: 'Wilma' }
      ]
    });
    await expect(api.read<Select>({
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
    await api.write({ '@id': 'fred', name: 'Fred' } as Subject);
    await expect(api.read<Select>({
      '@select': '?w', '@where': { '@id': '?w', name: 'Wilma' }
    })).resolves.toEqual([]);
  });

  test('describes where', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
    await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
    await expect(api.read<Describe>({
      '@describe': '?f', '@where': { '@id': '?f', name: 'Fred' }
    })).resolves.toEqual([{ '@id': 'fred', name: 'Fred' }]);
  });

  test('describes with boolean value', async () => {
    await api.write<Subject>({ '@id': 'fred', married: true });
    await expect(api.read<Describe>({ '@describe': 'fred' }))
      .resolves.toEqual([{ '@id': 'fred', married: true }]);
  });

  test('describes with double value', async () => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred', age: 40.5 });
    await expect(api.read<Describe>({ '@describe': 'fred' }))
      .resolves.toMatchObject([{ '@id': 'fred', name: 'Fred', age: 40.5 }]);
  });
});
