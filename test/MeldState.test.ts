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
    const context = new DomainContext('test.m-ld.org');
    let clone = new DatasetEngine({
      dataset: await memStore({ context }),
      remotes: mockRemotes(),
      config: testConfig()
    });
    await clone.initialise();
    api = new ApiStateMachine(context, clone);
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
    // This write has no effect because we're asking for triples with subject of
    // both fred and bambam
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

  test('inserts an anonymous subject', async () => {
    await api.write<Update>({ '@insert': { name: 'Fred' } });
    await expect(api.read({
      '@describe': '?s',
      '@where': { '@id': '?s', name: 'Fred' }
    })).resolves.toMatchObject([{ name: 'Fred' }]);
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

  test('reads with a procedure', async done => {
    await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
    api.read(async state => {
      await expect(state.read<Describe>({ '@describe': 'fred' }))
        .resolves.toMatchObject([{ '@id': 'fred', name: 'Fred' }]);
      done();
    });
  });

  test('writes with a procedure', async done => {
    api.write(async state => {
      state = await state.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await expect(state.read<Describe>({ '@describe': 'fred' }))
        .resolves.toMatchObject([{ '@id': 'fred', name: 'Fred' }]);
      done();
    });
  });

  test('write state is predictable', async done => {
    api.write(async state => {
      state = await state.write<Subject>({ '@id': 'fred', age: 40 });
      await expect(state.read<Describe>({
        '@describe': '?id', '@where': { '@id': '?id', age: 40 }
      }))
        // We only expect one person of that age
        .resolves.toMatchObject([{ '@id': 'fred', age: 40 }]);
      done();
    });
    // Immediately make another write which could affect the query
    api.write(state => state.write<Subject>({ '@id': 'wilma', age: 40 }));
  });

  test('handler state follows writes', async done => {
    let hadFred = false;
    api.read(async state => {
      await expect(state.read<Describe>({
        '@describe': '?id', '@where': { '@id': '?id', age: 40 }
      })).resolves.toEqual([]);
    }, async (_, state) => {
      if (!hadFred) {
        await expect(state.read<Describe>({
          '@describe': '?id', '@where': { '@id': '?id', age: 40 }
        })).resolves.toMatchObject([{ '@id': 'fred', age: 40 }]);
        hadFred = true;
      } else {
        await expect(state.read<Describe>({
          '@describe': '?id', '@where': { '@id': '?id', age: 40 }
        })).resolves.toMatchObject([{ '@id': 'wilma', age: 40 }]);
        done();
      }
    });
    api.write(async state => {
      state = await state.write<Subject>({ '@id': 'fred', age: 40 });
      await state.write<Subject>({
        '@delete': { '@id': 'fred', age: 40 },
        '@insert': { '@id': 'wilma', age: 40 }
      });
    });
  });
});
