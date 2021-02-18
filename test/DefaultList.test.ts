import { MeldReadState, InterimUpdate, MeldUpdate } from '../src/api';
import { memStore } from './testClones';
import { DefaultList } from '../src/constraints/DefaultList';
import { JrqlGraph } from '../src/engine/dataset/JrqlGraph';
import { GraphState } from '../src/engine/dataset/GraphState';
import { Dataset } from '../src/engine/dataset';
import { mock } from 'jest-mock-extended';
import { Update } from '../src/jrql-support';

// Note that DefaultList is quite heavily tested by MeldState.test.ts but not
// for apply mode
describe('Default list constraint', () => {
  let data: Dataset;
  let graph: JrqlGraph;
  let state: MeldReadState;

  beforeEach(async () => {
    data = await memStore();
    graph = new JrqlGraph(data.graph());
    state = new GraphState(graph);
  });

  test('Passes an empty update', async () => {
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': [],
      '@insert': []
    });
    // @ts-ignore 'Type instantiation is excessively deep and possibly infinite.ts(2589)'
    await expect(constraint.check(state, update)).resolves.toBeUndefined();
  });

  test('Rewrites a list insert', async () => {
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': [],
      // @ts-ignore 'Type instantiation is excessively deep and possibly infinite.ts(2589)'
      '@insert': [{
        '@id': 'http://test.m-ld.org/shopping',
        'data:,0': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
        }
      },
      {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
        'http://json-rql.org/#item': 'Bread'
      }]
    });
    await expect(constraint.check(state, update)).resolves.toBeUndefined();
    expect(update.remove).toBeCalledWith('@insert', {
      '@id': 'http://test.m-ld.org/shopping',
      'data:,0': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
      }
    });
    let indexKey: string | undefined;
    update.assert.mock.calls.forEach(u => {
      indexKey = findIndexKey(u);
      if (indexKey != null)
        expect(u).toEqual([{
          '@insert': {
            '@id': 'http://test.m-ld.org/shopping',
            [indexKey]: {
              '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
            }
          }
        }]);
    });
    expect(indexKey).toBeDefined(); // We got the expected rewrite
    expect(update.assert).toBeCalledWith({
      '@insert': {
        '@id': 'http://test.m-ld.org/shopping',
        '@type': 'http://m-ld.org/RdfLseq'
      }
    });
    expect(update.entail).toBeCalledWith(expect.objectContaining({
      '@insert': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
        'http://json-rql.org/#index': 0
      }
    }));
  });

  test('Resolves a slot conflict with rejected remote', async () => {
    // Create a well-formed list with one slot containing 'Bread'
    await data.transact({
      prepare: async () => ({
        patch: await graph.update({
          '@insert': {
            '@id': 'http://test.m-ld.org/shopping',
            '@type': 'http://m-ld.org/RdfLseq',
            'http://m-ld.org/RdfLseq/?=atest____________': {
              '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
              'http://json-rql.org/#item': 'Bread',
              'http://json-rql.org/#index': 0
            }
          }
        })
      })
    });
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': [],
      '@insert': [{
        '@id': 'http://test.m-ld.org/shopping',
        'http://m-ld.org/RdfLseq/?=bother___________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
        }
      }]
    });
    await expect(constraint.apply(state, update)).resolves.toBeDefined();
    expect(update.remove).not.toHaveBeenCalled();
    expect(update.assert).toBeCalledWith({
      '@delete': {
        '@id': 'http://test.m-ld.org/shopping',
        'http://m-ld.org/RdfLseq/?=bother___________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
        }
      }
    });
    expect(update.entail).not.toHaveBeenCalled(); // Index is the same
  });

  test('Resolves a slot conflict with replace current and no index move', async () => {
    // Create a well-formed list with one slot containing 'Bread'
    await data.transact({
      prepare: async () => ({
        patch: await graph.update({
          '@insert': {
            '@id': 'http://test.m-ld.org/shopping',
            '@type': 'http://m-ld.org/RdfLseq',
            'http://m-ld.org/RdfLseq/?=btest____________': {
              '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
              'http://json-rql.org/#item': 'Bread',
              'http://json-rql.org/#index': 0
            }
          }
        })
      })
    });
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': [],
      '@insert': [{
        '@id': 'http://test.m-ld.org/shopping',
        'http://m-ld.org/RdfLseq/?=aother___________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
        }
      }]
    });
    await expect(constraint.apply(state, update)).resolves.toBeDefined();
    expect(update.remove).not.toHaveBeenCalled();
    expect(update.assert).toBeCalledWith({
      '@delete': {
        '@id': 'http://test.m-ld.org/shopping',
        'http://m-ld.org/RdfLseq/?=btest____________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
        }
      }
    });
    expect(update.entail).not.toHaveBeenCalled(); // Index is the same
  });

  test('Resolves a slot conflict with a moved index', async () => {
    // Create a well-formed list with two slots 'Bread', 'Milk'
    await data.transact({
      prepare: async () => ({
        patch: await graph.update({
          '@insert': {
            '@id': 'http://test.m-ld.org/shopping',
            '@type': 'http://m-ld.org/RdfLseq',
            'http://m-ld.org/RdfLseq/?=btest____________': {
              '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
              'http://json-rql.org/#item': 'Bread',
              'http://json-rql.org/#index': 0
            },
            'http://m-ld.org/RdfLseq/?=dtest____________': {
              '@id': 'http://test.m-ld.org/.well-known/genid/slot1',
              'http://json-rql.org/#item': 'Milk',
              'http://json-rql.org/#index': 1
            }
          }
        })
      })
    });
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': [],
      '@insert': [{
        '@id': 'http://test.m-ld.org/shopping',
        // Slot one 'Milk' is moving to the head
        'http://m-ld.org/RdfLseq/?=aother___________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot1'
        }
      }]
    });
    await expect(constraint.apply(state, update)).resolves.toBeDefined();
    expect(update.remove).not.toHaveBeenCalled();
    expect(update.assert).toBeCalledWith({
      '@delete': {
        '@id': 'http://test.m-ld.org/shopping',
        'http://m-ld.org/RdfLseq/?=dtest____________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot1'
        }
      }
    });
    expect(update.entail).toBeCalledWith({
      '@insert': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot1',
        'http://json-rql.org/#index': 0
      },
      '@delete': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot1',
        'http://json-rql.org/#index': 1
      }
    });
    expect(update.entail).toBeCalledWith({
      '@insert': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
        'http://json-rql.org/#index': 1
      },
      '@delete': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
        'http://json-rql.org/#index': 0
      }
    });
    //console.log(JSON.stringify(update.entail.mock.calls, null, ' '))
  });
});

function mockInterim(update: MeldUpdate) {
  // Passing an implementation into the mock adds unwanted properties
  return Object.assign(mock<InterimUpdate>(), { update: Promise.resolve(update) });
}

function findIndexKey([u]: [Update]): string | undefined {
  return Object.keys(u['@insert'] ?? [])
    .find(k => k.startsWith('http://m-ld.org/RdfLseq/?='));
}
