import { Update } from '../src';
import { MockGraphState, mockInterim } from './testClones';
import { DefaultList } from '../src/constraints/DefaultList';
import { SubjectGraph } from '../src/engine/SubjectGraph';

// Note that DefaultList is quite heavily tested by MeldState.test.ts but not
// for apply mode
describe('Default list constraint', () => {
  let state: MockGraphState;

  beforeEach(async () => {
    state = await MockGraphState.create();
  });

  afterEach(() => state.close());

  test('Passes an empty update', async () => {
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([])
    });
    // @ts-ignore 'Type instantiation is excessively deep and possibly infinite.ts(2589)'
    await expect(constraint.check(state.graph.asReadState, update)).resolves.toBeUndefined();
  });

  test('Rewrites a list insert', async () => {
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/shopping',
        '@list': {
          0: { '@id': 'http://test.m-ld.org/.well-known/genid/slot0' }
        }
      },
        {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
          '@item': 'Bread'
        }])
    });
    await expect(constraint.check(state.graph.asReadState, update)).resolves.toBeUndefined();
    expect(update.remove).toBeCalledWith('@insert', {
      '@id': 'http://test.m-ld.org/shopping',
      '@list': {
        0: { '@id': 'http://test.m-ld.org/.well-known/genid/slot0' }
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
        '@index': 0
      }
    }));
  });

  test('Resolves a slot conflict with rejected remote', async () => {
    // Create a well-formed list with one slot containing 'Bread'
    await state.write({
      '@insert': {
        '@id': 'http://test.m-ld.org/shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        'http://m-ld.org/RdfLseq/?=atest____________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
          '@item': 'Bread',
          '@index': 0
        }
      }
    });
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/shopping',
        'http://m-ld.org/RdfLseq/?=bother___________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
        }
      }])
    });
    await expect(constraint.apply(state.graph.asReadState, update)).resolves.toBeDefined();
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
    await state.write({
      '@insert': {
        '@id': 'http://test.m-ld.org/shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        'http://m-ld.org/RdfLseq/?=btest____________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
          '@item': 'Bread',
          '@index': 0
        }
      }
    });
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/shopping',
        'http://m-ld.org/RdfLseq/?=aother___________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0'
        }
      }])
    });
    await expect(constraint.apply(state.graph.asReadState, update)).resolves.toBeDefined();
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
    await state.write({
      '@insert': {
        '@id': 'http://test.m-ld.org/shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        'http://m-ld.org/RdfLseq/?=btest____________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
          '@item': 'Bread',
          '@index': 0
        },
        'http://m-ld.org/RdfLseq/?=dtest____________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot1',
          '@item': 'Milk',
          '@index': 1
        }
      }
    });
    const constraint = new DefaultList('test');
    const update = mockInterim({
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/shopping',
        // Slot one 'Milk' is moving to the head
        'http://m-ld.org/RdfLseq/?=aother___________': {
          '@id': 'http://test.m-ld.org/.well-known/genid/slot1'
        }
      }])
    });
    await expect(constraint.apply(state.graph.asReadState, update)).resolves.toBeDefined();
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
        '@index': 0
      },
      '@delete': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot1',
        '@index': 1
      }
    });
    expect(update.entail).toBeCalledWith({
      '@insert': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
        '@index': 1
      },
      '@delete': {
        '@id': 'http://test.m-ld.org/.well-known/genid/slot0',
        '@index': 0
      }
    });
  });
});

function findIndexKey([u]: [Update]): string | undefined {
  return Object.keys(u['@insert'] ?? [])
    .find(k => k.startsWith('http://m-ld.org/RdfLseq/?='));
}
