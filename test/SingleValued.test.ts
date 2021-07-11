import { InterimUpdate, MeldReadState, MeldUpdate } from '../src';
import { memStore } from './testClones';
import { SingleValued } from '../src/constraints/SingleValued';
import { JrqlGraph } from '../src/engine/dataset/JrqlGraph';
import { GraphState } from '../src/engine/dataset/GraphState';
import { Dataset } from '../src/engine/dataset';
import { mock } from 'jest-mock-extended';
import { SubjectGraph } from '../src/engine/SubjectGraph';
import { firstValueFrom } from 'rxjs';

describe('Single-valued constraint', () => {
  let data: Dataset;
  let graph: JrqlGraph;
  let state: MeldReadState;

  beforeEach(async () => {
    data = await memStore();
    graph = new JrqlGraph(data.graph());
    state = new GraphState(graph);
  });

  test('Passes an empty update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([])
    }))).resolves.toBeUndefined();
  });

  test('Passes a missing property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#height': 5
      }])
    }))).resolves.toBeUndefined();
  });

  test('Passes a single-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
      }])
    }))).resolves.toBeUndefined();
  });

  test('Fails a multi-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
      }])
    }))).rejects.toBeDefined();
  });

  test('Fails a single-valued additive property update', async () => {
    await data.transact({
      prepare: async () => ({
        patch: await graph.write({
          '@insert': { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }
        })
      })
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
      }])
    }))).rejects.toBeDefined();
  });

  test('does not apply to a single-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
      }])
    });
    // @ts-ignore - Type instantiation is excessively deep and possibly infinite. ts(2589)
    await constraint.apply(state, update);
    expect(update.assert.mock.calls).toEqual([]);
  });

  test('applies to a multi-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
      }])
    });
    await constraint.apply(state, update);
    expect(update.assert).toBeCalledWith({
      '@delete': { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Flintstone'] }
    });
  });

  test('applies to a single-valued additive property update', async () => {
    await data.transact({
      prepare: async () => ({
        patch: await graph.write({
          '@insert': { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }
        })
      })
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
      }])
    });
    await constraint.apply(state, update);
    expect(update.assert).toBeCalledWith({
      '@delete': { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Flintstone'] }
    });
  });

  test('applies selectively to existing data', async () => {
    // Test case arose from compliance tests
    await data.transact({
      prepare: async () => ({
        patch: await graph.write({
          '@insert': [
            { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' },
            { '@id': 'http://test.m-ld.org/wilma', 'http://test.m-ld.org/#name': 'Wilma' }
          ]
        })
      })
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
      }])
    });
    await constraint.apply(state, update);
    // FIXME: not applied to the dataset!

    await expect(firstValueFrom(graph.describe1('http://test.m-ld.org/fred')))
      .resolves.toMatchObject({ 'http://test.m-ld.org/#name': 'Fred' });
    await expect(firstValueFrom(graph.describe1('http://test.m-ld.org/wilma')))
      .resolves.toMatchObject({ 'http://test.m-ld.org/#name': 'Wilma' });
  });
});

function mockInterim(update: MeldUpdate) {
  // Passing an implementation into the mock adds unwanted properties
  return Object.assign(mock<InterimUpdate>(), { update: Promise.resolve(update) });
}
