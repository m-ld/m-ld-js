import { MeldReadState } from '../src/api';
import { memStore } from './testClones';
import { SingleValued } from '../src/constraints/SingleValued';
import { JrqlGraph } from '../src/engine/dataset/JrqlGraph';
import { graphState } from '../src/engine/dataset/SuSetDataset';
import { Dataset } from '../src/engine/dataset';

describe('Single-valued constraint', () => {
  let data: Dataset;
  let graph: JrqlGraph;
  let state: MeldReadState;

  beforeEach(async () => {
    data = await memStore();
    graph = new JrqlGraph(data.graph());
    state = graphState(graph);
  });

  test('Passes an empty update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': []
    })).resolves.toBeUndefined();
  });

  test('Passes a missing property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#height': 5 }]
    })).resolves.toBeUndefined();
  });

  test('Passes a single-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
    })).resolves.toBeUndefined();
  });

  test('Fails a multi-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone'] }]
    })).rejects.toBeDefined();
  });

  test('Fails a single-valued additive property update', async () => {
    await data.transact({
      prepare: async () => ({
        patch: await graph.insert({
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        })
      })
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone' }]
    })).rejects.toBeDefined();
  });

  test('does not apply to a single-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.apply(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
    })).resolves.toBeNull();
  });

  test('applies to a multi-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.apply(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone'] }]
    })).resolves.toEqual({
      '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Flintstone'] }],
      '@insert': []
    });
  });

  test('applies to a single-valued additive property update', async () => {
    await data.transact({
      prepare: async () => ({
        patch: await graph.insert({
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        })
      })
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.apply(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone' }]
    })).resolves.toEqual({
      '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Flintstone'] }],
      '@insert': []
    });
  });

  test('applies selectively to existing data', async () => {
    // Test case arose from compliance tests
    await data.transact({
      prepare: async () => ({
        patch: await graph.insert([
          { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' },
          { '@id': 'http://test.m-ld.org/wilma', 'http://test.m-ld.org/#name': 'Wilma' }
        ])
      })
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await constraint.apply(state, {
      '@ticks': 0,
      '@delete': [],
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone' }]
    });

    await expect(graph.describe1('http://test.m-ld.org/fred'))
      .resolves.toMatchObject({ 'http://test.m-ld.org/#name': 'Fred' });
    await expect(graph.describe1('http://test.m-ld.org/wilma'))
      .resolves.toMatchObject({ 'http://test.m-ld.org/#name': 'Wilma' });
  });
});