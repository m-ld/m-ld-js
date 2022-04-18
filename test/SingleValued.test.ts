import { MockGraphState, mockInterim } from './testClones';
import { SingleValued } from '../src/constraints/SingleValued';
import { SubjectGraph } from '../src/engine/SubjectGraph';

describe('Single-valued constraint', () => {
  let state: MockGraphState;

  beforeEach(async () => {
    state = await MockGraphState.create();
  });

  afterEach(() => state.close());

  test('Passes an empty update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.graph.asReadState, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([])
    }))).resolves.toBeUndefined();
  });

  test('Passes a missing property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.graph.asReadState, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#height': 5
      }])
    }))).resolves.toBeUndefined();
  });

  test('Passes a single-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.graph.asReadState, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
      }])
    }))).resolves.toBeUndefined();
  });

  test('Fails a multi-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.graph.asReadState, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
      }])
    }))).rejects.toBeDefined();
  });

  test('Fails a single-valued additive property update', async () => {
    await state.write({
      '@insert': { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.graph.asReadState, mockInterim({
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
    await constraint.apply(state.graph.asReadState, update);
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
    await constraint.apply(state.graph.asReadState, update);
    expect(update.assert).toBeCalledWith({
      '@delete': {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': ['Flintstone']
      }
    });
  });

  test('applies to a single-valued additive property update', async () => {
    await state.write({
      '@insert': { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
      }])
    });
    await constraint.apply(state.graph.asReadState, update);
    expect(update.assert).toBeCalledWith({
      '@delete': {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#name': ['Flintstone']
      }
    });
  });

  test('applies selectively to existing data', async () => {
    // Test case arose from compliance tests
    await state.write({
      '@insert': [
        { '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' },
        { '@id': 'http://test.m-ld.org/wilma', 'http://test.m-ld.org/#name': 'Wilma' }
      ]
    });
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    const update = mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
      }])
    });
    await constraint.apply(state.graph.asReadState, update);
    // FIXME: not applied to the dataset!

    await expect(state.graph.get('http://test.m-ld.org/fred', state.ctx))
      .resolves.toMatchObject({ name: 'Fred' });
    await expect(state.graph.get('http://test.m-ld.org/wilma', state.ctx))
      .resolves.toMatchObject({ name: 'Wilma' });
  });
});

