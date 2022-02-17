import { InterimUpdate, MeldUpdate } from '../src';
import { MockGraphState } from './testClones';
import { SingleValued } from '../src/constraints/SingleValued';
import { mock } from 'jest-mock-extended';
import { SubjectGraph } from '../src/engine/SubjectGraph';

describe('Single-valued constraint', () => {
  let state: MockGraphState;

  beforeEach(async () => {
    state = await MockGraphState.create();
  });

  afterEach(() => state.close());

  test('Passes an empty update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.jrqlGraph.asReadState, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([])
    }))).resolves.toBeUndefined();
  });

  test('Passes a missing property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.jrqlGraph.asReadState, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#height': 5
      }])
    }))).resolves.toBeUndefined();
  });

  test('Passes a single-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.jrqlGraph.asReadState, mockInterim({
      '@ticks': 0,
      '@delete': new SubjectGraph([]),
      '@insert': new SubjectGraph([{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
      }])
    }))).resolves.toBeUndefined();
  });

  test('Fails a multi-valued property update', async () => {
    const constraint = new SingleValued('http://test.m-ld.org/#name');
    await expect(constraint.check(state.jrqlGraph.asReadState, mockInterim({
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
    await expect(constraint.check(state.jrqlGraph.asReadState, mockInterim({
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
    await constraint.apply(state.jrqlGraph.asReadState, update);
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
    await constraint.apply(state.jrqlGraph.asReadState, update);
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
    await constraint.apply(state.jrqlGraph.asReadState, update);
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
    await constraint.apply(state.jrqlGraph.asReadState, update);
    // FIXME: not applied to the dataset!

    await expect(state.jrqlGraph.get('http://test.m-ld.org/fred', state.ctx))
      .resolves.toMatchObject({ 'http://test.m-ld.org/#name': 'Fred' });
    await expect(state.jrqlGraph.get('http://test.m-ld.org/wilma', state.ctx))
      .resolves.toMatchObject({ 'http://test.m-ld.org/#name': 'Wilma' });
  });
});

function mockInterim(update: MeldUpdate) {
  // Passing an implementation into the mock adds unwanted properties
  return Object.assign(mock<InterimUpdate>(), { update: Promise.resolve(update) });
}
