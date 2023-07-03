import { SH } from '../src/ns';
import { Shape } from '../src/shacl/Shape';
import { PropertyShape } from '../src/shacl/index';
import { MockGraphState, mockInterim } from './testClones';
import { consume } from 'rx-flowable/consume';

describe('SHACL Property Shape', () => {
  test('create from a subject', () => {
    const shape = Shape.from({
      '@id': 'http://test.m-ld.org/nameShape',
      [SH.path]: { '@vocab': 'http://test.m-ld.org/#name' }
    });
    expect(shape).toBeInstanceOf(PropertyShape);
    expect((<PropertyShape>shape).path).toBe('http://test.m-ld.org/#name');
  });

  test('create from just a path', () => {
    const shape = new PropertyShape({
      src: { '@id': 'http://test.m-ld.org/nameShape' },
      path: 'http://test.m-ld.org/#name'
    });
    expect((<PropertyShape>shape).path).toBe('http://test.m-ld.org/#name');
  });

  test('update the path', async () => {
    const shape = await Shape.from({
      '@id': 'http://test.m-ld.org/nameShape',
      [SH.path]: { '@vocab': 'http://test.m-ld.org/#name' }
    });
    // This tests the ability to respond to m-ld updates
    shape.src[SH.path] = { '@vocab': 'http://test.m-ld.org/#height' };
    expect((<PropertyShape>shape).path).toBe('http://test.m-ld.org/#height');
  });

  test('declare a property shape', () => {
    const write = PropertyShape.declare({
      src: { '@id': 'http://test.m-ld.org/nameShape' },
      path: 'http://test.m-ld.org/#name'
    });
    expect(write).toMatchObject({
      '@id': 'http://test.m-ld.org/nameShape',
      [SH.path]: { '@vocab': 'http://test.m-ld.org/#name' }
    });
  });

  test('constructs with count', () => {
    const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
    expect(shape.path).toBe('http://test.m-ld.org/#name');
    expect(shape.minCount).toBe(1);
    expect(shape.maxCount).toBe(1);
  });

  describe('constraints', () => {
    let state: MockGraphState;

    beforeEach(async () => {
      state = await MockGraphState.create();
    });

    afterEach(() => state.close());

    describe('check', () => {
      test('OK update', async () => {
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        await expect(shape.check(state.graph.asReadState, mockInterim({
          '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
        }))).resolves.not.toThrow();
      });

      test('too many inserted in update', async () => {
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        const badFred = {
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
        };
        await expect(shape.check(state.graph.asReadState, mockInterim({
          '@insert': [badFred]
        }))).resolves.toMatchObject([{
          focusNode: badFred,
          resultMessage: expect.stringContaining('Too many values'),
          sourceConstraintComponent: 'http://www.w3.org/ns/shacl#MaxCountConstraintComponent'
        }]);
      });

      test('OK after update', async () => {
        await state.write({ '@id': 'fred', name: 'Fred' });
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        await expect(shape.check(state.graph.asReadState, mockInterim({
          '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }],
          '@insert': [{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
          }]
        }))).resolves.not.toThrow();
      });

      test('too many after update', async () => {
        await state.write({ '@id': 'fred', name: 'Fred' });
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        await expect(shape.check(state.graph.asReadState, mockInterim({
          '@insert': [{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
          }]
        }))).resolves.toMatchObject([{
          focusNode: {
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
          },
          resultMessage: expect.stringContaining('Too many values'),
          sourceConstraintComponent: 'http://www.w3.org/ns/shacl#MaxCountConstraintComponent'
        }]);
      });

      test('too few after update', async () => {
        await state.write({ '@id': 'fred', name: 'Fred' });
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        await expect(shape.check(state.graph.asReadState, mockInterim({
          '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
        }))).resolves.toMatchObject([{
          focusNode: { '@id': 'http://test.m-ld.org/fred' },
          resultMessage: expect.stringContaining('Too few values'),
          sourceConstraintComponent: 'http://www.w3.org/ns/shacl#MinCountConstraintComponent'
        }]);
      });

      test('minCount of zero after update', async () => {
        await state.write({ '@id': 'fred', name: 'Fred' });
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', maxCount: 1 });
        await expect(shape.check(state.graph.asReadState, mockInterim({
          '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
        }))).resolves.not.toThrow();
      });
    });

    describe('apply', () => {
      test('OK update', async () => {
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        await expect(shape.apply(state.graph.asReadState, mockInterim({
          '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
        }))).resolves.not.toThrow();
      });

      test('too many inserted in update', async () => {
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        const interim = mockInterim({
          '@insert': [{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
          }]
        });
        await shape.apply(state.graph.asReadState, interim);
        expect(interim.entail).toBeCalledWith(expect.objectContaining({
          '@delete': {
            '@id': 'http://test.m-ld.org/fred',
            'http://test.m-ld.org/#name': 'Flintstone'
          }
        }));
      });

      test('too many after update', async () => {
        await state.write({ '@id': 'fred', name: 'Fred' });
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        const interim = mockInterim({
          '@insert': [{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
          }]
        });
        await shape.apply(state.graph.asReadState, interim);
        expect(interim.entail).toBeCalledWith(expect.objectContaining({
          '@delete': {
            '@id': 'http://test.m-ld.org/fred',
            'http://test.m-ld.org/#name': 'Flintstone'
          }
        }));
      });

      test('too few after update', async () => {
        await state.write({ '@id': 'fred', name: 'Fred' });
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        const interim = mockInterim({
          '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
        });
        await shape.apply(state.graph.asReadState, interim);
        expect(interim.assert).toBeCalledWith(expect.objectContaining({
          '@insert': {
            '@id': 'http://test.m-ld.org/fred',
            'http://test.m-ld.org/#name': 'Fred'
          }
        }));
      });

      test('entailed hidden are reinstated', async () => {
        await state.write({ '@id': 'fred', name: 'Fred' });
        const shape = new PropertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
        const interim = mockInterim({
          '@delete': [{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
          }]
        });
        interim.hidden.mockReturnValue(consume(['Flintstone']));
        await shape.apply(state.graph.asReadState, interim);
        expect(interim.entail).toBeCalledWith(expect.objectContaining({
          '@insert': {
            '@id': 'http://test.m-ld.org/fred',
            'http://test.m-ld.org/#name': 'Flintstone'
          }
        }));
      });
    });
  });
});