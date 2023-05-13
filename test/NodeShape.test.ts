import { NodeShape, Shape } from '../src/shacl';
import { SH } from '../src/ns';
import { MockGraphState, mockUpdate } from './testClones';

describe('SHACL Node Shape', () => {
  test('create from a subject', () => {
    const shape = Shape.from({
      '@id': 'http://test.m-ld.org/flintstoneShape',
      [SH.targetClass]: { '@vocab': 'http://test.m-ld.org/#Flintstone' }
    });
    expect(shape).toBeInstanceOf(NodeShape);
    expect((<NodeShape>shape).targetClass)
      .toEqual(new Set(['http://test.m-ld.org/#Flintstone']));
  });

  test('create from target class', () => {
    const shape = new NodeShape({ targetClass: 'http://test.m-ld.org/#Flintstone' });
    expect((<NodeShape>shape).targetClass)
      .toEqual(new Set(['http://test.m-ld.org/#Flintstone']));
  });

  test('declare a node shape', () => {
    const write = NodeShape.declare({
      targetClass: 'http://test.m-ld.org/#Flintstone'
    });
    expect(write).toMatchObject({
      [SH.targetClass]: [{ '@vocab': 'http://test.m-ld.org/#Flintstone' }]
    });
  });

  describe('affected', () => {
    let state: MockGraphState;

    beforeEach(async () => {
      state = await MockGraphState.create();
    });

    afterEach(() => state.close());

    test('insert with no matching type', async () => {
      const shape = new NodeShape({ targetClass: 'http://test.m-ld.org/#Flintstone' });
      await expect(shape.affected(state.graph.asReadState, mockUpdate({
        '@insert': [{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }]
      }))).resolves.toEqual({
        '@delete': [], '@insert': [], '@update': []
      });
    });

    test('insert with matching type in update', async () => {
      const shape = new NodeShape({ targetClass: 'http://test.m-ld.org/#Flintstone' });
      await expect(shape.affected(state.graph.asReadState, mockUpdate({
        '@insert': [{
          '@id': 'http://test.m-ld.org/fred',
          '@type': 'http://test.m-ld.org/#Flintstone',
          'http://test.m-ld.org/#name': 'Fred'
        }]
      }))).resolves.toEqual({
        '@delete': [],
        '@insert': [{
          '@id': 'http://test.m-ld.org/fred',
          '@type': 'http://test.m-ld.org/#Flintstone',
          'http://test.m-ld.org/#name': 'Fred'
        }],
        '@update': []
      });
    });

    test('insert with matching type in state', async () => {
      await state.write({ '@id': 'fred', '@type': 'http://test.m-ld.org/#Flintstone' });
      const shape = new NodeShape({ targetClass: 'http://test.m-ld.org/#Flintstone' });
      await expect(shape.affected(state.graph.asReadState, mockUpdate({
        '@insert': [{
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#name': 'Fred'
        }]
      }))).resolves.toEqual({
        '@delete': [],
        '@insert': [{
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#name': 'Fred'
        }],
        '@update': []
      });
    });

    test('delete with matching type in state', async () => {
      await state.write({ '@id': 'fred', '@type': 'http://test.m-ld.org/#Flintstone' });
      const shape = new NodeShape({ targetClass: 'http://test.m-ld.org/#Flintstone' });
      await expect(shape.affected(state.graph.asReadState, mockUpdate({
        '@delete': [{
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#name': 'Fred'
        }]
      }))).resolves.toEqual({
        '@delete': [{
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#name': 'Fred'
        }],
        '@insert': [],
        '@update': []
      });
    });

  });
});