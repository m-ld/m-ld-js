import { SH } from '../src/ns';
import { PropertyShape, Shape } from '../src/shacl/index';

describe('SHACL support', () => {
  test('create a property shape from a subject', () => {
    const shape = Shape.from({
      '@id': 'http://test.m-ld.org/nameShape',
      [SH.path]: { '@vocab': 'http://test.m-ld.org/#name' }
    });
    expect(shape).toBeInstanceOf(PropertyShape);
    expect((<PropertyShape>shape).path).toEqual({ '@vocab': 'http://test.m-ld.org/#name' });
  });

  test('create a property shape from just a path', () => {
    const shape = new PropertyShape({ '@id': 'http://test.m-ld.org/nameShape' }, {
      path: { '@vocab': 'http://test.m-ld.org/#name' }
    });
    expect((<PropertyShape>shape).path).toEqual({ '@vocab': 'http://test.m-ld.org/#name' });
  });

  test('update the path of a property shape', () => {
    const shape = Shape.from({
      '@id': 'http://test.m-ld.org/nameShape',
      [SH.path]: { '@vocab': 'http://test.m-ld.org/#name' }
    });
    // This tests the ability to respond to m-ld updates
    shape.src[SH.path] = { '@vocab': 'http://test.m-ld.org/#height' };
    expect((<PropertyShape>shape).path).toEqual({ '@vocab': 'http://test.m-ld.org/#height' });
  });

  test('declare a property shape', () => {
    const write = PropertyShape.declare({
      shapeId: 'http://test.m-ld.org/nameShape',
      path: { '@vocab': 'http://test.m-ld.org/#name' }
    });
    expect(write).toMatchObject({
      '@id': 'http://test.m-ld.org/nameShape',
      [SH.path]: { '@vocab': 'http://test.m-ld.org/#name' }
    });
  });
});