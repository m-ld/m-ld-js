import { asSubjectUpdates, Reference, Resource, updateSubject } from '../src';

describe('MeldClone utilities', () => {
  test('converts simple group update to subject updates', () => {
    expect(asSubjectUpdates({
      '@delete': [{ '@id': 'foo', size: 10 }],
      '@insert': [{ '@id': 'foo', size: 20 }]
    })).toEqual({
      'foo': {
        '@delete': { '@id': 'foo', size: 10 },
        '@insert': { '@id': 'foo', size: 20 }
      }
    });
  });

  test('converts array group update to subject updates', () => {
    expect(asSubjectUpdates({
      '@delete': [{ '@id': 'foo', size: 10 }, { '@id': 'bar', size: 30 }],
      '@insert': [{ '@id': 'foo', size: 20 }, { '@id': 'bar', size: 40 }]
    })).toEqual({
      'foo': {
        '@delete': { '@id': 'foo', size: 10 },
        '@insert': { '@id': 'foo', size: 20 }
      },
      'bar': {
        '@delete': { '@id': 'bar', size: 30 },
        '@insert': { '@id': 'bar', size: 40 }
      }
    });
  });

  interface Box {
    size: number;
    label?: string;
    contents?: Reference[];
  }

  test('does not update mismatching ids', () => {
    const box: Resource<Box> = { '@id': 'bar', size: 10, label: 'My box' };
    updateSubject(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': {} });
    expect(box).toEqual({ '@id': 'bar', size: 10, label: 'My box' });
  });

  test('adds a missing value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10 };
    updateSubject(box, { '@insert': { '@id': 'foo', label: 'My box' }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, label: 'My box' });
  });

  test('adds an array value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10 };
    updateSubject(box, { '@insert': { '@id': 'foo', size: [20, 30] }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: [10, 20, 30] });
  });

  test('does not add an empty array value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10 };
    updateSubject(box, { '@insert': { '@id': 'foo', size: [] }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10 });
  });

  test('adds an inserted value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    updateSubject(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: [10, 20], label: 'My box' });
  });

  test('does not insert a duplicate value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    updateSubject(box, { '@insert': { '@id': 'foo', size: 10 }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, label: 'My box' });
  });

  test('removes a deleted value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    updateSubject(box, { '@delete': { '@id': 'foo', size: 10 }, '@insert': {} });
    expect(box).toEqual({ '@id': 'foo', label: 'My box' });
  });

  test('updates a value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    updateSubject(box, { '@insert': { '@id': 'foo', size: 20 }, '@delete': { '@id': 'foo', size: 10 } });
    expect(box).toEqual({ '@id': 'foo', size: 20, label: 'My box' });
  });

  test('updates unchanged value', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, label: 'My box' };
    updateSubject(box, { '@insert': { '@id': 'foo', size: 10 }, '@delete': { '@id': 'foo', size: 10 } });
    expect(box).toEqual({ '@id': 'foo', size: 10, label: 'My box' });
  });

  // FIXME This breaks the Node type, but not possible to prevent at runtime
  test('adds a singleton reference as a singleton if array property undefined', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10 };
    updateSubject(box, { '@insert': { '@id': 'foo', contents: { '@id': 'bar' } }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: { '@id': 'bar' } });
  });

  test('adds a singleton reference into array if array property defined', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, contents: [] };
    updateSubject(box, { '@insert': { '@id': 'foo', contents: { '@id': 'bar' } }, '@delete': {} });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: [{ '@id': 'bar' }] });
  });

  test('updates a reference', () => {
    const box: Resource<Box> = { '@id': 'foo', size: 10, contents: [{ '@id': 'bar' }] };
    updateSubject(box, {
      '@insert': { '@id': 'foo', contents: { '@id': 'baz' } },
      '@delete': { '@id': 'foo', contents: { '@id': 'bar' } }
    });
    expect(box).toEqual({ '@id': 'foo', size: 10, contents: [{ '@id': 'baz' }] });
  });
});
