import { PatchQuads } from '../src/engine/dataset';
import { quad, defaultGraph, namedNode } from '@rdfjs/data-model';

describe('Patch quads', () => {
  const quads = [
    quad(
      namedNode('ns:subject1'),
      namedNode('ns:predicate1'),
      namedNode('ns:object1'),
      defaultGraph()),
    quad(
      namedNode('ns:subject2'),
      namedNode('ns:predicate1'),
      namedNode('ns:object1'),
      namedNode('ns:graph1')),
    quad(
      namedNode('ns:subject1'),
      namedNode('ns:predicate1'),
      namedNode('ns:object2'),
      namedNode('ns:graph1'))
  ];

  test('Construct empty', () => {
    const patch = new PatchQuads();
    expect(patch.oldQuads).toEqual([]);
    expect(patch.newQuads).toEqual([]);
    expect(patch.isEmpty).toBe(true);
  });

  test('Construct from quads', () => {
    const patch = new PatchQuads({ oldQuads: [quads[0]], newQuads: [quads[1]] });
    expect(patch.oldQuads.length).toBe(1);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.oldQuads[0].equals(quads[0])).toBe(true);
    expect(patch.newQuads[0].equals(quads[1])).toBe(true);
    expect(patch.isEmpty).toBe(false);
  });

  test('Constructor ensures minimal', () => {
    const patch = new PatchQuads({ oldQuads: [quads[0]], newQuads: [quads[0]] });
    expect(patch.oldQuads).toEqual([]);
    expect(patch.newQuads).toEqual([]);
    expect(patch.isEmpty).toBe(true);
  });

  test('Append mutates', () => {
    const patch = new PatchQuads();
    patch.append({ oldQuads: [quads[0]], newQuads: [quads[1]] });
    expect(patch.oldQuads.length).toBe(1);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.oldQuads[0].equals(quads[0])).toBe(true);
    expect(patch.newQuads[0].equals(quads[1])).toBe(true);
    expect(patch.isEmpty).toBe(false);
  });

  test('Append ensures minimal', () => {
    const patch = new PatchQuads();
    patch.append({ oldQuads: [quads[0]], newQuads: [quads[0]] });
    expect(patch.oldQuads).toEqual([]);
    expect(patch.newQuads).toEqual([]);
    expect(patch.isEmpty).toBe(true);
  });

  test('Append transitively ensures minimal', () => {
    const patch = new PatchQuads();
    patch.append({ oldQuads: [quads[0]], newQuads: [quads[1]] });
    patch.append({ oldQuads: [quads[1]], newQuads: [quads[2]] });
    expect(patch.oldQuads.length).toBe(1);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.oldQuads[0].equals(quads[0])).toBe(true);
    expect(patch.newQuads[0].equals(quads[2])).toBe(true);
    expect(patch.isEmpty).toBe(false);
  });

  test('Remove mutates', () => {
    const patch = new PatchQuads({ oldQuads: [quads[0]], newQuads: [quads[1]] });
    const removals = patch.remove('oldQuads', [quads[0]]);
    expect(removals.length).toBe(1);
    expect(removals[0].equals(quads[0])).toBe(true);
    expect(patch.oldQuads).toEqual([]);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.isEmpty).toBe(false);
  });

  test('Remove by filter', () => {
    const patch = new PatchQuads({ oldQuads: [quads[0]], newQuads: [quads[1]] });
    const removals = patch.remove('oldQuads', quad => quad.equals(quads[0]));
    expect(removals.length).toBe(1);
    expect(removals[0].equals(quads[0])).toBe(true);
    expect(patch.oldQuads).toEqual([]);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.isEmpty).toBe(false);
  });
});