import { PatchQuads } from '../src/engine/dataset';
import { quad, defaultGraph, namedNode } from '@rdfjs/data-model';

describe('Patch quads', () => {
  const a = quad(
      namedNode('ns:subject1'),
      namedNode('ns:predicate1'),
      namedNode('ns:object1'),
      defaultGraph()),
    b = quad(
      namedNode('ns:subject2'),
      namedNode('ns:predicate1'),
      namedNode('ns:object1'),
      namedNode('ns:graph1')),
    c = quad(
      namedNode('ns:subject1'),
      namedNode('ns:predicate1'),
      namedNode('ns:object2'),
      namedNode('ns:graph1'));

  test('Construct empty', () => {
    const patch = new PatchQuads();
    expect(patch.oldQuads).toEqual([]);
    expect(patch.newQuads).toEqual([]);
    expect(patch.isEmpty).toBe(true);
  });

  test('Construct from quads', () => {
    const patch = new PatchQuads({ oldQuads: [a], newQuads: [b] });
    expect(patch.oldQuads.length).toBe(1);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.oldQuads[0].equals(a)).toBe(true);
    expect(patch.newQuads[0].equals(b)).toBe(true);
    expect(patch.isEmpty).toBe(false);
  });

  test('Constructor allows for graph states', () => {
    const patch = new PatchQuads({ oldQuads: [a], newQuads: [a] });
    expect(patch.isEmpty).toBe(false);
  });

  test('Append mutates', () => {
    const patch = new PatchQuads();
    patch.append({ oldQuads: [a], newQuads: [b] });
    expect(patch.oldQuads.length).toBe(1);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.oldQuads[0].equals(a)).toBe(true);
    expect(patch.newQuads[0].equals(b)).toBe(true);
    expect(patch.isEmpty).toBe(false);
  });

  test('Construction removes delete then insert redundancy', () => {
    const patch = new PatchQuads(
      { oldQuads: [a/* squash */], newQuads: [a] });
    expect(patch.isEmpty).toBe(false);
    expect(patch.oldQuads.length).toBe(0);
    expect(patch.newQuads.length).toBe(1);
  });

  test('Append to nothing removes delete then insert redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ oldQuads: [a/* squash */], newQuads: [a] });
    expect(patch.isEmpty).toBe(false);
    expect(patch.oldQuads.length).toBe(0);
    expect(patch.newQuads.length).toBe(1);
  });

  test('Append removes transitive insert then delete redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ oldQuads: [a], newQuads: [b/* squash */] });
    patch.append({ oldQuads: [b], newQuads: [c] });
    expect(patch.isEmpty).toBe(false);
    expect(patch.oldQuads.length).toBe(2);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.oldQuads[0].equals(a) || patch.oldQuads[0].equals(b)).toBe(true);
    expect(patch.oldQuads[1].equals(a) || patch.oldQuads[1].equals(b)).toBe(true);
    expect(patch.newQuads[0].equals(c)).toBe(true);
  });

  test('Append removes transitive delete then insert redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ oldQuads: [a/* squash */], newQuads: [b/* squash */] });
    patch.append({ oldQuads: [b], newQuads: [a] });
    expect(patch.isEmpty).toBe(false);
    expect(patch.oldQuads.length).toBe(1);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.oldQuads[0].equals(b)).toBe(true);
    expect(patch.newQuads[0].equals(a)).toBe(true);
  });

  test('Append removes transitive delete then insert redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ oldQuads: [a], newQuads: [b/* squash */] });
    patch.append({ oldQuads: [b/* squash */], newQuads: [b] });
    expect(patch.isEmpty).toBe(false);
    expect(patch.oldQuads.length).toBe(1);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.oldQuads[0].equals(a)).toBe(true);
    expect(patch.newQuads[0].equals(b)).toBe(true);
  });

  test('Remove mutates', () => {
    const patch = new PatchQuads({ oldQuads: [a], newQuads: [b] });
    const removals = patch.remove('oldQuads', [a]);
    expect(removals.length).toBe(1);
    expect(removals[0].equals(a)).toBe(true);
    expect(patch.oldQuads).toEqual([]);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.isEmpty).toBe(false);
  });

  test('Remove by filter', () => {
    const patch = new PatchQuads({ oldQuads: [a], newQuads: [b] });
    const removals = patch.remove('oldQuads', quad => quad.equals(a));
    expect(removals.length).toBe(1);
    expect(removals[0].equals(a)).toBe(true);
    expect(patch.oldQuads).toEqual([]);
    expect(patch.newQuads.length).toBe(1);
    expect(patch.isEmpty).toBe(false);
  });
});