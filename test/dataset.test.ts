import { PatchQuads } from '../src/engine/dataset';
import { DataFactory as RdfDataFactory } from 'rdf-data-factory';
const rdf = new RdfDataFactory();

describe('Patch quads', () => {
  const a = rdf.quad(
    rdf.namedNode('ns:subject1'),
    rdf.namedNode('ns:predicate1'),
    rdf.namedNode('ns:object1'),
    rdf.defaultGraph()),
    b = rdf.quad(
      rdf.namedNode('ns:subject2'),
      rdf.namedNode('ns:predicate1'),
      rdf.namedNode('ns:object1'),
      rdf.namedNode('ns:graph1')),
    c = rdf.quad(
      rdf.namedNode('ns:subject1'),
      rdf.namedNode('ns:predicate1'),
      rdf.namedNode('ns:object2'),
      rdf.namedNode('ns:graph1'));

  test('Construct empty', () => {
    const patch = new PatchQuads();
    expect([...patch.deletes]).toEqual([]);
    expect([...patch.inserts]).toEqual([]);
    expect(patch.isEmpty).toBe(true);
  });

  test('Construct from quads', () => {
    const patch = new PatchQuads({ deletes: [a], inserts: [b] });
    expect([...patch.deletes].length).toBe(1);
    expect([...patch.inserts].length).toBe(1);
    expect([...patch.deletes][0].equals(a)).toBe(true);
    expect([...patch.inserts][0].equals(b)).toBe(true);
    expect(patch.isEmpty).toBe(false);
  });

  test('Constructor allows for graph states', () => {
    const patch = new PatchQuads({ deletes: [a], inserts: [a] });
    expect(patch.isEmpty).toBe(false);
  });

  test('Append mutates', () => {
    const patch = new PatchQuads();
    patch.append({ deletes: [a], inserts: [b] });
    expect([...patch.deletes].length).toBe(1);
    expect([...patch.inserts].length).toBe(1);
    expect([...patch.deletes][0].equals(a)).toBe(true);
    expect([...patch.inserts][0].equals(b)).toBe(true);
    expect(patch.isEmpty).toBe(false);
  });

  test('Construction removes delete then insert redundancy', () => {
    const patch = new PatchQuads(
      { deletes: [a/* squash */], inserts: [a] });
    expect(patch.isEmpty).toBe(false);
    expect([...patch.deletes].length).toBe(0);
    expect([...patch.inserts].length).toBe(1);
  });

  test('Append to nothing removes delete then insert redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ deletes: [a/* squash */], inserts: [a] });
    expect(patch.isEmpty).toBe(false);
    expect([...patch.deletes].length).toBe(0);
    expect([...patch.inserts].length).toBe(1);
  });

  test('Append removes transitive insert then delete redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ deletes: [a], inserts: [b/* squash */] });
    patch.append({ deletes: [b], inserts: [c] });
    expect(patch.isEmpty).toBe(false);
    expect([...patch.deletes].length).toBe(2);
    expect([...patch.inserts].length).toBe(1);
    expect([...patch.deletes][0].equals(a) || [...patch.deletes][0].equals(b)).toBe(true);
    expect([...patch.deletes][1].equals(a) || [...patch.deletes][1].equals(b)).toBe(true);
    expect([...patch.inserts][0].equals(c)).toBe(true);
  });

  test('Append removes transitive delete then insert redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ deletes: [a/* squash */], inserts: [b/* squash */] });
    patch.append({ deletes: [b], inserts: [a] });
    expect(patch.isEmpty).toBe(false);
    expect([...patch.deletes].length).toBe(1);
    expect([...patch.inserts].length).toBe(1);
    expect([...patch.deletes][0].equals(b)).toBe(true);
    expect([...patch.inserts][0].equals(a)).toBe(true);
  });

  test('Append removes transitive delete then insert redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ deletes: [a], inserts: [b/* squash */] });
    patch.append({ deletes: [b/* squash */], inserts: [b] });
    expect(patch.isEmpty).toBe(false);
    expect([...patch.deletes].length).toBe(1);
    expect([...patch.inserts].length).toBe(1);
    expect([...patch.deletes][0].equals(a)).toBe(true);
    expect([...patch.inserts][0].equals(b)).toBe(true);
  });

  test('Remove mutates', () => {
    const patch = new PatchQuads({ deletes: [a], inserts: [b] });
    const removals = patch.remove('deletes', [a]);
    expect(removals.length).toBe(1);
    expect(removals[0].equals(a)).toBe(true);
    expect([...patch.deletes]).toEqual([]);
    expect([...patch.inserts].length).toBe(1);
    expect(patch.isEmpty).toBe(false);
  });

  test('Remove by filter', () => {
    const patch = new PatchQuads({ deletes: [a], inserts: [b] });
    const removals = patch.remove('deletes', quad => quad.equals(a));
    expect(removals.length).toBe(1);
    expect(removals[0].equals(a)).toBe(true);
    expect([...patch.deletes]).toEqual([]);
    expect([...patch.inserts].length).toBe(1);
    expect(patch.isEmpty).toBe(false);
  });
});