import { PatchQuads, QuadStoreDataset } from '../src/engine/dataset';
import { DataFactory as RdfDataFactory } from 'rdf-data-factory';
import { Quad } from 'rdf-js';
import { MemoryLevel } from 'memory-level';

const rdf = new RdfDataFactory();

describe('Patch quads', () => {
  const a = rdf.quad(
      rdf.namedNode('http://ex.org/subject1'),
      rdf.namedNode('http://ex.org/predicate1'),
      rdf.namedNode('http://ex.org/object1'),
      rdf.defaultGraph()),
    b = rdf.quad(
      rdf.namedNode('http://ex.org/subject2'),
      rdf.namedNode('http://ex.org/predicate1'),
      rdf.namedNode('http://ex.org/object1'),
      rdf.namedNode('http://ex.org/graph1')),
    c = rdf.quad(
      rdf.namedNode('http://ex.org/subject1'),
      rdf.namedNode('http://ex.org/predicate1'),
      rdf.namedNode('http://ex.org/object2'),
      rdf.namedNode('http://ex.org/graph1'));

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

  test('Append with iterable mutates', () => {
    const patch = new PatchQuads();
    function *deletes(quads: Quad[]) { for (let q of quads) yield q; }
    function *inserts(quads: Quad[]) { for (let q of quads) yield q; }
    patch.append({ deletes: deletes([a]), inserts: inserts([b]) });
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

  test('Append removes insert then delete redundancy', () => {
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

  test('Include removes delete and insert redundancy', () => {
    const patch = new PatchQuads();
    patch.include({ deletes: [a], inserts: [b] });
    patch.include({ deletes: [b], inserts: [c] });
    expect(patch.isEmpty).toBe(false);
    expect([...patch.deletes].length).toBe(1);
    expect([...patch.inserts].length).toBe(2);
    expect([...patch.deletes][0].equals(a)).toBe(true);
    expect([...patch.inserts][0].equals(b) || [...patch.inserts][0].equals(c)).toBe(true);
    expect([...patch.inserts][1].equals(b) || [...patch.inserts][1].equals(c)).toBe(true);
  });

  test('Append removes delete then insert redundancy', () => {
    const patch = new PatchQuads();
    patch.append({ deletes: [a/* squash */], inserts: [b] });
    patch.append({ deletes: [c], inserts: [a] });
    expect(patch.isEmpty).toBe(false);
    expect([...patch.deletes].length).toBe(1);
    expect([...patch.inserts].length).toBe(2);
    expect([...patch.deletes][0].equals(c)).toBe(true);
    expect([...patch.inserts][0].equals(a) || [...patch.inserts][0].equals(b)).toBe(true);
    expect([...patch.inserts][1].equals(a) || [...patch.inserts][1].equals(b)).toBe(true);
  });

  test('Append removes both delete and insert redundancy', () => {
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

// Note that the quadstore dataset is heavily tested by e.g.
// SuSetDataset and MeldClone tests
describe('Quadstore Dataset', () => {
  test('generate skolem from base', async () => {
    const ds = await new QuadStoreDataset('ex.org', new MemoryLevel()).initialise();
    expect(ds.graph().rdf.skolem!().value).toMatch(
      /http:\/\/ex\.org\/\.well-known\/genid\/\w+/);
  });
});