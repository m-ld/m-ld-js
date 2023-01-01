import { DataFactory as RdfDataFactory } from 'rdf-data-factory';
import { quadIndexKey, QuadSet, tripleIndexKey, TripleMap } from '../src/engine/quads';

describe('quads utilities', () => {
  const rdf = new RdfDataFactory();

  const makeFredName = () => rdf.quad(
    rdf.namedNode('http://ex.org/fred'),
    rdf.namedNode('http://ex.org/#name'),
    rdf.literal('Fred'));

  test('triple index key', () => {
    expect(tripleIndexKey(makeFredName())).toBe(
      '"http://ex.org/fred",' +
      '"http://ex.org/#name",' +
      '"Literal","Fred","http://www.w3.org/2001/XMLSchema#string"');
  });

  test('triple index lbound', () => {
    expect(tripleIndexKey(rdf.quad(
      rdf.namedNode('http://ex.org/fred'),
      rdf.namedNode('http://ex.org/#name'),
      rdf.variable('any')))).toBe(
      '"http://ex.org/fred",' +
      '"http://ex.org/#name"');
  });

  test('quad index key', () => {
    expect(quadIndexKey(makeFredName())).toBe(
      '"","http://ex.org/fred",' +
      '"http://ex.org/#name",' +
      '"Literal","Fred","http://www.w3.org/2001/XMLSchema#string"');
  });

  test('can add a quad to a triple index and a quad index', () => {
    const fredName = makeFredName();
    const triples = new TripleMap<string>([[fredName, 'a']]);
    const quads = new QuadSet([fredName]);
    expect(triples.get(makeFredName())).toBe('a');
    expect(quads.has(makeFredName())).toBe(true);
  });
});