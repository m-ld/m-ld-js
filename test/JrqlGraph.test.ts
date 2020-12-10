import { JrqlGraph } from '../src/engine/dataset/JrqlGraph';
import { Graph } from '../src/engine/dataset';
import { mock } from 'jest-mock-extended';
import dataFactory = require('@rdfjs/data-model');

describe('json-rql Graph handler', () => {
  let jrqlGraph: JrqlGraph;

  beforeEach(() => jrqlGraph = new JrqlGraph(
    mock<Graph>({ name: dataFactory.defaultGraph(), dataFactory }),
    { '@base': 'http://test.m-ld.org/', '@vocab': '#' }));

  test('quadifies @id-only top-level subject with variable p-o', async () => {
    const quads = await jrqlGraph.quads({ '@id': 'fred' }, { query: true });
    expect(quads.length).toBe(1);
    expect(dataFactory.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('Variable');
  });

  test('quadifies anonymous subject', async () => {
    const quads = await jrqlGraph.quads({ 'name': 'Fred' }, { query: true });
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(dataFactory.namedNode('http://test.m-ld.org/#name').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Fred');
  });

  test('quadifies anonymous variable predicate', async () => {
    const quads = await jrqlGraph.quads({ '?': 'Fred' }, { query: true });
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Fred');
  });

  test('quadifies anonymous reference predicate', async () => {
    const quads = await jrqlGraph.quads({ '?': { '@id': 'fred' } }, { query: true });
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('NamedNode');
    expect(quads[0].object.value).toBe('http://test.m-ld.org/fred');
  });

  test('quadifies with numeric property', async () => {
    const quads = await jrqlGraph.quads({ '@id': 'fred', age: 40 }, { query: true });
    expect(quads.length).toBe(1);
    expect(dataFactory.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(dataFactory.namedNode('http://test.m-ld.org/#age').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('40');
  });

  test('quadifies with numeric array property', async () => {
    const quads = await jrqlGraph.quads({ '@id': 'fred', age: [40] }, { query: true });
    expect(quads.length).toBe(1);
    expect(dataFactory.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(dataFactory.namedNode('http://test.m-ld.org/#age').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('40');
  });
});