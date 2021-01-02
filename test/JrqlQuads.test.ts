import rdf = require('@rdfjs/data-model');
import { JrqlQuads, toIndexNumber } from '../src/engine/dataset/JrqlQuads';
import * as N3 from 'n3';
import { genIdRegex } from './testUtil';

describe('json-rql Quads translation', () => {
  let jrql: JrqlQuads;
  const context = { '@base': 'http://test.m-ld.org/', '@vocab': '#' };

  beforeEach(() => jrql = new JrqlQuads(rdf, rdf.defaultGraph(), context['@base']));

  test('quadifies @id-only top-level subject with variable p-o', async () => {
    const quads = await jrql.quads({ '@id': 'fred' }, { query: true }, context);
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('Variable');
  });

  test('quadifies anonymous subject', async () => {
    const quads = await jrql.quads({ 'name': 'Fred' }, { query: true }, context);
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(rdf.namedNode('http://test.m-ld.org/#name').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Fred');
  });

  test('quadifies anonymous variable predicate', async () => {
    const quads = await jrql.quads({ '?': 'Fred' }, { query: true }, context);
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Fred');
  });

  test('quadifies anonymous reference predicate', async () => {
    const quads = await jrql.quads({ '?': { '@id': 'fred' } }, { query: true }, context);
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('NamedNode');
    expect(quads[0].object.value).toBe('http://test.m-ld.org/fred');
  });

  test('quadifies with numeric property', async () => {
    const quads = await jrql.quads({ '@id': 'fred', age: 40 }, { query: true }, context);
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(rdf.namedNode('http://test.m-ld.org/#age').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('40');
  });

  test('quadifies with numeric array property', async () => {
    const quads = await jrql.quads({ '@id': 'fred', age: [40] }, { query: true }, context);
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(rdf.namedNode('http://test.m-ld.org/#age').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('40');
  });

  describe('lists', () => {
    test('converts key to index number', () => {
      expect(toIndexNumber(undefined)).toBeUndefined();
      expect(toIndexNumber(null)).toBeUndefined();
      expect(toIndexNumber('a')).toBeUndefined();
      expect(toIndexNumber([0])).toBeUndefined();
      expect(toIndexNumber('[0]')).toBeUndefined();

      // Allow numeric keys (Javascript object)
      expect(toIndexNumber(0)).toBe(0);
      expect(toIndexNumber(10)).toBe(10);

      expect(toIndexNumber('0')).toBe(0);
      expect(toIndexNumber('10')).toBe(10);

      expect(toIndexNumber('0,0')).toEqual([0, 0]);
      expect(toIndexNumber('0,10')).toEqual([0, 10]);

      expect(toIndexNumber('data:,0')).toBe(0);
      expect(toIndexNumber('data:,10')).toBe(10);

      expect(toIndexNumber('data:,0,0')).toEqual([0, 0]);
      expect(toIndexNumber('data:,0,10')).toEqual([0, 10]);
    });

    test('quadifies a top-level singleton list', async () => {
      const store = new N3.Store();
      store.addQuads(await jrql.quads({
        '@id': 'shopping',
        '@list': 'Bread'
      }, { query: true }, context));
      expect(store.size).toBe(2);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:,0'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('Variable');

      const breadSlot = store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0];
      expect(breadSlot).toBeDefined();
    });

    test('quadifies a singleton list property', async () => {
      const store = new N3.Store();
      store.addQuads(await jrql.quads({
        '@id': 'fred',
        shopping: { '@list': 'Bread' }
      }, { query: true }, context));
      expect(store.size).toBe(3);

      const fredShopping = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/fred'),
        rdf.namedNode('http://test.m-ld.org/#shopping'), null, null)[0];
      expect(fredShopping.object.termType).toBe('Variable');

      const shoppingBread = store.getQuads(
        fredShopping.object,
        rdf.namedNode('data:,0'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('Variable');

      const breadSlot = store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0];
      expect(breadSlot).toBeDefined();
    });

    test('quadifies a top-level array list', async () => {
      const store = new N3.Store();
      store.addQuads(await jrql.quads({
        '@id': 'shopping',
        '@list': ['Bread', 'Jam']
      }, { query: true }, context));
      expect(store.size).toBe(4);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:,0'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('Variable');

      const shoppingJam = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:,1'), null, null)[0];
      expect(shoppingJam.object.termType).toBe('Variable');

      const breadSlot = store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0];
      expect(breadSlot).toBeDefined();

      const jamSlot = store.getQuads(
        shoppingJam.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Jam'), null)[0];
      expect(jamSlot).toBeDefined();
    });

    test('quadifies a top-level indexed hash list', async () => {
      const store = new N3.Store();
      store.addQuads(await jrql.quads({
        '@id': 'shopping',
        '@list': { '1': 'Bread' }
      }, { query: true }, context));
      expect(store.size).toBe(2);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:,1'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('Variable');

      const breadSlot = store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0];
      expect(breadSlot).toBeDefined();
    });

    test('quadifies a top-level indexed hash list with multiple items', async () => {
      const store = new N3.Store();
      store.addQuads(await jrql.quads({
        '@id': 'shopping',
        '@list': { '1': ['Bread', 'Milk'] }
      }, { query: true }, context));
      expect(store.size).toBe(4);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:,1,0'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('Variable');

      expect(store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0]).toBeDefined();

      const shoppingMilk = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:,1,1'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('Variable');

      expect(store.getQuads(
        shoppingMilk.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Milk'), null)[0]).toBeDefined();
    });

    test('quadifies a top-level data URL indexed hash list', async () => {
      const store = new N3.Store();
      store.addQuads(await jrql.quads({
        '@id': 'shopping',
        '@list': { 'data:,1': 'Bread' }
      }, { query: true }, context));
      expect(store.size).toBe(2);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:,1'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('Variable');

      const breadSlot = store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0];
      expect(breadSlot).toBeDefined();
    });

    test('rejects a list with bad indexes', async () => {
      await expect(jrql.quads({
        '@id': 'shopping', '@list': { 'data:,x': 'Bread' }
      }, { query: true }, context))
        .rejects.toThrow();
      await expect(jrql.quads({
        '@id': 'shopping', '@list': { 'x': 'Bread' }
      }, { query: true }, context))
        .rejects.toThrow();
      await expect(jrql.quads({
        '@id': 'shopping', '@list': { 'http://example.org/Bad': 'Bread' }
      }, { query: true }, context))
        .rejects.toThrow();
    });
  });
});