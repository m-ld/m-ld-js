import { JrqlQuads } from '../src/engine/dataset/JrqlQuads';
import * as N3 from 'n3';
import { Graph } from '../src/engine/dataset';
import { mock } from 'jest-mock-extended';
import { Constraint, IndirectedData } from '../src';
import { JrqlMode } from '../src/engine/jrql-util';
import { Literal, RdfFactory } from '../src/engine/quads';
import { dateDatatype } from './datatypeFixtures';
import { JsonldContext } from '../src/engine/jsonld';
import { CacheFactory } from '../src/engine/cache';

describe('json-rql Quads translation', () => {
  const rdf = new RdfFactory('http://test.m-ld.org');

  let jrql: JrqlQuads;
  let ctx: JsonldContext;
  let indirectedData: IndirectedData;

  beforeEach(async () => {
    ctx = await JsonldContext.active({
      '@base': 'http://test.m-ld.org/',
      '@vocab': '#',
      'ex': 'http://example.org/'
    });
    jrql = new JrqlQuads(
      mock<Graph>({ rdf }),
      (...args) => indirectedData?.(...args), // Deferred
      new CacheFactory({ max: 0 }) // TODO
    );
  });

  test('quadifies @id-only top-level subject with variable p-o', () => {
    const quads = jrql.toQuads({ '@id': 'fred' }, JrqlMode.match, ctx);
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('Variable');
  });

  test('quadifies anonymous subject', () => {
    const quads = jrql.toQuads({ 'name': 'Fred' }, JrqlMode.match, ctx);
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(rdf.namedNode('http://test.m-ld.org/#name').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Fred');
  });

  test('quadifies subject with prefixed id', () => {
    const quads = jrql.toQuads({ '@id': 'ex:fred', 'name': 'Fred' }, JrqlMode.load, ctx);
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://example.org/fred').equals(quads[0].subject)).toBe(true);
    expect(quads[0].predicate.value).toBe('http://test.m-ld.org/#name');
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Fred');
  });

  test('quadifies anonymous variable predicate', () => {
    const quads = jrql.toQuads({ '?': 'Fred' }, JrqlMode.match, ctx);
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Fred');
  });

  test('quadifies anonymous reference predicate', () => {
    const quads = jrql.toQuads({ '?': { '@id': 'fred' } }, JrqlMode.match, ctx);
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('NamedNode');
    expect(quads[0].object.value).toBe('http://test.m-ld.org/fred');
  });

  test('quadifies anonymous vocab reference predicate', () => {
    const quads = jrql.toQuads({ '?': { '@vocab': 'name' } }, JrqlMode.match, ctx);
    expect(quads.length).toBe(1);
    expect(quads[0].subject.termType).toBe('Variable');
    expect(quads[0].predicate.termType).toBe('Variable');
    expect(quads[0].object.termType).toBe('NamedNode');
    expect(quads[0].object.value).toBe('http://test.m-ld.org/#name');
  });

  test('quadifies with numeric property', () => {
    const quads = jrql.toQuads({ '@id': 'fred', age: 40 }, JrqlMode.match, ctx);
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(rdf.namedNode('http://test.m-ld.org/#age').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('40');
  });

  test('quadifies with numeric array property', () => {
    const quads = jrql.toQuads({ '@id': 'fred', age: [40] }, JrqlMode.match, ctx);
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(rdf.namedNode('http://test.m-ld.org/#age').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('40');
  });

  test('extracts inline filter', () => {
    const filters: Constraint[] = [];
    const quads = jrql.in(JrqlMode.match, ctx)
      .on('filter', f => filters.push(f))
      .toQuads({
      '@id': 'fred', age: { '@value': '?age', '@gt': 40 }
    });
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(rdf.namedNode('http://test.m-ld.org/#age').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Variable');
    expect(quads[0].object.value).toBe('age');
    expect(filters).toEqual([{ '@gt': ['?age', 40] }]);
  });

  test('extracts anonymous inline filter', () => {
    const filters: Constraint[] = [];
    const quads = jrql.in(JrqlMode.match, ctx)
      .on('filter', f => filters.push(f))
      .toQuads({ '@id': 'fred', age: { '@gt': 40 } });
    expect(quads.length).toBe(1);
    expect(rdf.namedNode('http://test.m-ld.org/fred').equals(quads[0].subject)).toBe(true);
    expect(rdf.namedNode('http://test.m-ld.org/#age').equals(quads[0].predicate)).toBe(true);
    expect(quads[0].object.termType).toBe('Variable');
    expect(filters).toEqual([{ '@gt': [`?${quads[0].object.value}`, 40] }]);
  });

  test('serialises json-able datatype', () => {
    indirectedData = datatype => {
      if (datatype === dateDatatype['@id'])
        return dateDatatype;
    }
    const quads = jrql.in(JrqlMode.serial, ctx).toQuads({
      '@id': 'fred',
      birthday: {
        '@id': 'Sat Jan 01 2000',
        '@type': 'http://ex.org/date',
        '@value': { year: 0, month: 1, date: 1 }
      }
    });
    expect(quads.length).toBe(1);
    expect(quads[0].subject.value).toBe('http://test.m-ld.org/fred');
    expect(quads[0].predicate.value).toBe('http://test.m-ld.org/#birthday');
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Sat Jan 01 2000');
    expect((<Literal>quads[0].object).typed!).toEqual({
      data: new Date('01-01-2000'), type: dateDatatype
    });
  });

  test('serialises json-able without datatype', () => {
    indirectedData = () => undefined
    const quads = jrql.in(JrqlMode.serial, ctx).toQuads({
      '@id': 'fred',
      birthday: {
        '@id': 'Sat Jan 01 2000',
        '@type': 'http://ex.org/date',
        '@value': { year: 0, month: 1, date: 1 }
      }
    });
    expect(quads.length).toBe(1);
    expect(quads[0].subject.value).toBe('http://test.m-ld.org/fred');
    expect(quads[0].predicate.value).toBe('http://test.m-ld.org/#birthday');
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Sat Jan 01 2000');
    expect((<Literal>quads[0].object).typed).toEqual({
      data: { year: 0, month: 1, date: 1 }
    });
  });

  test('validates json-able datatype', () => {
    indirectedData = datatype => {
      if (datatype === dateDatatype['@id'])
        return dateDatatype;
    }
    const quads = jrql.in(JrqlMode.graph, ctx).toQuads({
      '@id': 'fred',
      birthday: {
        '@type': 'http://ex.org/date',
        '@value': '01-01-2000'
      }
    });
    expect(quads.length).toBe(1);
    expect(quads[0].subject.value).toBe('http://test.m-ld.org/fred');
    expect(quads[0].predicate.value).toBe('http://test.m-ld.org/#birthday');
    expect(quads[0].object.termType).toBe('Literal');
    expect(quads[0].object.value).toBe('Sat Jan 01 2000');
    expect((<Literal>quads[0].object).typed).toEqual({
      data: new Date('01-01-2000'), type: dateDatatype
    });
  });

  describe('lists', () => {
    test('quadifies a top-level singleton list', () => {
      const store = new N3.Store();
      store.addQuads(jrql.toQuads({
        '@id': 'shopping',
        '@list': 'Bread'
      }, JrqlMode.load, ctx));
      expect(store.size).toBe(2);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:application/mld-li,0'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('NamedNode');

      expect(store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0]).toBeDefined();
    });

    test('quadifies a singleton list property', () => {
      const store = new N3.Store();
      store.addQuads(jrql.toQuads({
        '@id': 'fred',
        shopping: { '@list': 'Bread' }
      }, JrqlMode.load, ctx));
      expect(store.size).toBe(3);

      const fredShopping = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/fred'),
        rdf.namedNode('http://test.m-ld.org/#shopping'), null, null)[0];
      expect(fredShopping.object.termType).toBe('NamedNode');

      const shoppingBread = store.getQuads(
        fredShopping.object,
        rdf.namedNode('data:application/mld-li,0'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('NamedNode');

      expect(store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0]).toBeDefined();
    });

    test('quadifies a top-level array list', () => {
      const store = new N3.Store();
      store.addQuads(jrql.toQuads({
        '@id': 'shopping',
        '@list': ['Bread', 'Jam']
      }, JrqlMode.load, ctx));
      expect(store.size).toBe(4);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:application/mld-li,0'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('NamedNode');

      const shoppingJam = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:application/mld-li,1'), null, null)[0];
      expect(shoppingJam.object.termType).toBe('NamedNode');

      expect(store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0]).toBeDefined();

      expect(store.getQuads(
        shoppingJam.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Jam'), null)[0]).toBeDefined();
    });

    test('quadifies a top-level indexed hash list', () => {
      const store = new N3.Store();
      store.addQuads(jrql.toQuads({
        '@id': 'shopping',
        '@list': { '1': 'Bread' }
      }, JrqlMode.load, ctx));
      expect(store.size).toBe(2);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:application/mld-li,1'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('NamedNode');

      expect(store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0]).toBeDefined();
    });

    test('quadifies a top-level indexed hash list with multiple items', () => {
      const store = new N3.Store();
      store.addQuads(jrql.toQuads({
        '@id': 'shopping',
        '@list': { '1': ['Bread', 'Milk'] }
      }, JrqlMode.load, ctx));
      expect(store.size).toBe(4);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:application/mld-li,1,0'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('NamedNode');

      expect(store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0]).toBeDefined();

      const shoppingMilk = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:application/mld-li,1,1'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('NamedNode');

      expect(store.getQuads(
        shoppingMilk.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Milk'), null)[0]).toBeDefined();
    });

    test('quadifies a top-level data URL indexed hash list', () => {
      const store = new N3.Store();
      store.addQuads(jrql.toQuads({
        '@id': 'shopping',
        '@list': { 'data:application/mld-li,1': 'Bread' }
      }, JrqlMode.load, ctx));
      expect(store.size).toBe(2);

      const shoppingBread = store.getQuads(
        rdf.namedNode('http://test.m-ld.org/shopping'),
        rdf.namedNode('data:application/mld-li,1'), null, null)[0];
      expect(shoppingBread.object.termType).toBe('NamedNode');

      expect(store.getQuads(
        shoppingBread.object,
        rdf.namedNode('http://json-rql.org/#item'),
        rdf.literal('Bread'), null)[0]).toBeDefined();
    });

    test('rejects a list with bad indexes', () => {
      expect(() => jrql.toQuads({
        '@id': 'shopping', '@list': { 'data:application/mld-li,x': 'Bread' }
      }, JrqlMode.load, ctx)).toThrow()
      expect(() => jrql.toQuads({
        '@id': 'shopping', '@list': { 'x': 'Bread' }
      }, JrqlMode.load, ctx)).toThrow();
      expect(() => jrql.toQuads({
        '@id': 'shopping', '@list': { 'http://example.org/Bad': 'Bread' }
      }, JrqlMode.load, ctx)).toThrow();
    });
  });
});