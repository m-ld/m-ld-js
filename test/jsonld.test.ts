import { compareValues, expandValue, getValues, JsonldContext } from '../src/engine/jsonld';

describe('JSON-LD', () => {
  const context = {
    '@base': 'http://example.org/',
    '@vocab': 'http://example.org/#',
    'mld': 'http://m-ld.org/'
  };
  let ctx: JsonldContext;

  beforeAll(async () => {
    ctx = await JsonldContext.active(context);
  });

  describe('expand term', () => {
    test('expand prefix', () => {
      expect(ctx.expandTerm('mld:hello')).toBe('http://m-ld.org/hello');
    });

    test('ignores undefined prefix', () => {
      expect(ctx.expandTerm('foo:hello')).toBe('foo:hello');
    });

    test('leaves an IRI', () => {
      expect(ctx.expandTerm('http://hello.com/')).toBe('http://hello.com/');
    });

    test('expands against base', () => {
      expect(ctx.expandTerm('hello')).toBe('http://example.org/hello');
    });
  });

  describe('compact IRI', () => {
    test('compacts with prefix', () => {
      expect(ctx.compactIri('http://m-ld.org/hello')).toBe('mld:hello');
    });

    test('ignores prefix', () => {
      expect(ctx.compactIri('foo:hello')).toBe('foo:hello');
    });

    test('leaves an unmatched IRI', () => {
      expect(ctx.compactIri('http://hello.com/')).toBe('http://hello.com/');
    });

    test('compacts using base', () => {
      expect(ctx.compactIri('http://example.org/hello')).toBe('hello');
    });
  });

  test('gets values from subject', () => {
    expect(getValues({}, 'any')).toEqual([]);
    expect(getValues({ any: null }, 'any')).toEqual([]);
    expect(getValues({ any: undefined }, 'any')).toEqual([]);
    expect(getValues({ any: { '@set': [] } }, 'any')).toEqual([]);
    expect(getValues({ any: 'any' }, 'any')).toEqual(['any']);
    expect(getValues({ any: ['any'] }, 'any')).toEqual(['any']);
    // Edge case: jsonld library incorrectly skips falsy values
    expect(getValues({ any: '' }, 'any')).toEqual(['']);
    expect(getValues({ any: [0, ''] }, 'any')).toEqual([0, '']);
    expect(getValues({ any: { '@set': 0 } }, 'any')).toEqual([0]);
    expect(getValues({ any: { '@set': [0] } }, 'any')).toEqual([0]);
  });

  test('compares values', () => {
    expect(compareValues(1, 1)).toBe(true);
    expect(compareValues(1, 2)).toBe(false);
    expect(compareValues({ '@id': 'a' }, { '@id': 'a' })).toBe(true);
    expect(compareValues({ '@id': 'a' }, { '@id': 'b' })).toBe(false);
    expect(compareValues({ '@vocab': 'a' }, { '@vocab': 'a' })).toBe(true);
    expect(compareValues({ '@vocab': 'a' }, { '@vocab': 'b' })).toBe(false);
    // relative IRIs do not compare for reference and vocab reference
    expect(compareValues({ '@vocab': 'a' }, { '@id': 'a' })).toBe(false);
    expect(compareValues({ '@id': 'a' }, { '@vocab': 'a' })).toBe(false);
    // absolute IRIs do compare for reference and vocab reference
    expect(compareValues({ '@vocab': 'http://a' }, { '@id': 'http://a' })).toBe(true);
    expect(compareValues({ '@id': 'http://a' }, { '@vocab': 'http://a' })).toBe(true);
  });

  describe('expanding values', () => {
    test('references & subjects', () => {
      expect(expandValue(
        'spouse', { '@id': 'fred' }))
        .toEqual({ raw: 'fred', canonical: 'fred', type: '@id' });
      expect(expandValue(
        'spouse', { '@vocab': 'fred' }))
        .toEqual({ raw: 'fred', canonical: 'fred', type: '@vocab' });
      expect(expandValue(
        'spouse', { '@id': 'fred', name: 'Fred' }))
        .toEqual({ raw: 'fred', canonical: 'fred', type: '@id' });
      expect(expandValue(
        'spouse', { name: 'Fred' }))
        .toEqual({ raw: { name: 'Fred' }, canonical: '[object Object]', type: '@none' });
    });
    test('plain JSON values', () => {
      expect(expandValue(
        'name', 'Fred'))
        .toEqual({ raw: 'Fred', canonical: 'Fred', type: '@none' });
      expect(expandValue(
        'height', 1))
        .toEqual({ raw: 1, canonical: '1', type: 'http://www.w3.org/2001/XMLSchema#integer' });
      expect(expandValue(
        'height', 1.1))
        .toEqual({ raw: 1.1, canonical: '1.1E0', type: 'http://www.w3.org/2001/XMLSchema#double' });
    });
    test('type from context', async () => {
      const ctx = await JsonldContext.active({
        'name': {
          '@id': 'http://test.m-ld.org/#name',
          '@type': 'http://www.w3.org/2001/XMLSchema#string'
        }
      });
      expect(expandValue(
        'name', 'Fred', ctx))
        .toEqual({
          raw: 'Fred',
          canonical: 'Fred',
          type: 'http://www.w3.org/2001/XMLSchema#string'
        });
    });
    test('language from context', async () => {
      const ctx = await JsonldContext.active({
        'name': { '@id': 'http://test.m-ld.org/#name', '@language': 'en-gb' }
      });
      expect(expandValue(
        'name', 'Fred', ctx))
        .toEqual({
          raw: 'Fred',
          canonical: 'Fred',
          type: 'http://www.w3.org/2001/XMLSchema#string',
          language: 'en-gb'
        });
    });
    test('value objects', () => {
      expect(expandValue(
        'name', { '@value': 'Fred' }))
        .toEqual({ raw: 'Fred', canonical: 'Fred', type: '@none', isValue: true });
      expect(expandValue(
        'name', {
          '@value': 'Fred',
          '@type': 'http://www.w3.org/2001/XMLSchema#string'
        }))
        .toEqual({
          raw: 'Fred',
          canonical: 'Fred',
          type: 'http://www.w3.org/2001/XMLSchema#string',
          isValue: true
        });
    });
  });
});