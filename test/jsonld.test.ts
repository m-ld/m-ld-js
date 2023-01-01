import { compareValues, getValues, JsonldContext, mapValue } from '../src/engine/jsonld';

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

  describe('mapping values', () => {
    test('references & subjects', () => {
      expect(mapValue(
        'spouse', { '@id': 'fred' }, (value, type) => [value, type]))
        .toEqual(['fred', '@id']);
      expect(mapValue(
        'spouse', { '@vocab': 'fred' }, (value, type) => [value, type]))
        .toEqual(['fred', '@vocab']);
      expect(mapValue(
        'spouse', { '@id': 'fred', name: 'Fred' }, (value, type) => [value, type]))
        .toEqual(['fred', '@id']);
      expect(() => mapValue(
        'spouse', { name: 'Fred' }, (value, type) => [value, type]))
        .toThrow(TypeError);
    });
    test('plain JSON values', () => {
      expect(mapValue(
        'name', 'Fred', (value, type) => [value, type]))
        .toEqual(['Fred', '@none']);
      expect(mapValue(
        'height', 1, (value, type) => [value, type]))
        .toEqual(['1', 'http://www.w3.org/2001/XMLSchema#integer']);
      expect(mapValue(
        'height', 1.1, (value, type) => [value, type]))
        .toEqual(['1.1E0', 'http://www.w3.org/2001/XMLSchema#double']);
    });
    test('type from context', async () => {
      const ctx = await JsonldContext.active({
        'name': {
          '@id': 'http://test.m-ld.org/#name',
          '@type': 'http://www.w3.org/2001/XMLSchema#string'
        }
      });
      expect(mapValue(
        'name', 'Fred', (value, type) => [value, type], { ctx }))
        .toEqual(['Fred', 'http://www.w3.org/2001/XMLSchema#string']);
    });
    test('language from context', async () => {
      const ctx = await JsonldContext.active({
        'name': { '@id': 'http://test.m-ld.org/#name', '@language': 'en-gb' }
      });
      expect(mapValue(
        'name', 'Fred', (value, type, language) => [value, type, language], { ctx }))
        .toEqual(['Fred', 'http://www.w3.org/2001/XMLSchema#string', 'en-gb']);
    });
    test('value objects', () => {
      expect(mapValue(
        'name', { '@value': 'Fred' }, (value, type) => [value, type]))
        .toEqual(['Fred', '@none']);
      expect(mapValue(
        'name', {
          '@value': 'Fred',
          '@type': 'http://www.w3.org/2001/XMLSchema#string'
        }, (value, type) => [value, type]))
        .toEqual(['Fred', 'http://www.w3.org/2001/XMLSchema#string']);
    });
    test('intercept raw', () => {
      expect(mapValue(
        'name', 'Fred', (value, type) => [value, type], {
          interceptRaw(value) {
            expect(value).toBe('Fred');
            return ['Barney', 'trouble'];
          }
        }))
        .toEqual(['Barney', 'trouble']);
      expect(mapValue(
        'name', { '@value': 'Fred' }, (value, type) => [value, type], {
          interceptRaw() { fail('value object should not intercept'); }
        }))
        .toEqual(['Fred', '@none']);
      expect(mapValue(
        'spouse', { '@id': 'fred' }, (value, type) => [value, type], {
          interceptRaw(value) {
            expect(value).toBe('fred');
            return ['barney', 'trouble'];
          }
        }))
        .toEqual(['barney', 'trouble']);
    });
  });
});