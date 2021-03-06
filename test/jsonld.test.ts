import { expandTerm, compactIri, ActiveContext, activeCtx, getValues } from '../src/engine/jsonld';

describe('JSON-LD', () => {
  const context = {
    '@base': 'http://example.org/',
    '@vocab': 'http://example.org/#',
    'mld': 'http://m-ld.org/'
  };
  let ctx: ActiveContext;

  beforeAll(async () => {
    ctx = await activeCtx(context);
  });

  describe('expand term', () => {
    test('expand prefix', () => {
      expect(expandTerm('mld:hello', ctx)).toBe('http://m-ld.org/hello');
    });

    test('ignores undefined prefix', () => {
      expect(expandTerm('foo:hello', ctx)).toBe('foo:hello');
    });

    test('leaves an IRI', () => {
      expect(expandTerm('http://hello.com/', ctx)).toBe('http://hello.com/');
    });

    test('expands against base', () => {
      expect(expandTerm('hello', ctx)).toBe('http://example.org/hello');
    });
  });

  describe('compact IRI', () => {
    test('compacts with prefix', () => {
      expect(compactIri('http://m-ld.org/hello', ctx)).toBe('mld:hello');
    });

    test('ignores prefix', () => {
      expect(compactIri('foo:hello', ctx)).toBe('foo:hello');
    });

    test('leaves an unmatched IRI', () => {
      expect(compactIri('http://hello.com/', ctx)).toBe('http://hello.com/');
    });

    test('compacts using base', () => {
      expect(compactIri('http://example.org/hello', ctx)).toBe('hello');
    });
  });

  test('gets values from subject', () => {
    expect(getValues({}, 'any')).toEqual([]);
    expect(getValues({ any: null }, 'any')).toEqual([]);
    expect(getValues({ any: undefined }, 'any')).toEqual([]);
    expect(getValues({ any: 'any' }, 'any')).toEqual(['any']);
    expect(getValues({ any: ['any'] }, 'any')).toEqual(['any']);
    // Edge case: jsonld library incorrectly skips falsy values
    expect(getValues({ any: '' }, 'any')).toEqual(['']);
    expect(getValues({ any: [0, ''] }, 'any')).toEqual([0, '']);
  });
});