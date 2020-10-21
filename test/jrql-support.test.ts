import { Pattern, isWrite } from '../src/jrql-support';

describe('write type checking', () => {
  test('includes updates', () => {
    expect(isWrite({ 'insert': {} } as Pattern)).toBe(true);
  });

  test('includes writeable groups', () => {
    expect(isWrite({ '@graph': [] } as Pattern)).toBe(true);
  });

  test('includes writeable groups with context', () => {
    expect(isWrite({ '@graph': [] } as Pattern)).toBe(true);
  });

  test('includes subjects', () => {
    expect(isWrite({ '@id': 'fred', name: 'Fred' } as Pattern)).toBe(true);
  });

  // No-op is still a logical write
  test('includes empty subjects', () => {
    expect(isWrite({} as Pattern)).toBe(true);
  });

  test('excludes reads', () => {
    expect(isWrite({ '@describe': '?s' } as Pattern)).toBe(false);
  });

  test('excludes unions', () => {
    expect(isWrite({ '@union': [] } as Pattern)).toBe(false);
  });
});