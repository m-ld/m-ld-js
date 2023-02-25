import { addPropertyObject } from '../src/engine/jrql-util';

// Note that this function is also well tested through it usages
describe('add subject properties', () => {
  test('adds subject', () => {
    const fred = { '@id': 'fred' };
    addPropertyObject(fred, 'wife', { '@id': 'wilma', name: 'Wilma' });
    expect(fred).toEqual({ '@id': 'fred', wife: { '@id': 'wilma', name: 'Wilma' } });
  });

  test('favours subject over reference', () => {
    const fred = { '@id': 'fred', wife: { '@id': 'wilma' } };
    addPropertyObject(fred, 'wife', { '@id': 'wilma', name: 'Wilma' });
    expect(fred).toEqual({ '@id': 'fred', wife: { '@id': 'wilma', name: 'Wilma' } });
  });

  test('favours subject over reference in array', () => {
    const fred = { '@id': 'fred', kids: [{ '@id': 'stony' }, { '@id': 'pebbles' }] };
    addPropertyObject(fred, 'kids', { '@id': 'pebbles', name: 'Pebbles' });
    expect(fred).toEqual({
      '@id': 'fred',
      kids: [{ '@id': 'stony' }, { '@id': 'pebbles', name: 'Pebbles' }]
    });
  });

  test('favours subject over vocab reference', () => {
    const fred = { '@id': 'fred', character: { '@vocab': 'main' } };
    addPropertyObject(fred, 'character', { '@vocab': 'main', abbrev: 'MC' });
    expect(fred).toEqual({ '@id': 'fred', character: { '@vocab': 'main', abbrev: 'MC' } });
  });
});