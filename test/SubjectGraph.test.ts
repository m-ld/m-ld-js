import { toIndexNumber } from '../src/engine/jrql-util';
import { jrqlValue, SubjectGraph } from '../src/engine/SubjectGraph';
import { RdfFactory } from '../src/engine/quads';
import { dateDatatype } from './datatypeFixtures';
import { JsonldContext } from '../src/engine/jsonld';

test('converts key to index number', () => {
  expect(toIndexNumber(undefined)).toBeUndefined();
  expect(toIndexNumber(null)).toBeUndefined();
  expect(toIndexNumber('a')).toBeUndefined();
  expect(toIndexNumber([0])).toBeUndefined();
  expect(toIndexNumber('[0]')).toBeUndefined();

  // Allow numeric keys (Javascript object)
  expect(toIndexNumber(0)).toEqual([0]);
  expect(toIndexNumber(10)).toEqual([10]);

  expect(toIndexNumber('0')).toEqual([0]);
  expect(toIndexNumber('10')).toEqual([10]);

  expect(toIndexNumber('0,0')).toEqual([0, 0]);
  expect(toIndexNumber('0,10')).toEqual([0, 10]);

  expect(toIndexNumber('data:application/mld-li,0')).toEqual([0]);
  expect(toIndexNumber('data:application/mld-li,10')).toEqual([10]);

  expect(toIndexNumber('data:application/mld-li,0,0')).toEqual([0, 0]);
  expect(toIndexNumber('data:application/mld-li,0,10')).toEqual([0, 10]);
});

describe('Subject Graph', () => {
  test('has graph representation', () => {
    const fred = { '@id': 'fred', name: 'Fred' };
    expect(new SubjectGraph([fred]).graph.get('fred')).toEqual(fred);
  });

  test('has deep graph ', () => {
    const fred = { '@id': 'fred', wife: { '@id': 'wilma' } };
    const wilma = { '@id': 'wilma', name: 'Wilma' };
    expect(new SubjectGraph([fred, wilma]).graph.get('fred')?.wife).toEqual(wilma);
  });

  test('has merged subjects', () => {
    const fred = { '@id': 'fred', wife: { '@id': 'wilma' } };
    const fred2 = { '@id': 'fred', name: 'Fred' };
    expect(new SubjectGraph([fred, fred2]).graph.get('fred')).toEqual({
      '@id': 'fred', wife: { '@id': 'wilma' }, name: 'Fred'
    });
  });

  // Expected behaviour, but not used
  test.skip('has merged properties', () => {
    const fred = { '@id': 'fred', name: 'Fred' };
    const fred2 = { '@id': 'fred', name: 'Flintstone' };
    expect(new SubjectGraph([fred, fred2]).graph.get('fred')).toEqual({
      '@id': 'fred', name: expect.arrayContaining(['Fred', 'Flintstone'])
    });
  });
});

// Note plain value generation is tested via dependent code
describe('json-rql typed values', () => {
  test('creates value object from typed literal', () => {
    const rdf = new RdfFactory(undefined);
    const date = new Date('01-01-2000');
    expect(jrqlValue(
      'date',
      rdf.literal(dateDatatype.toLexical(date), dateDatatype, date)
    )).toEqual({
      '@type': dateDatatype['@id'],
      '@value': date
    });
  });

  test('creates serialised value object from typed literal', () => {
    const rdf = new RdfFactory(undefined);
    const date = new Date('01-01-2000');
    expect(jrqlValue(
      'date',
      rdf.literal(dateDatatype.toLexical(date), dateDatatype, date),
      JsonldContext.NONE,
      true
    )).toEqual({
      '@type': dateDatatype['@id'],
      '@value': { year: 2000, month: 1, date: 1 }
    });
  });
});
