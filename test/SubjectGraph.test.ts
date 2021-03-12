import { toIndexNumber } from '../src/engine/jrql-util';

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
