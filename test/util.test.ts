import { Future, toArray } from '../src/util';

test('Future can be resolved', async () => {
  const f = new Future<string>();
  setTimeout(() => f.resolve('yes'), 0);
  expect(await f).toBe('yes');
});

test('Future can be rejected', async () => {
  const f = new Future<string>();
  setTimeout(() => f.reject('no'), 0);
  try {
    await f;
  } catch (e) {
    expect(e).toBe('no');
  }
});

test('graphy to array', () => {
  expect(toArray()).toEqual([]);
  expect(toArray(null)).toEqual([]);
  expect(toArray('')).toEqual(['']);
  expect(toArray(0)).toEqual([0]);
  expect(toArray([0])).toEqual([0]);
  expect(toArray([0, 0])).toEqual([0, 0]);
  expect(toArray([0, 1])).toEqual([0, 1]);
  expect(toArray([0, null])).toEqual([0]);
  expect(toArray([0, undefined])).toEqual([0]);
});
