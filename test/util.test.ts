import { deepValues, Future, setAtPath } from '../src/engine/util';
import { any, array, shortId } from '../src';

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
  expect.assertions(1);
});

test('Future ignores multiple resolutions', async () => {
  const f = new Future<string>();
  f.resolve('this');
  f.resolve('that');
  expect(await f).toBe('this');
});

test('Future ignores reject after resolve', async () => {
  const f = new Future<string>();
  f.resolve('this');
  f.reject('AARGH');
  expect(await f).toBe('this');
});

test('Future ignores resolve after reject', async () => {
  const f = new Future<string>();
  f.reject('AARGH');
  f.resolve('this');
  try {
    await f;
  } catch (e) {
    expect(e).toBe('AARGH');
  }
  expect.assertions(1);
});

test('Unhandled future does not cause UnhandledPromiseRejection', () => {
  new Future().reject('oops');
});

test('graphy to array', () => {
  expect(array()).toEqual([]);
  expect(array(null)).toEqual([]);
  expect(array('')).toEqual(['']);
  expect(array(0)).toEqual([0]);
  expect(array([0])).toEqual([0]);
  expect(array([0, 0])).toEqual([0, 0]);
  expect(array([0, 1])).toEqual([0, 1]);
  expect(array([0, null])).toEqual([0]);
  expect(array([0, undefined])).toEqual([0]);
});

test('short id is valid XML local name', () => {
  for (let i = 0; i < 10; i++)
    expect(shortId()).toMatch(/^[a-zA-Z_]([a-zA-Z0-9_])*/g);
});

test('short id for a string is valid XML local name', () => {
  expect(shortId('_*.')).toMatch(/^[a-zA-Z_]([a-zA-Z0-9_])*/g);
});

test('short id for a longer string is valid XML local name', () => {
  expect(shortId('assain-tackies.m-ld.org')).toMatch(/^[a-zA-Z_]([a-zA-Z0-9_])*/g);
});

test('short Id is always different', () => {
  expect(shortId()).not.toEqual(shortId());
});

test('short Id is the right length', () => {
  expect(shortId(10).length).toBe(10);
});

test('short Id for a string is always the same', () => {
  expect(shortId('foobar')).toEqual(shortId('foobar'));
});

test('short Id for strings are different', () => {
  expect(shortId('foobar')).not.toEqual(shortId('snafu'));
});

test('any var is always different', () => {
  expect(any()).not.toEqual(any());
});

test('get deep values of an object', () => {
  expect([...deepValues({})]).toEqual([]);
  expect([...deepValues([])]).toEqual([]);
  expect([...deepValues([0])]).toEqual([[['0'], 0]]);
  expect([...deepValues({ 'a': 1 })]).toEqual([[['a'], 1]]);
  expect([...deepValues({ 'a': [0] })]).toEqual([[['a', '0'], 0]]);
});

test('set shallow path value of an array', () => {
  const a = [0];
  setAtPath(a, ['0'], 1);
  expect(a).toEqual([1]);
});

test('set shallow path value of an object', () => {
  const a = { a: 1 };
  setAtPath(a, ['a'], 2);
  expect(a).toEqual({ a: 2 });
});

test('set deep path value of an object', () => {
  const a = { a: {} };
  setAtPath(a, ['a', 'b'], 2);
  expect(a).toEqual({ a: { b: 2 } });
});

test('set created path object value of an object', () => {
  const a = {};
  setAtPath(a, ['a', 'b'], 2, () => ({}));
  expect(a).toEqual({ a: { b: 2 } });
});

test('set created path array value of an object', () => {
  const a = {};
  setAtPath(a, ['a', '0'], 2, () => []);
  expect(a).toEqual({ a: [2] });
});
