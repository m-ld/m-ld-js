import { Future } from '../src/util';

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
