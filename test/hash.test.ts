import { Hash } from '../src/hash'

test('Hash is equal to itself', () => {
  const hash = Hash.random();
  expect(hash.equals(hash)).toBe(true);
});

test('Random hashes are different', () => {
  expect(Hash.random().equals(Hash.random())).toBe(false);
});

test('Encoding is 44 characters long', () => {
  expect(Hash.random().encode().length).toBe(44);
  expect(Hash.digest('1').encode().length).toBe(44);
});

test('Add does something', () => {
  const one = Hash.random(), two = Hash.random();
  expect(one.add(two).equals(one)).toBe(false);
});

test('Add is commutative', () => {
  const one = Hash.random(), two = Hash.random();
  expect(one.add(two).equals(two.add(one))).toBe(true);
});

test('Add is really commutative', () => {
  const zero = Hash.random(), one = Hash.digest('1'), two = Hash.digest('2');
  expect(zero.add(one).add(two).equals(zero.add(two).add(one))).toBe(true);
});

test('Coerce to width', () => {
  expect(Hash.toWidth(Buffer.of(0), 1).toString('hex')).toBe('00');
  expect(Hash.toWidth(Buffer.of(0), 2).toString('hex')).toBe('0000');
  expect(Hash.toWidth(Buffer.of(1), 2).toString('hex')).toBe('0001');
  expect(Hash.toWidth(Buffer.of(-1), 2).toString('hex')).toBe('ffff');
  expect(Hash.toWidth(Buffer.of(127), 2).toString('hex')).toBe('007f');
  expect(Hash.toWidth(Buffer.of(-128), 2).toString('hex')).toBe('ff80');
  expect(Hash.toWidth(Buffer.of(0, 0), 1).toString('hex')).toBe('00');
  expect(Hash.toWidth(Buffer.of(1, 0), 1).toString('hex')).toBe('00');
  expect(Hash.toWidth(Buffer.of(1, 1), 1).toString('hex')).toBe('01');
});