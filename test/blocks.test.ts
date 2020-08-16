import { HashStringBagBlock } from '../src/engine/blocks'

test('Genesis block has ID and no data', () => {
  expect(HashStringBagBlock.genesis().id).toBeTruthy();
  expect(HashStringBagBlock.genesis().data).toBeUndefined();
});

test('Next block is not the same as this', () => {
  const first = HashStringBagBlock.genesis();
  expect(first.id.equals(first.next('1').id)).toBe(false);
});

test('Block ordering is unimportant', () => {
  const first = HashStringBagBlock.genesis();
  expect(first.next('1').next('2').id
    .equals(first.next('2').next('1').id)).toBe(true);
});

test('Standard block 1', () => {
  expect(HashStringBagBlock.genesis('1').id.encode())
    .toBe('a4ayc/80/OGda4BO/1o/V0etpOqiLx1JwB5S3beHW0s=');
});

test('Standard block 2', () => {
  expect(HashStringBagBlock.genesis('1').next('2').id.encode())
    .toBe('P/oQriWTE9B9qtnAivWcWklJrMNY9Dzamli5TKObBoA=');
});
