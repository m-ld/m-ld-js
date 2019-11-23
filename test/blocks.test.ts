import { HashStringBagBlock } from '../src/blocks'

test('Genesis block has ID and no data', () => {
  expect(HashStringBagBlock.genesis().id).toBeTruthy;
  expect(HashStringBagBlock.genesis().data).toBeNull;
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
