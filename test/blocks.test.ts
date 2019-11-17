import { UuidBagBlock } from '../src/blocks'

test('Genesis block has random ID', () => {
  expect(`${UuidBagBlock.genesis().id}`).toMatch(
    /[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}/);
});

test('Genesis block has null data', () => {
  expect(UuidBagBlock.genesis().data).toBeNull;
});