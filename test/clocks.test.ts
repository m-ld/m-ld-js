import { TreeClock } from '../src/clocks'

test('Genesis has no ticks', () => {
  expect(TreeClock.GENESIS.getTicks()).toBe(0);
});

test('Fork not equal', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.equals(fork.right)).toBe(false);
});

test('Fork fork not equal', () => {
  const fork = TreeClock.GENESIS.forked().left.forked();
  expect(fork.left.equals(fork.right)).toBe(false);
});

test('Genesis tick', () => {
  expect(TreeClock.GENESIS.ticked().getTicks()).toBe(1);
  expect(TreeClock.GENESIS.ticked().ticked().getTicks()).toBe(2);
});

test('Fork tick', () => {
  expect(TreeClock.GENESIS.forked().left.ticked().getTicks()).toBe(1);
  expect(TreeClock.GENESIS.forked().left.ticked().ticked().getTicks()).toBe(2);
});

test('Fork tick tick', () => {
  expect(TreeClock.GENESIS.forked().left.forked().left.ticked().getTicks()).toBe(1);
  expect(TreeClock.GENESIS.forked().left.forked().left.ticked().ticked().getTicks()).toBe(2);
});

test('Tick fork', () => {
  const ticked = TreeClock.GENESIS.ticked();
  const forked = ticked.forked();
  expect(forked.left.getTicks()).toBe(1);
  expect(forked.right.getTicks()).toBe(1);
});

test('Tick fork tick', () => {
  const ticked = TreeClock.GENESIS.ticked();
  const forked = ticked.forked();
  expect(forked.left.ticked().getTicks()).toBe(2);
  expect(forked.right.getTicks()).toBe(1);
});

test('Ticks for genesis not Id', () => {
  const ticked = TreeClock.GENESIS.ticked();
  expect(ticked.getTicks(false)).toBe(null);
});

test('Ticks for forked not Id', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.getTicks(false)).toBe(0);
});

test('Bad update', () => {
  expect(() => TreeClock.GENESIS.update(TreeClock.GENESIS.ticked())).toThrowError();
});

test('Forked no update', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.equals(fork.left.update(fork.right))).toBe(true);
});

test('Forked left tick no update', () => {
  const fork = TreeClock.GENESIS.forked();
  const newLeft = fork.left.ticked();
  expect(newLeft.equals(newLeft.update(fork.right))).toBe(true);
});

test('Forked right tick no update', () => {
  const fork = TreeClock.GENESIS.forked();
  const newRight = fork.right.ticked();
  expect(newRight.equals(newRight.update(fork.left))).toBe(true);
});

test('Forked left tick update from right', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.equals(fork.left.update(fork.right.ticked()))).toBe(false);
  expect(fork.left.update(fork.right.ticked()).getTicks()).toBe(fork.left.getTicks());
});

test('Forked right tick update from left', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.right.equals(fork.right.update(fork.left.ticked()))).toBe(false);
  expect(fork.right.update(fork.left.ticked()).getTicks()).toBe(fork.right.getTicks());
});

test('Ticks for updated forked not Id', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.update(fork.right.ticked()).getTicks(false)).toBe(1);
});

test('Ticks for ticked updated forked not Id', () => {
  const fork = TreeClock.GENESIS.ticked().forked();
  const updatedLeft = fork.left.update(fork.right.ticked());
  expect(updatedLeft.getTicks(false)).toBe(2);
});

test('No-op merge', () => {
  expect(TreeClock.GENESIS.equals(TreeClock.GENESIS.mergeId(TreeClock.GENESIS))).toBe(true);
});

test('Merge forked', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(TreeClock.GENESIS.equals(fork.left.mergeId(fork.right))).toBe(true);
});

test('Merge ticked forked', () => {
  const ticked = TreeClock.GENESIS.ticked();
  const fork = ticked.forked();
  expect(ticked.equals(fork.left.mergeId(fork.right))).toBe(true);
});

test('Merge forked ticked', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.ticked().mergeId(fork.right).getTicks()).toBe(1);
});

test('Merge forked ticked ticked', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.ticked().mergeId(fork.right.ticked()).getTicks()).toBe(1);
});

test('Non-contiguous merge', () => {
  const fork1 = TreeClock.GENESIS.forked(), fork2 = fork1.right.forked();
  const clock1 = fork1.left.ticked(), clock3 = fork2.right.ticked();

  expect(clock1.mergeId(clock3).getTicks()).toBe(1);
  expect(clock1.update(clock3).mergeId(clock3).getTicks()).toBe(2);
});

test('Merge all', () => {
  const fork1 = TreeClock.GENESIS.forked(), fork2 = fork1.right.forked();
  const clock1 = fork1.left.ticked(),
    clock2 = fork2.left.ticked(),
    clock3 = fork2.right.ticked(),
    clock4 = clock1.update(clock3).mergeId(clock3),
    clock5 = clock2.update(clock4).mergeId(clock4);

  expect(clock5.getTicks()).toBe(3);
});

test('Less than genesis self', () => {
  expect(TreeClock.GENESIS.anyLt(TreeClock.GENESIS)).toBe(false);
});

test('Less than forked', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.anyLt(fork.right)).toBe(false);
});

test('Less than forked ticked', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.anyLt(fork.right.ticked())).toBe(false);
});

test('Less than twice forked ticked', () => {
  const fork = TreeClock.GENESIS.forked();
  const rightFork = fork.right.forked();
  expect(fork.left.anyLt(rightFork.left.update(rightFork.right.ticked()))).toBe(true);
});

test('Less than thrice forked ticked', () => {
  const fork = TreeClock.GENESIS.forked();
  const rightFork = fork.right.forked();
  const leftFork = fork.left.forked();
  expect(leftFork.left.anyLt(rightFork.left.update(rightFork.right.ticked()))).toBe(true);
});

test('Less than compensating ticks', () => {
  const fork = TreeClock.GENESIS.forked();
  const rightFork = fork.right.forked();
  const leftFork = fork.left.forked();
  expect(leftFork.left.update(leftFork.right.ticked())
    .anyLt(rightFork.left.update(rightFork.right.ticked()))).toBe(true);
});