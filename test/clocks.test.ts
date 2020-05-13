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
  const fork = ticked.forked();
  expect(fork.left.getTicks()).toBe(1);
  expect(fork.right.getTicks()).toBe(1);
});

test('Tick fork tick', () => {
  const ticked = TreeClock.GENESIS.ticked();
  let { left, right } = ticked.forked();
  left = left.ticked();
  expect(left.getTicks()).toBe(2);
  right = right.update(left);
  expect(right.getTicks()).toBe(1);
  expect(right.getTicks(false)).toBe(2);
});

test('Ticks for genesis not Id', () => {
  const ticked = TreeClock.GENESIS.ticked();
  expect(ticked.getTicks(false)).toBe(null);
});

test('Ticks for forked not Id', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.getTicks(false)).toBe(0);
});

test('Ticks for other Id', () => {
  let { left, right } = TreeClock.GENESIS.forked();
  left = left.ticked();
  right = right.ticked().update(left);
  left = left.ticked();
  expect(left.getTicks()).toBe(2);
  expect(right.getTicks()).toBe(1);
  expect(right.getTicks(left)).toBe(1);
});

test('Ticks for other forked Id', () => {
  let { left, right } = TreeClock.GENESIS.forked();
  left = left.ticked();
  right = right.ticked().update(left); // Get one tick before fork
  left = left.forked().left.ticked();
  expect(left.getTicks()).toBe(2);
  expect(right.getTicks()).toBe(1);
  expect(right.getTicks(left)).toBe(1);
  expect(right.update(left).getTicks(left)).toBe(2);
  expect(left.getTicks(right)).toBe(0);
  expect(left.update(right).getTicks(right)).toBe(1);
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

test('Less than forked right ticked', () => {
  const { left, right } = TreeClock.GENESIS.forked();
  expect(left.anyLt(right.ticked())).toBe(false);
});

test('Less than self-updated ticked', () => {
  let { left, right } = TreeClock.GENESIS.forked();
  expect(right.anyLt(right.update(left.ticked()))).toBe(true);
});

test('Less than self-updated forked ticked', () => {
  let { left: next, right: id3 } = TreeClock.GENESIS.forked();
  let { left: id1, right: id2 } = next.forked();
  id3 = id3.ticked(); // {0,0},*3
  id1 = id1.update(id3).ticked(); // {*1,0},3

  id2 = id2.update(id1).ticked(); // {1,*1},3
  id1 = id1.update(id2).update(id3.ticked()).ticked(); // {*2,1},2
  expect(id2.getTicks(id1)).toBe(1);
  expect(id2.anyLt(id2.update(id1))).toBe(true);
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

test('Get ticks for other ID', () => {
  let { left, right } = TreeClock.GENESIS.forked();
  left = left.ticked();
  right = right.ticked().update(left);
  left = left.ticked();
  expect(left.getTicks()).toBe(2);
  expect(right.getTicks()).toBe(1);
  expect(right.getTicks(left)).toBe(1);
});