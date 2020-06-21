import { TreeClock } from '../src/clocks'

test('Genesis has no ticks', () => {
  expect(TreeClock.GENESIS.ticks).toBe(0);
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
  expect(TreeClock.GENESIS.ticked().ticks).toBe(1);
  expect(TreeClock.GENESIS.ticked().ticked().ticks).toBe(2);
});

test('Fork tick', () => {
  expect(TreeClock.GENESIS.forked().left.ticked().ticks).toBe(1);
  expect(TreeClock.GENESIS.forked().left.ticked().ticked().ticks).toBe(2);
});

test('Fork tick tick', () => {
  expect(TreeClock.GENESIS.forked().left.forked().left.ticked().ticks).toBe(1);
  expect(TreeClock.GENESIS.forked().left.forked().left.ticked().ticked().ticks).toBe(2);
});

test('Tick fork', () => {
  const ticked = TreeClock.GENESIS.ticked();
  const fork = ticked.forked();
  expect(fork.left.ticks).toBe(1);
  expect(fork.right.ticks).toBe(1);
});

test('Tick fork tick', () => {
  const ticked = TreeClock.GENESIS.ticked();
  let { left, right } = ticked.forked();
  left = left.ticked();
  expect(left.ticks).toBe(2);
  right = right.update(left);
  expect(right.ticks).toBe(1);
  expect(right.nonIdTicks).toBe(2);
});

test('Ticks for genesis not Id', () => {
  const ticked = TreeClock.GENESIS.ticked();
  expect(ticked.nonIdTicks).toBe(null);
});

test('Ticks for forked not Id', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.nonIdTicks).toBe(null);
});

test('Ticks for forked ticked not Id', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.update(fork.right.ticked()).nonIdTicks).toBe(1);
});

test('Ticks for other Id', () => {
  let { left, right } = TreeClock.GENESIS.forked();
  left = left.ticked();
  right = right.ticked().update(left);
  left = left.ticked();
  expect(left.ticks).toBe(2);
  expect(right.ticks).toBe(1);
  expect(right.getTicks(left)).toBe(1);
});

test('Ticks for other forked Id', () => {
  let { left, right } = TreeClock.GENESIS.forked();
  left = left.ticked();
  right = right.ticked().update(left); // Get one tick before fork
  left = left.forked().left.ticked();
  expect(left.ticks).toBe(2);
  expect(right.ticks).toBe(1);
  expect(right.getTicks(left)).toBe(1);
  expect(right.update(left).getTicks(left)).toBe(2);
  expect(left.getTicks(right)).toBe(0);
  expect(left.update(right).getTicks(right)).toBe(1);
});

test('Self-update is allowed', () => {
  expect(TreeClock.GENESIS.update(TreeClock.GENESIS.ticked())
    .equals(TreeClock.GENESIS.ticked())).toBe(true);
});

test('Self-update with fork is not allowed', () => {
  expect(() => TreeClock.GENESIS.update(TreeClock.GENESIS.forked().left)).toThrowError();
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
  expect(fork.left.update(fork.right.ticked()).ticks).toBe(fork.left.ticks);
});

test('Forked right tick update from left', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.right.equals(fork.right.update(fork.left.ticked()))).toBe(false);
  expect(fork.right.update(fork.left.ticked()).ticks).toBe(fork.right.ticks);
});

test('Ticks for updated forked not Id', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.update(fork.right.ticked()).nonIdTicks).toBe(1);
});

test('Ticks for ticked updated forked not Id', () => {
  const fork = TreeClock.GENESIS.ticked().forked();
  const updatedLeft = fork.left.update(fork.right.ticked());
  expect(updatedLeft.nonIdTicks).toBe(2);
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
  expect(fork.left.ticked().mergeId(fork.right).ticks).toBe(1);
});

test('Merge forked ticked ticked', () => {
  const fork = TreeClock.GENESIS.forked();
  expect(fork.left.ticked().mergeId(fork.right.ticked()).ticks).toBe(1);
});

test('Non-contiguous merge', () => {
  const fork1 = TreeClock.GENESIS.forked(), fork2 = fork1.right.forked();
  const clock1 = fork1.left.ticked(), clock3 = fork2.right.ticked();

  expect(clock1.mergeId(clock3).ticks).toBe(1);
  expect(clock1.update(clock3).mergeId(clock3).ticks).toBe(2);
});

test('Merge all', () => {
  const fork1 = TreeClock.GENESIS.forked(), fork2 = fork1.right.forked();
  const clock1 = fork1.left.ticked(),
    clock2 = fork2.left.ticked(),
    clock3 = fork2.right.ticked(),
    clock4 = clock1.update(clock3).mergeId(clock3),
    clock5 = clock2.update(clock4).mergeId(clock4);

  expect(clock5.ticks).toBe(3);
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
  expect(left.ticks).toBe(2);
  expect(right.ticks).toBe(1);
  expect(right.getTicks(left)).toBe(1);
});

test('to string', () => {
  expect(TreeClock.GENESIS.toString()).toBe('ID');
  expect(TreeClock.GENESIS.forked().left.toString()).toBe('{ ID,  }');
  expect(TreeClock.GENESIS.ticked().forked().left.toString()).toBe('1{ ID,  }');
  expect(TreeClock.GENESIS.forked().left.ticked().toString()).toBe('{ ID1,  }');
});

test('non-ID in fork with zero ticks is still lt', () => {
  let { left: id1, right } = TreeClock.GENESIS.forked();
  const fork = right.ticked().forked();
  // New ID one level down
  const id2 = fork.right.ticked();
  expect(id1.anyLt(id2)).toBe(false);
});

test('non-ID in fork with ticks is not lt', () => {
  let { left: id1, right } = TreeClock.GENESIS.forked();
  const fork = right.ticked().forked();
  const ticked = fork.left.ticked();
  // New ID one level down, with ticked left
  const id2 = fork.right.update(ticked).ticked();
  expect(id1.anyLt(id2)).toBe(true);
});