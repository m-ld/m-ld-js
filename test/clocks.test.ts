import { GlobalClock, TreeClock } from '../src/engine/clocks';

describe('Tree clock', () => {
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

  test('Genesis set tick', () => {
    expect(TreeClock.GENESIS.ticked(0).ticks).toBe(0);
    expect(TreeClock.GENESIS.ticked().ticked(0).ticks).toBe(0);
    expect(() => TreeClock.GENESIS.ticked(-1)).toThrowError();
  });

  test('Fork tick', () => {
    expect(TreeClock.GENESIS.forked().left.ticked().ticks).toBe(1);
    expect(TreeClock.GENESIS.forked().left.ticked().ticked().ticks).toBe(2);
  });

  test('Fork tick tick', () => {
    expect(TreeClock.GENESIS.forked().left.forked().left.ticked().ticks).toBe(1);
    expect(TreeClock.GENESIS.forked().left.forked().left.ticked().ticked().ticks).toBe(2);
  });

  test('Fork tick untick', () => {
    expect(TreeClock.GENESIS.ticked().forked().left.ticked(1).ticks).toBe(1);
  });

  test('Fork untick back past fork', () => {
    const tickedFork = TreeClock.GENESIS.ticked().forked().left;
    expect(tickedFork.ticked(0).equals(TreeClock.GENESIS)).toBe(true);
  });

  test('Untick to zero retains forked ID', () => {
    const tickedFork = TreeClock.GENESIS.forked().left.ticked();
    expect(tickedFork.ticked(0).equals(TreeClock.GENESIS)).toBe(false);
    expect(tickedFork.ticked(0).hash()).toBe(TreeClock.GENESIS.hash());
  });

  test('Untick does not affect non-ID leaf', () => {
    // This was a defect in which a left branch could be un-ticked without having an ID
    let { left, right } = TreeClock.GENESIS.ticked().forked();
    right = right.update(left.ticked(10));
    expect(right.ticked(5).ticks).toBe(5);
  });

  test('Untick does not affect non-ID fork', () => {
    let { left, right } = TreeClock.GENESIS.ticked().forked();
    // This time the left is a fork with no ID in it
    right = right.update(left.ticked(10).forked().left.ticked(2));
    expect(right.ticked(5).ticks).toBe(5);
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

  test('Less than genesis self', () => {
    expect(TreeClock.GENESIS.anyNonIdLt(TreeClock.GENESIS)).toBe(false);
  });

  test('Less than forked', () => {
    const fork = TreeClock.GENESIS.forked();
    expect(fork.left.anyNonIdLt(fork.right)).toBe(false);
  });

  test('Less than forked right ticked', () => {
    const { left, right } = TreeClock.GENESIS.forked();
    expect(left.anyNonIdLt(right.ticked())).toBe(false);
  });

  test('Less than self-updated ticked', () => {
    let { left, right } = TreeClock.GENESIS.forked();
    expect(right.anyNonIdLt(right.update(left.ticked()))).toBe(true);
  });

  test('Less than self-updated forked ticked', () => {
    let { left: next, right: id3 } = TreeClock.GENESIS.forked();
    let { left: id1, right: id2 } = next.forked();
    id3 = id3.ticked(); // {0,0},*3
    id1 = id1.update(id3).ticked(); // {*1,0},3

    id2 = id2.update(id1).ticked(); // {1,*1},3
    id1 = id1.update(id2).update(id3.ticked()).ticked(); // {*2,1},2
    expect(id2.getTicks(id1)).toBe(1);
    expect(id2.anyNonIdLt(id2.update(id1))).toBe(true);
  });

  test('Less than twice forked ticked', () => {
    const fork = TreeClock.GENESIS.forked();
    const rightFork = fork.right.forked();
    expect(fork.left.anyNonIdLt(rightFork.left.update(rightFork.right.ticked()))).toBe(true);
  });

  test('Less than thrice forked ticked', () => {
    const fork = TreeClock.GENESIS.forked();
    const rightFork = fork.right.forked();
    const leftFork = fork.left.forked();
    expect(leftFork.left.anyNonIdLt(rightFork.left.update(rightFork.right.ticked()))).toBe(true);
  });

  test('Less than compensating ticks', () => {
    const fork = TreeClock.GENESIS.forked();
    const rightFork = fork.right.forked();
    const leftFork = fork.left.forked();
    expect(leftFork.left.update(leftFork.right.ticked())
      .anyNonIdLt(rightFork.left.update(rightFork.right.ticked()))).toBe(true);
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

  test('Get ticks for other just forked ID', () => {
    let { left, right } = TreeClock.GENESIS.forked();
    right = right.ticked();
    expect(right.getTicks(left)).toBe(0);
  });

  test('to string', () => {
    expect(TreeClock.GENESIS.toString()).toBe('[]');
    expect(TreeClock.GENESIS.forked().left.toString()).toBe('[[],0]');
    expect(TreeClock.GENESIS.forked().left.ticked().toString()).toBe('[[1],0]');
    expect(TreeClock.GENESIS.ticked().forked().left.toString()).toBe('[1,[],0]');
  });

  test('to JSON', () => {
    expect(TreeClock.GENESIS.toJSON()).toEqual([]);
    expect(TreeClock.GENESIS.forked().left.toJSON()).toEqual([[], 0]);
    expect(TreeClock.GENESIS.forked().left.ticked().toJSON()).toEqual([[1], 0]);
    expect(TreeClock.GENESIS.ticked().forked().left.toJSON()).toEqual([1, [], 0]);
  });

  test('from JSON', () => {
    expect(TreeClock.fromJson([])
      .equals(TreeClock.GENESIS)).toBe(true);
    expect(TreeClock.fromJson([[], 0])
      .equals(TreeClock.GENESIS.forked().left)).toBe(true);
    expect(TreeClock.fromJson([[1], 0])
      .equals(TreeClock.GENESIS.forked().left.ticked())).toBe(true);
    expect(TreeClock.fromJson([1, [], 0])
      .equals(TreeClock.GENESIS.ticked().forked().left)).toBe(true);
  });

  test('non-ID in fork with zero ticks is still lt', () => {
    let { left: id1, right } = TreeClock.GENESIS.forked();
    const fork = right.ticked().forked();
    // New ID one level down
    const id2 = fork.right.ticked();
    // { ID, } !< { , 1{ , ID1 }}
    expect(id1.anyNonIdLt(id2)).toBe(false);
  });

  test('non-ID in fork with ticks is not lt', () => {
    let { left: id1, right } = TreeClock.GENESIS.forked();
    const fork = right.ticked().forked();
    const ticked = fork.left.ticked();
    // New ID one level down, with ticked left
    const id2 = fork.right.update(ticked).ticked();
    // { ID, } < { , 1{ 1, ID1 } }
    expect(id1.anyNonIdLt(id2)).toBe(true);
  });

  test('forked ID is not lt unforked', () => {
    // This tests an edge case generated by
    // https://github.com/m-ld/m-ld-js/commit/d569c201535359fb2236f26876240e10915fa986
    let { left, right } = TreeClock.GENESIS.forked();
    const ticked = left.ticked();
    const id2 = right.update(ticked);
    const id1 = ticked.forked().left;
    // { 1{ ID, }, } !< { 1, ID }
    expect(id1.anyNonIdLt(id2)).toBe(false);
  });

  test('forked ID with ticks is not lt unforked', () => {
    let { left, right } = TreeClock.GENESIS.forked();
    const ticked = left.ticked();
    const id2 = right.update(ticked);
    const fork = ticked.forked();
    const id1 = fork.left.update(fork.right.ticked());
    // { 1{ ID, 1 }, } !< { 1, ID }
    expect(id1.anyNonIdLt(id2)).toBe(false);
  });

  test('update from global clock', () => {
    const { left, right } = TreeClock.GENESIS.forked();
    expect(right.update(
      GlobalClock.GENESIS.update(left.ticked())).toJSON()).toEqual([1, []]);
  });

  test('hash is stable', () => {
    const one = TreeClock.GENESIS.ticked(), two = TreeClock.GENESIS.ticked();
    expect(one.hash()).toEqual(two.hash());
  });

  test('hash is unique', () => {
    let { left, right } = TreeClock.GENESIS.forked();
    expect(left.ticked().hash()).not.toEqual(right.ticked().hash());
    expect(left.ticked().hash()).not.toEqual(left.ticked().ticked().hash());
  });

  test('hash ignores unticked genesis fork', () => {
    let { left, right } = TreeClock.GENESIS.forked();
    expect(left.hash()).toEqual(right.hash());
  });

  test('hash ignores unticked fork of ticked', () => {
    let { left, right } = TreeClock.GENESIS.ticked().forked();
    expect(left.hash()).toEqual(right.hash());
  });

  test('hash ignores deeply unticked forks', () => {
    let { left, right } = TreeClock.GENESIS.ticked().forked();
    right = right.forked().right;
    expect(left.hash()).toEqual(right.hash());
  });

  test('big clock hash is short', () => {
    let time = TreeClock.GENESIS;
    for (let i = 0; i < 10; i++) {
      const { left, right } = time.forked();
      time = right.update(left.ticked(0xFFFFFFFF));
      expect(time.hash().length).toBeLessThanOrEqual(22);
    }
  });
});

describe('Global clock', () => {
  test('Genesis has genesis TID', () => {
    const gwc = GlobalClock.GENESIS;
    expect(new Set(gwc.tids())).toEqual(new Set([TreeClock.GENESIS.hash()]));
    expect(gwc.tid(TreeClock.GENESIS.ticked())).toBe(TreeClock.GENESIS.hash());
  });

  test('Update no-op does not change tids', () => {
    const gwc = GlobalClock.GENESIS.update(TreeClock.GENESIS);
    expect(new Set(gwc.tids())).toEqual(new Set([TreeClock.GENESIS.hash()]));
  });

  test('Ticked changes tids', () => {
    let time = TreeClock.GENESIS.ticked();
    const gwc = GlobalClock.GENESIS.update(time).update(time = time.ticked());
    expect(new Set(gwc.tids())).toEqual(new Set([time.hash()]));
    expect(gwc.tid(TreeClock.GENESIS.ticked())).toBe(time.hash());
  });

  test('Just-forked have same hash', () => {
    const time = TreeClock.GENESIS;
    const gwc = GlobalClock.GENESIS.update(time.forked().left);
    expect(new Set(gwc.tids())).toEqual(new Set([time.hash()]));
    expect(gwc.tid(time.forked().left.ticked())).toBe(time.hash());
    expect(gwc.tid(time.forked().right.ticked())).toBe(time.hash());
  });

  test('Forked ticked has both hashes', () => {
    let { left, right } = TreeClock.GENESIS.forked();
    left = left.ticked();
    const gwc = GlobalClock.GENESIS.update(left).update(right);
    expect(new Set(gwc.tids())).toEqual(new Set([left.hash(), right.hash()]));
    expect(gwc.tid(left.ticked())).toBe(left.hash());
    expect(gwc.tid(right.ticked())).toBe(right.hash());
  });

  test('Forked both ticked has both hashes', () => {
    let { left, right } = TreeClock.GENESIS.forked();
    left = left.ticked();
    right = right.ticked(2);
    const gwc = GlobalClock.GENESIS.update(left).update(right);
    expect(new Set(gwc.tids())).toEqual(new Set([left.hash(), right.hash()]));
    expect(gwc.tid(left.ticked())).toBe(left.hash());
    expect(gwc.tid(right.ticked())).toBe(right.hash());
  });
});