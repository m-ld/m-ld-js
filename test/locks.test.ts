// noinspection ES6MissingAwait

import { LockManager } from '../src/engine/locks';

describe('Sharable lock', () => {
  let ops: number[]; // Used to assert concurrent operation ordering
  const pushOp = async (n: number) => ops.push(n);

  beforeEach(() => {
    ops = [];
  });

  test('exclusive ops execute immediately', async () => {
    const lock = new LockManager<'it'>();
    await lock.exclusive('it', 'test', async () => {
      expect(lock.state('it')).toBe('exclusive');
      await pushOp(1);
      await pushOp(2);
      expect(lock.state('it')).toBe('exclusive');
    });
    expect(ops).toEqual([1, 2]);
    await lock.open('it');
    expect(lock.state('it')).toBe('open');
    await lock.exclusive('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    expect(ops).toEqual([1, 2, 3, 4]);
  });

  test('three exclusive ops execute immediately', async () => {
    const lock = new LockManager<'it'>();
    await lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    expect(ops).toEqual([1, 2]);
    await lock.exclusive('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    expect(ops).toEqual([1, 2, 3, 4]);
    await lock.exclusive('it', 'test', async () => {
      await pushOp(5);
      await pushOp(6);
    });
    expect(ops).toEqual([1, 2, 3, 4, 5, 6]);
  });

  test('exclusive ops do not share', async () => {
    const lock = new LockManager<'it'>();
    lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    await lock.exclusive('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    expect(ops).toEqual([1, 2, 3, 4]);
  });

  test('three exclusive ops do not share', async () => {
    const lock = new LockManager<'it'>();
    lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    lock.exclusive('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    await lock.exclusive('it', 'test', async () => {
      await pushOp(5);
      await pushOp(6);
    });
    expect(ops).toEqual([1, 2, 3, 4, 5, 6]);
  });

  test('sync throw in exclusive lock rejects', async () => {
    const lock = new LockManager<'it'>();
    lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    await expect(lock.exclusive('it', 'test', () => {
      throw 'bang';
    })).rejects.toBe('bang');
  });

  test('async throw in exclusive lock rejects', async () => {
    const lock = new LockManager<'it'>();
    lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    await expect(lock.exclusive('it', 'test', () => {
      return Promise.reject('bang');
    })).rejects.toBe('bang');
  });

  test('shared ops execute immediately', async () => {
    const lock = new LockManager<'it'>();
    await lock.share('it', 'test', async () => {
      expect(lock.state('it')).toBe('shared');
      await pushOp(1);
      await pushOp(2);
    });
    expect(ops).toEqual([1, 2]);
    await lock.open('it');
    expect(lock.state('it')).toBe('open');
    await lock.share('it', 'test', async () => {
      expect(lock.state('it')).toBe('shared');
      await pushOp(3);
      await pushOp(4);
    });
    expect(ops).toEqual([1, 2, 3, 4]);
  });

  test('shared ops do share', async () => {
    const lock = new LockManager<'it'>();
    lock.share('it', 'test', async () => {
      expect(lock.state('it')).toBe('shared');
      await pushOp(1);
      await pushOp(2);
    });
    await lock.share('it', 'test', async () => {
      expect(lock.state('it')).toBe('shared');
      await pushOp(3);
      await pushOp(4);
    });
    expect(ops).toEqual([1, 3, 2, 4]);
  });

  test('shared ops can recursively share (sync)', async () => {
    const lock = new LockManager<'it'>();
    await lock.share('it', 'test', async () => {
      // NOTE this re-share is done immediately
      await lock.share('it', 'test', async () => {
        await pushOp(1);
        await pushOp(2);
      });
      await pushOp(3);
      await pushOp(4);
    });
    expect(ops).toEqual([1, 2, 3, 4]);
  });

  test('shared ops can recursively share (async)', async () => {
    const lock = new LockManager<'it'>();
    await lock.share('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
      // Re-share after awaits
      await lock.share('it', 'test', async () => {
        await pushOp(3);
        await pushOp(4);
      });
    });
    expect(ops).toEqual([1, 2, 3, 4]);
  });

  test('shared ops execute even if something rejects', async () => {
    const lock = new LockManager<'it'>();
    lock.share('it', 'test', async () => {
      await pushOp(1);
      throw 'bang';
    }).catch(() => {
    });
    await lock.share('it', 'test', async () => {
      await pushOp(2);
      await pushOp(3);
      expect(lock.state('it')).toBe('shared');
    });
    expect(ops).toEqual([1, 2, 3]);
  });

  test('shared ops resolve as soon as they can', async () => {
    const lock = new LockManager<'it'>();
    await Promise.all([
      lock.share('it', 'test', async () => {
        await pushOp(1);
        await pushOp(2);
      }).then(() => {
        // Some of the other op may have happened, but not all of it
        expect(ops.slice(0, 3)).toEqual([1, 3, 2]);
        expect(ops.length).toBeLessThan(1000);
      }),
      lock.share('it', 'test', async () => {
        for (let i = 3; i < 1000; i++)
          await pushOp(i);
      })
    ]);
  });

  test('shared ops do not share with exclusive ops', async () => {
    const lock = new LockManager<'it'>();
    lock.share('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    lock.share('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    await lock.exclusive('it', 'test', async () => {
      await pushOp(5);
      await pushOp(6);
    });
    expect(ops).toEqual([1, 3, 2, 4, 5, 6]);
  });

  test('exclusive ops do not share with shared ops', async () => {
    const lock = new LockManager<'it'>();
    lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    lock.share('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    await lock.share('it', 'test', async () => {
      await pushOp(5);
      await pushOp(6);
    });
    expect(ops).toEqual([1, 2, 3, 5, 4, 6]);
  });

  test('exclusive ops follow shared ops in call order', async () => {
    const lock = new LockManager<'it'>();
    lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    lock.share('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    lock.share('it', 'test', async () => {
      await pushOp(5);
      await pushOp(6);
    });
    await lock.exclusive('it', 'test', async () => {
      await pushOp(7);
      await pushOp(8);
    });
    expect(ops).toEqual([1, 2, 3, 5, 4, 6, 7, 8]);
  });

  test('shared ops follow exclusive ops in call order', async () => {
    const lock = new LockManager<'it'>();
    lock.share('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    lock.share('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    lock.exclusive('it', 'test', async () => {
      await pushOp(5);
      await pushOp(6);
    });
    await lock.share('it', 'test', async () => {
      await pushOp(7);
      await pushOp(8);
    });
    expect(ops).toEqual([1, 3, 2, 4, 5, 6, 7, 8]);
  });

  test('shared ops can be extended', async () => {
    const lock = new LockManager<'it'>();
    await lock.share('it', 'test', async () => {
      await lock.extend('it', 'test', (async () => {
        await pushOp(1);
        await pushOp(2);
      })());
      await pushOp(3);
      await pushOp(4);
    });
    expect(ops).toEqual([1, 2, 3, 4]);
  });

  test('exclusive ops can be extended', async () => {
    const lock = new LockManager<'it'>();
    await lock.exclusive('it', 'test', async () => {
      lock.extend('it', 'test', (async () => {
        await pushOp(1);
        await pushOp(2);
      })());
      await pushOp(3);
      await pushOp(4);
    });
    expect(ops).toEqual([1, 3, 2, 4]);
  });

  test('running op is extended', async () => {
    const lock = new LockManager<'it'>();
    lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await lock.extend('it', 'test', (async () => {
        await pushOp(2);
      })());
      await pushOp(3);
    });
    await lock.exclusive('it', 'test', async () => {
      await pushOp(4);
    });
    expect(ops).toEqual([1, 2, 3, 4]);
  });

  test('cannot extend if no running op', async () => {
    const lock = new LockManager<'it'>();
    await lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    await lock.open('it');
    expect(() => lock.extend('it', 'test', (async () => {
      await pushOp(3);
      await pushOp(4);
    })())).toThrowError();
  });

  test('failed exclusive op is reported', async () => {
    const lock = new LockManager<'it'>();
    await expect(lock.exclusive('it', 'test', async () => {
      await pushOp(1);
      throw new Error();
    })).rejects.toThrowError();
  });

  test('failed shared op is reported', async () => {
    const lock = new LockManager<'it'>();
    await expect(lock.share('it', 'test', async () => {
      await pushOp(1);
      throw new Error();
    })).rejects.toThrowError();
  });

  test('failed ops do not kill shared lock', async () => {
    const lock = new LockManager<'it'>();
    lock.share('it', 'test', async () => {
      await pushOp(1);
      await pushOp(2);
    });
    await expect(lock.share('it', 'test', async () => {
      await pushOp(3);
      throw new Error();
    })).rejects.toThrowError();
    await lock.exclusive('it', 'test', async () => {
      await pushOp(5);
      await pushOp(6);
    });
    expect(ops).toEqual([1, 3, 2, 5, 6]);
  });

  test('sync tasks execute immediately', async () => {
    const lock = new LockManager<'it'>();
    lock.share('it', 'test', async () => {
      ops.push(1);
      ops.push(2);
    });
    lock.share('it', 'test', async () => {
      await pushOp(3);
      await pushOp(4);
    });
    await lock.exclusive('it', 'test', async () => {
      await pushOp(5);
      await pushOp(6);
    });
    expect(ops).toEqual([1, 2, 3, 4, 5, 6]);
  });
});