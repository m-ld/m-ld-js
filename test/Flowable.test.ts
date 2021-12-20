import { Readable } from 'stream';
import { EventEmitter } from 'events';
import { from, merge, Subscription } from 'rxjs';
import { drain } from '../src/flowable/drain';
import { flatMap } from '../src/flowable/operators/flatMap';
import { batch } from '../src/flowable/operators/batch';
import { consume } from '../src/flowable/consume';
import { MinimalReadable } from '../src/flowable/consume/readable';
import { mergeMap } from 'rxjs/operators';
import { Bite } from '../src/flowable';

const readArrayAsync = (data: any[]) => new Readable({
  objectMode: true,
  read() {
    setImmediate(() => this.push(data.shift() ?? null));
  }
});

describe('Consuming readable', () => {
  test('completes with empty readable', done => {
    const rc = consume<true>(readArrayAsync([]));
    rc.subscribe({
      next: done /* will error on mystery item */,
      error: done,
      complete: done
    });
  });

  test('errors if Readable errors', done => {
    const r = new Readable({
      read: () => {
      }
    });
    const rc = consume<true>(r);
    rc.subscribe({
      next: done /* will error on mystery item */,
      error(err) {
        expect(err.message).toBe('bang');
        done();
      },
      complete: () => done('Unexpected complete')
    });
    r.destroy(new Error('bang'));
  });

  test('completes after bites done', async () => {
    const rc = consume<number>(readArrayAsync([0]));
    const willComplete = new Promise<void>((resolve, reject) => {
      rc.subscribe({
        next: async ({ next }) => {
          await expect(Promise.race([willComplete, true])).resolves.toBe(true);
          next();
        },
        error: reject,
        complete: resolve
      });
    });
    await expect(willComplete).resolves.toBeUndefined();
  });

  test('flows at pace of slowest consumer', async () => {
    const rc = consume<number>(readArrayAsync([0, 1]));
    const trigger = new EventEmitter;
    const values: number[] = [];
    const willCompleteOne = new Promise<void>(resolve => {
      rc.subscribe({
        next: ({ value, next }) => {
          values.push(value);
          next();
          setImmediate(() => trigger.emit('fire'));
        },
        complete: resolve
      });
    });
    rc.subscribe({
      next: ({ value, next }) => {
        values.push(value);
        trigger.once('fire', next);
      }
    });
    await willCompleteOne;
    expect(values).toEqual([0, 0, 1, 1]);
  });

  test('unsubscribe does not stop flow', async () => {
    const rc = consume<number>(readArrayAsync([0, 1]));
    const values: number[] = [];
    const willCompleteOne = new Promise<void>(resolve => {
      rc.subscribe({
        next: ({ value, next }) => {
          values.push(value);
          next();
        },
        complete: resolve
      });
    });
    const twoSubs = rc.subscribe({
      next: ({ value }) => {
        values.push(value);
        twoSubs.unsubscribe();
      }
    });
    await willCompleteOne;
    expect(values).toEqual([0, 0, 1]);
  });

  test('guards against premature end while reading', done => {
    // noinspection JSPotentiallyInvalidUsageOfThis
    const naughtyReadable = new class extends EventEmitter implements MinimalReadable<number> {
      readable = true;
      buffer = [0, 1];
      constructor() {
        super();
        this.on('newListener', (event, handler) => {
          if (event === 'readable')
            handler();
        });
      }
      read() {
        const n = this.buffer.shift();
        // Naughty! this should only emit when n is null
        if (!this.buffer.length)
          this.emit('end');
        return n ?? null;
      }
      destroyed = false;
      destroy = () => {
      };
    };
    consume(naughtyReadable).subscribe(({ value, next }) => {
      if (value == 1)
        done();
      next();
    });
  });

  test('guards against premature end before subscribing', done => {
    // AsyncIterators with autoStart can emit 'end' before they have a 'readable' listener
    const mockReadable = new class extends EventEmitter implements MinimalReadable<number> {
      readable = false;
      read = () => null;
      destroyed = false;
      destroy = () => {
      };
    };
    const rc = consume(mockReadable);
    setImmediate(() => mockReadable.emit('end'));
    setImmediate(() => rc.subscribe({ complete: done }));
  });

  test('reads if readable before subscribe', done => {
    // AsyncIterators with autoStart can emit 'end' before they have a 'readable' listener
    const mockReadable = new class extends EventEmitter implements MinimalReadable<number> {
      readable = true; // Already readable
      read = () => {
        setImmediate(() => this.emit('end'));
        return null;
      };
      destroyed = false;
      destroy = () => {
      };
    };
    consume(mockReadable).subscribe({ complete: done });
  });

  test('calls destroy when exhausted', done => {
    let buffer = [0, 1];
    consume(new Readable({
      objectMode: true,
      read() {
        this.push(buffer.shift() ?? null);
      },
      destroy(error: Error | null, callback: (error: (Error | null)) => void) {
        callback(error);
        done(error);
      }
    })).subscribe({
      next: ({ next }) => next()
    });
  });

  test('calls destroy if prematurely unsubscribed', done => {
    let buffer = [0, 1];
    const subs: Subscription = consume(new Readable({
      objectMode: true,
      read() {
        this.push(buffer.shift() ?? null);
      },
      destroy(error: Error | null, callback: (error: (Error | null)) => void) {
        callback(error);
        done(error);
      }
    })).subscribe(() => setImmediate(() => subs.unsubscribe()));
  });

  test('errors if the readable is prematurely destroyed', done => {
    const source = readArrayAsync([0, 1]);
    consume(source).subscribe({
      next() {
        source.destroy();
      },
      error(err) {
        expect(err instanceof Error).toBe(true);
        done();
      }
    });
  });
});

describe('Consuming iterable', () => {
  test('completes with empty readable', done => {
    const rc = consume([]);
    rc.subscribe({
      next: done /* will error on mystery item */,
      error: done,
      complete: done
    });
  });

  test('consumes synchronously', () => {
    const rc = consume([0, 1]);
    const values: number[] = [];
    rc.subscribe({
      next: ({ value, next }) => {
        values.push(value);
        next();
      }
    });
    expect(values).toEqual([0, 1]);
  });

  test('consumes asynchronously', done => {
    const rc = consume([0, 1]);
    const values: number[] = [];
    rc.subscribe({
      next: ({ value, next }) => {
        values.push(value);
        setImmediate(() => next());
      },
      complete: () => {
        expect(values).toEqual([0, 1]);
        done();
      }
    });
  });
});

////////////////////////////////////////////////////////////////////////////////
// The following tests use the drain utility, which also tests the flowable method

describe('Consuming promise', () => {
  test('consumes resolved promise', async () => {
    const rc = consume(Promise.resolve(0));
    await expect(drain(rc)).resolves.toEqual([0]);
  });

  test('consumes immediate promise', async () => {
    const rc = consume(new Promise<number>(resolve =>
      setImmediate(() => resolve(0))));
    await expect(drain(rc)).resolves.toEqual([0]);
  });
});

describe('Consuming observable', () => {
  test('consumes observable', async () => {
    const rc = consume(from([0, 1]));
    await expect(drain(rc)).resolves.toEqual([0, 1]);
  });

  test('consuming observable waits for next', done => {
    const rc = consume(from([0, 1]));
    let nextCalls = 0;
    rc.subscribe({
      next: () => {
        nextCalls++;
        setImmediate(() => {
          // Next value does not arrive until we request it
          expect(nextCalls).toBe(1);
          done();
        });
      }
    });
  });
});

describe('Utilities', () => {
  test('flatMap concatenates inputs', async () => {
    const rc = consume([0, 1]).pipe(
      flatMap(n => consume([n * 2, n * 2 + 1])));
    await expect(drain(rc)).resolves.toEqual([0, 1, 2, 3]);
  });

  test('batch empty', async () => {
    const rc = consume([]).pipe(batch(2));
    await expect(drain(rc)).resolves.toEqual([]);
  });

  test('batch one by one', async () => {
    const rc = consume([0, 1]).pipe(batch(1));
    await expect(drain(rc)).resolves.toEqual([[0], [1]]);
  });

  test('batch two by two', async () => {
    const rc = consume([0, 1, 2, 3]).pipe(batch(2));
    await expect(drain(rc)).resolves.toEqual([[0, 1], [2, 3]]);
  });

  test('batch insufficient', async () => {
    const rc = consume([0]).pipe(batch(2));
    await expect(drain(rc)).resolves.toEqual([[0]]);
  });

  test('batch irregular', async () => {
    const rc = consume([0, 1, 2]).pipe(batch(2));
    await expect(drain(rc)).resolves.toEqual([[0, 1], [2]]);
  });

  test('batch, promise and merge', async () => {
    const data1 = readArrayAsync([[0, 1]]);
    const data2 = new Promise<number[]>(resolve =>
      setImmediate(() => resolve([2, 3])));
    const rc1 = from(data1).pipe(mergeMap(data => consume(data)));
    const rc2 = from(data2).pipe(mergeMap(data => consume(data).pipe(
      batch(1),
      mergeMap(({ value, next }) => new Promise<Bite<number>>(resolve =>
        setImmediate(() => resolve({ value: value[0], next })))))));
    await expect(drain(merge(rc1, rc2))).resolves.toEqual([0, 1, 2, 3]);
  });
});