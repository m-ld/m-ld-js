import { RemoteOperations } from '../src/engine/dataset/RemoteOperations';
import { OperationMessage } from '../src/engine/index';
import { defer, EMPTY, find, firstValueFrom, of, ReplaySubject, Subject, throwError } from 'rxjs';
import { MockProcess, mockRemotes } from './testClones';
import { TreeClock } from '../src/engine/clocks';
import { take, toArray } from 'rxjs/operators';

describe('Remote operations relay', () => {
  test('relays nothing before attach', done => {
    const remote = new MockProcess(TreeClock.GENESIS);
    const operations = new Subject<OperationMessage>();
    const ro = new RemoteOperations(mockRemotes(operations));
    ro.receiving.subscribe({
      next: () => done('unexpected op'),
      complete: done,
      error: done
    });
    operations.next(remote.sentOperation({}, {}));
    setTimeout(() => ro.close(), 1);
  });

  test('attaches with no rev-ups', done => {
    const remote = new MockProcess(TreeClock.GENESIS);
    const sentOp = remote.sentOperation({}, {});
    const operations = new Subject<OperationMessage>();
    const ro = new RemoteOperations(mockRemotes(operations));
    ro.receiving.subscribe(([op, period]) => {
      expect(period).toBe(0);
      expect(op).toBe(sentOp);
      expect(ro.outdated.value).toBe(false);
      done();
    });
    ro.attach(EMPTY).then(() => operations.next(sentOp));
  });

  test('attaches with new period', async () => {
    const remote = new MockProcess(TreeClock.GENESIS);
    const sentOp1 = remote.sentOperation({}, {});
    const sentOp2 = remote.sentOperation({}, {});
    const sentOp3 = remote.sentOperation({}, {});
    const operations = new Subject<OperationMessage>();
    const ro = new RemoteOperations(mockRemotes(operations));
    const received = firstValueFrom(ro.receiving.pipe(take(2), toArray()));
    await ro.attach(EMPTY);
    operations.next(sentOp1);
    ro.detach();
    operations.next(sentOp2);
    await ro.attach(EMPTY);
    operations.next(sentOp3);
    await expect(received).resolves.toEqual(
      // One-th period operation is skipped
      [[sentOp1, 0], [sentOp3, 2]]);
  });

  test('attaches with rev-up', async () => {
    const remote = new MockProcess(TreeClock.GENESIS);
    const revupOp = remote.sentOperation({}, {});
    const sentOp1 = remote.sentOperation({}, {});
    const sentOp2 = remote.sentOperation({}, {});
    const operations = new Subject<OperationMessage>();
    const ro = new RemoteOperations(mockRemotes(operations));
    const received = firstValueFrom(ro.receiving.pipe(take(3), toArray()));
    await ro.attach(of(revupOp));
    operations.next(sentOp1);
    operations.next(sentOp2);
    await expect(received).resolves.toEqual(
      // All ops are in the zeroth period
      [[revupOp, 0], [sentOp1, 0], [sentOp2, 0]]);
    expect(ro.outdated.value).toBe(true); // Still outdated...
    revupOp.delivered.resolve(); // ... until delivered
    await firstValueFrom(ro.outdated.pipe(find(v => !v)));
  });

  test('detaches if rev-up fails', done => {
    const remote = new MockProcess(TreeClock.GENESIS);
    const operations = new ReplaySubject<OperationMessage>();
    // This op could sneak through via the replay
    operations.next(remote.sentOperation({}, {}));
    const ro = new RemoteOperations(mockRemotes(operations));
    const attach = ro.attach(throwError(() => 'bang'));
    ro.receiving.subscribe({
      next: () => done('unexpected op'),
      complete: done,
      error: done
    });
    attach.then(() => done('expected bang'), () => {
      // Another attempt to sneak through
      operations.next(remote.sentOperation({}, {}));
      setTimeout(() => ro.close(), 1);
    });
  });

  test('ignores rev-up if operations complete', done => {
    const remote = new MockProcess(TreeClock.GENESIS);
    const operations = new Subject<OperationMessage>();
    const ro = new RemoteOperations(mockRemotes(operations));
    ro.attach(defer(() => {
      operations.complete();
      return of(remote.sentOperation({}, {}));
    }));
    ro.receiving.subscribe({
      next: () => done('unexpected op'),
      complete: done,
      error: done
    });
    setTimeout(() => ro.close(), 1);
  });

  test('ignores post rev-up op if operations complete', done => {
    const remote = new MockProcess(TreeClock.GENESIS);
    const operations = new ReplaySubject<OperationMessage>();
    const ro = new RemoteOperations(mockRemotes(operations));
    ro.attach(defer(() => {
      operations.next(remote.sentOperation({}, {}));
      operations.complete();
      return EMPTY;
    }));
    ro.receiving.subscribe({
      next: () => done('unexpected op'),
      complete: done,
      error: done
    });
    setTimeout(() => ro.close(), 1);
  });

});