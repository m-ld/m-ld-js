import { MeldRemotes, OperationMessage } from '..';
import { LiveValue } from '../api-support';
import { BehaviorSubject, endWith, merge, NEVER, Observable } from 'rxjs';
import { delayUntil, Future, HotSwitch, onErrorNever, tapLast } from '../util';
import { ignoreElements, map, takeUntil } from 'rxjs/operators';
import { MeldOperationMessage } from '../MeldOperationMessage';

export class RemoteOperations {
  private readonly outdatedState = new BehaviorSubject<boolean>(true);
  private readonly operations = new HotSwitch<MeldOperationMessage>();
  private _period = -1;

  constructor(
    private readonly remotes: MeldRemotes) {
  }

  get receiving(): Observable<[MeldOperationMessage, number]> {
    return this.operations.pipe(map(op => [op, this._period]));
  }

  get period() {
    return this._period;
  }

  get outdated(): LiveValue<boolean> {
    return this.outdatedState;
  }

  close(err?: any) {
    if (err)
      this.outdatedState.error(err);
    else
      this.outdatedState.complete();
    this.operations.close();
  }

  detach = (outdated?: 'outdated') => {
    if (outdated)
      this.outdatedState.next(true);
    this.nextPeriod(NEVER);
  };

  attach(revups: Observable<OperationMessage>): Promise<unknown> {
    const captureLastRevup = new Future<MeldOperationMessage | undefined>();
    // Push the rev-up to next tick - probably only for unit tests' sake
    setImmediate(() => {
      // Errors should be handled in the returned promise
      this.nextPeriod(onErrorNever(merge(
        revups.pipe(
          map(MeldOperationMessage.fromMessage),
          tapLast(captureLastRevup)),
        // Operations must be paused during revups because the collaborator
        // might send an update while also sending revups of its own prior
        // operations. That would break the fifo guarantee.
        this.remotes.operations.pipe(
          map(MeldOperationMessage.fromMessage),
          delayUntil(captureLastRevup))
      )).pipe(
        // If the remote operations finalise, mirror that
        takeUntil(this.remotes.operations.pipe(ignoreElements(), endWith(0)))
      ));
      captureLastRevup.then(async lastRevup => {
        // Here, we are definitely before the first post-revup update, but
        // the actual last revup might not yet have been applied to the dataset.
        if (lastRevup != null)
          await lastRevup.delivered;
        this.outdatedState.next(false);
      }).catch(() => {
        // Rev-up failed - detached and nothing-doing
        this.nextPeriod(NEVER);
      });
    });
    return Promise.resolve(captureLastRevup);
  }

  private nextPeriod(ops: Observable<MeldOperationMessage>) {
    this._period++;
    this.operations.switch(ops);
  }
}
