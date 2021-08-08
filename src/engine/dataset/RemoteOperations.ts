import { MeldRemotes, OperationMessage } from '..';
import { LiveValue } from '../LiveValue';
import { BehaviorSubject, merge, NEVER, Observable } from 'rxjs';
import { delayUntil, Future, HotSwitch, onErrorNever, settled, tapLast } from '../util';

export class RemoteOperations {
  private readonly outdatedState = new BehaviorSubject<boolean>(true);
  private readonly operations = new HotSwitch<OperationMessage>();

  constructor(
    private readonly remotes: MeldRemotes) {
  }

  get receiving(): Observable<OperationMessage> {
    return this.operations;
  }

  get outdated(): LiveValue<boolean> {
    return this.outdatedState;
  }

  close(err?: any) {
    if (err)
      this.outdatedState.error(err);
    else
      this.outdatedState.complete();
  }

  detach = (outdated?: 'outdated') => {
    if (outdated)
      this.outdatedState.next(true);
    this.operations.switch(NEVER);
  };

  attach(revups: Observable<OperationMessage>): Promise<unknown> {
    const lastRevup = new Future<OperationMessage | undefined>();
    // Push the rev-up to next tick - probably only for unit tests' sake
    setImmediate(() => {
      // Operations must be paused during revups because the collaborator might
      // send an update while also sending revups of its own prior operations.
      // That would break the fifo guarantee.
      this.operations.switch(merge(
        // Errors should be handled in the returned promise
        onErrorNever(revups.pipe(tapLast(lastRevup))),
        // If the revups error, we will detach, below
        this.remotes.operations.pipe(delayUntil(settled(lastRevup)))));
      lastRevup.then(
        async (lastRevup: OperationMessage | undefined) => {
          // Here, we are definitely before the first post-revup update, but
          // the actual last revup might not yet have been applied to the dataset.
          if (lastRevup != null)
            await lastRevup.delivered;
          this.outdatedState.next(false);
        },
        // Rev-up failed - detached and nothing-doing
        () => this.operations.switch(NEVER));
    });
    return Promise.resolve(lastRevup);
  }
}
