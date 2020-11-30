import { DeltaMessage, MeldRemotes } from '..';
import { LiveValue } from "../LiveValue";
import { Observable, merge, NEVER, BehaviorSubject, from } from 'rxjs';
import { delayUntil, Future, tapLast, onErrorNever, HotSwitch, settled } from '../util';

export class RemoteUpdates {
  private readonly outdatedState = new BehaviorSubject<boolean>(true);
  private readonly updates = new HotSwitch<DeltaMessage>();

  constructor(
    private readonly remotes: MeldRemotes) {
  }

  get receiving(): Observable<DeltaMessage> {
    return this.updates;
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
    this.updates.switch(NEVER);
  };

  attach(revups: Observable<DeltaMessage>): Promise<unknown> {
    const lastRevup = new Future<DeltaMessage | undefined>();
    // Push the rev-up to next tick - probably only for unit tests' sake
    setImmediate(() => {
      // Updates must be paused during revups because the collaborator might
      // send an update while also sending revups of its own prior updates.
      // That would break the fifo guarantee.
      this.updates.switch(merge(
        // Errors should be handled in the returned promise
        onErrorNever(revups.pipe(tapLast(lastRevup))),
        // If the revups error, we will detach, below
        this.remotes.updates.pipe(delayUntil(settled(lastRevup)))));
      lastRevup.then(
        async (lastRevup: DeltaMessage | undefined) => {
          // Here, we are definitely before the first post-revup update, but
          // the actual last revup might not yet have been applied to the dataset.
          if (lastRevup != null)
            await lastRevup.delivered;
          this.outdatedState.next(false);
        },
        // Rev-up failed - detached and nothing-doing
        () => this.updates.switch(NEVER));
    });
    return Promise.resolve(lastRevup);
  }
}
