import { DeltaMessage, MeldRemotes } from '..';
import { LiveValue } from "../LiveValue";
import { Observable, merge, NEVER, BehaviorSubject } from 'rxjs';
import { switchAll } from 'rxjs/operators';
import { delayUntil, Future, tapLast, onErrorNever } from '../util';

export class RemoteUpdates {
  readonly receiving: Observable<DeltaMessage>;
  // Note this is a behaviour subject because the subscribe from the
  // DatasetEngine happens after the remote updates are attached
  private readonly remoteUpdates = new BehaviorSubject<Observable<DeltaMessage>>(NEVER);
  private readonly outdatedState = new BehaviorSubject<boolean>(true);

  constructor(
    private readonly remotes: MeldRemotes) {
    this.receiving = this.remoteUpdates.pipe(switchAll());
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

  detach = () => {
    // We may or may not be outdated at this point
    this.remoteUpdates.next(NEVER);
  };

  attach(revups: Observable<DeltaMessage>): Promise<unknown> {
    const lastRevup = new Future<DeltaMessage | undefined>();
    // Push the rev-up to next tick - probably only for unit tests' sake
    setImmediate(() => {
      // Updates must be paused during revups because the collaborator might
      // send an update while also sending revups of its own prior updates.
      // That would break the fifo guarantee.
      this.remoteUpdates.next(merge(
        // Errors should be handled in the returned promise
        onErrorNever(revups.pipe(tapLast(lastRevup))),
        this.remotes.updates.pipe(delayUntil(onErrorNever(lastRevup)))));
      lastRevup.then(
        async (lastRevup: DeltaMessage | undefined) => {
          // Here, we are definitely before the first post-revup update, but
          // the actual last revup might not yet have been applied to the dataset.
          if (lastRevup != null)
            await lastRevup.delivered;
          this.outdatedState.next(false);
        },
        // Rev-up failed - detached and nothing-doing
        () => { });
    });
    return Promise.resolve(lastRevup);
  }
}
