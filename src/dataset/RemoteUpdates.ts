import { DeltaMessage, MeldRemotes } from '../m-ld';
import { LiveValue } from "../LiveValue";
import { Observable, merge, NEVER, BehaviorSubject, defer } from 'rxjs';
import { switchAll } from 'rxjs/operators';
import { delayUntil, Future, tapLast, onErrorNever } from '../util';

export interface AttachStatus {
  attached: boolean;
  revvingUp: boolean;
}

export class RemoteUpdates {
  readonly receiving: Observable<DeltaMessage>;
  // Note this is a behaviour subject because the subscribe from the
  // DatasetClone happens after the remote updates are attached
  private readonly remoteUpdates = new BehaviorSubject<Observable<DeltaMessage>>(NEVER);
  private readonly attachState = new BehaviorSubject<AttachStatus>({
    attached: false, revvingUp: false
  });

  constructor(
    private readonly remotes: MeldRemotes) {
    this.receiving = this.remoteUpdates.pipe(switchAll());
  }

  get state(): LiveValue<AttachStatus> {
    return this.attachState;
  }

  close(err?: any) {
    if (err)
      this.attachState.error(err);
    else
      this.attachState.complete();
  }

  attach = () => {
    // Don't override the revving-up state if already attached
    if (!this.state.value.attached) {
      this.attachState.next({ attached: true, revvingUp: false });
      return this.remoteUpdates.next(this.remotes.updates);
    }
  };

  detach = () => {
    this.attachState.next({ attached: false, revvingUp: false });
    return this.remoteUpdates.next(NEVER);
  };

  injectRevups(revups: Observable<DeltaMessage>): Promise<DeltaMessage | undefined> {
    const lastRevup = new Future<DeltaMessage | undefined>();
    this.attachState.next({ attached: true, revvingUp: true });
    // Push the rev-up to next tick - probably only for unit tests' sake
    setImmediate(() => {
      // Updates must be paused during revups because the collaborator might
      // send an update while also sending revups of its own prior updates.
      // That would break the ordering guarantee.
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
          this.attachState.next({ attached: true, revvingUp: false });
        },
        // Rev-up failed - detached and nothing-doing
        () => this.attachState.next({ attached: false, revvingUp: false }));
    });
    return Promise.resolve(lastRevup);
  }
}
