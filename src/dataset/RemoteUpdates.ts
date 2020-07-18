import { DeltaMessage, MeldRemotes, LiveValue } from '../m-ld';
import { Observable, Subject as Source, merge, NEVER, BehaviorSubject } from 'rxjs';
import { switchAll } from 'rxjs/operators';
import { delayUntil, Future, tapLast, onErrorNever } from '../util';

export interface AttachStatus {
  attached: boolean;
  outdated: boolean;
}

export class RemoteUpdates {
  readonly receiving: Observable<DeltaMessage>;
  private readonly remoteUpdates = new Source<Observable<DeltaMessage>>();
  private readonly attachState = new BehaviorSubject<AttachStatus>({
    attached: false, outdated: true
  });

  constructor(
    private readonly remotes: MeldRemotes) {
    this.receiving = this.remoteUpdates.pipe(switchAll());
  }

  get state(): LiveValue<AttachStatus> {
    return this.attachState;
  }

  private setState(state: Partial<AttachStatus>) {
    this.attachState.next({ ...this.state.value, ...state });
  }

  close(err?: any) {
    if (err)
      this.attachState.error(err);
    else
      this.attachState.complete();
  }

  maybeOutdated = (isGenesis: boolean) =>
    this.setState({ outdated: !isGenesis });

  attach = () => {
    this.setState({ attached: true });
    return this.remoteUpdates.next(this.remotes.updates);
  };

  detach = (isGenesis: boolean) => {
    // A genesis clone is always in date, otherwise we could be out of date
    this.setState({ attached: false, outdated: !isGenesis });
    return this.remoteUpdates.next(NEVER);
  };

  injectRevups(revups: Observable<DeltaMessage>): Promise<DeltaMessage | undefined> {
    const lastRevup = new Future<DeltaMessage | undefined>();
    this.setState({ attached: true, outdated: true });
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
        this.setState({ attached: true, outdated: false });
      },
      // Rev-up failed - we are neither attached nor up-to-date
      () => this.setState({ attached: false, outdated: true }));
    return Promise.resolve(lastRevup);
  }
}
