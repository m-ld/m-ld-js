import { DeltaMessage, MeldRemotes, MeldStatus } from '../m-ld';
import { Observable, Subject as Source, merge, NEVER, BehaviorSubject } from 'rxjs';
import { filter, switchAll, first } from 'rxjs/operators';
import { delayUntil, Future, tapLast, onErrorNever } from '../util';
import { Logger } from 'loglevel';

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
    private readonly remotes: MeldRemotes,
    log: Logger) {
    this.receiving = this.remoteUpdates.pipe(switchAll());
    this.attachState.subscribe(state => log.debug(JSON.stringify(state)));
  }

  get attached(): boolean {
    return this.attachState.value.attached;
  }

  async status(match?: Partial<AttachStatus>): Promise<AttachStatus> {
    if (match == null)
      return this.attachState.value;
    else
      return this.attachState.pipe(
        filter(state =>
          (match.attached === undefined || state.attached === match.attached) &&
          (match.outdated === undefined || state.outdated === match.outdated)),
        first()).toPromise();
  };

  setOutdated = () =>
    this.attachState.next({ attached: this.attached, outdated: true });

  attach = () => {
    this.attachState.next({ attached: true, outdated: false });
    return this.remoteUpdates.next(this.remotes.updates);
  };

  detach = (isGenesis: boolean) => {
    // A genesis clone is always in date
    this.attachState.next({ attached: false, outdated: !isGenesis });
    return this.remoteUpdates.next(NEVER);
  };

  injectRevups(revups: Observable<DeltaMessage>): Promise<DeltaMessage | undefined> {
    const lastRevup = new Future<DeltaMessage | undefined>();
    this.attachState.next({ attached: true, outdated: true });
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
        this.attachState.next({ attached: true, outdated: false });
      },
      () => this.attachState.next({ attached: false, outdated: false }));
    return Promise.resolve(lastRevup);
  }
}
