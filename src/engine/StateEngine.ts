import { Read, Write } from '../jrql-support';
import { GraphSubject, MeldUpdate } from '../api';
import { LockManager } from './locks';
import { Observable, Subscription } from 'rxjs';
import { QueryableRdfSource } from '../rdfjs-support';
import { Consumable } from '../flowable';
import { QueryableRdfSourceProxy } from './quads';

/** Simplified clone engine with only the basic requirements of an engine */
export interface CloneEngine extends EngineState {
  /**
   * The state lock **must** be held as a precondition for both {@link
   * EngineState.read} and {@link EngineState.write}.
   */
  readonly lock: LockManager<'state'>;
  /** An update MUST happen when 'state' is exclusively locked */
  readonly dataUpdates: Observable<MeldUpdate>;
}

export interface EngineState extends QueryableRdfSource {
  read(request: Read): Consumable<GraphSubject>;
  write(request: Write): Promise<this>;
}

export type EngineUpdateProc =
  (update: MeldUpdate, state: EngineState) => PromiseLike<unknown> | void;
export type EngineStateProc =
  (state: EngineState) => PromiseLike<unknown> | void;

/**
 * Gates access to a {@link CloneEngine} such that its state is immutable during
 * read and write procedures
 */
export class StateEngine extends QueryableRdfSourceProxy {
  private state: EngineState;
  private readonly handlers: EngineUpdateProc[] = [];
  private handling: Promise<unknown>;

  constructor(
    private readonly engine: CloneEngine) {
    super();
    this.newState();
    this.engine.dataUpdates.subscribe(this.nextState);
  }

  protected get src(): QueryableRdfSource {
    return this.state;
  }

  follow(handler: EngineUpdateProc): Subscription {
    const key = this.handlers.push(handler) - 1;
    return new Subscription(() => { delete this.handlers[key]; });
  }

  /** procedure and handler must not reject */
  read(procedure: EngineStateProc, handler?: EngineUpdateProc): Subscription {
    const subs = new Subscription;
    // noinspection JSIgnoredPromiseFromCall – return subscription synchronously
    this.engine.lock.share('state', async () => {
      if (!subs.closed) {
        if (handler != null)
          subs.add(this.follow(handler));
        await procedure(this.state);
        // TODO destroy any unsubscribed queries?
      }
    });
    return subs;
  }

  write(procedure: EngineStateProc): Promise<unknown> {
    return this.engine.lock.exclusive('state', () => procedure(this.state));
  }

  private nextState = (update: MeldUpdate) => {
    const state = this.newState();
    // Run all the handlers for the new state, ensuring lock coverage
    // noinspection JSIgnoredPromiseFromCall
    this.engine.lock.extend('state',
      this.handling = Promise.all(Object.values(this.handlers)
        .map(handler => handler(update, state))));
  };

  private newState() {
    const state: EngineState = {
      countQuads: (...args) => gateEngine().countQuads(...args),
      // @ts-ignore - TS can't cope with overloaded query method
      query: (...args) => gateEngine().query(...args),
      match: (...args) => gateEngine().match(...args),
      read: request => gateEngine().read(request),
      write: async request => {
        // Ensure all read handlers are complete before changing state
        await this.handling;
        await gateEngine().write(request);
        // At this point, there should be a new state from the data update, but
        // not if the write was a no-op
        return this.state;
      }
    };
    const gateEngine = () => {
      if (this.state !== state)
        throw new Error('State has been de-scoped.');
      return this.engine;
    };
    return this.state = state;
  }
}
