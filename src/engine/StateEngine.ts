import { Query, Read, Write } from '../jrql-support';
import { GraphSubject, MeldQuadDeleteInsert, MeldUpdate } from '../api';
import { LockManager } from './locks';
import { Observable, Subscription } from 'rxjs';
import { QueryableRdfSource } from '../rdfjs-support';
import { Consumable } from 'rx-flowable';
import { QueryableRdfSourceProxy } from './quads';

/** Simplified clone engine with only the basic requirements of an engine */
export interface CloneEngine extends EngineState {
  /**
   * The state lock **must** be held as a precondition for both
   * {@link EngineState.read} and {@link EngineState.write}.
   */
  readonly lock: LockManager<'state'>;
  /** An update MUST happen when 'state' is exclusively locked */
  readonly dataUpdates: Observable<MeldUpdate>;
}

export type EngineWrite = { jrql: Write } | { rdf: MeldQuadDeleteInsert };

export interface EngineState extends QueryableRdfSource {
  read(request: Read): Consumable<GraphSubject>;
  write(request: EngineWrite): Promise<EngineState>;
  ask(pattern: Query): Promise<boolean>;
}

export type EngineUpdateProc =
  (update: MeldUpdate, state: EngineState) => PromiseLike<unknown> | void;
export type EngineStateProc<T = unknown> =
  (state: EngineState) => PromiseLike<T> | T;

export const followUnsubscribed = Symbol('Unsubscribed');

/**
 * Gates access to a {@link CloneEngine} such that its state is immutable during
 * read and write procedures
 */
export class StateEngine extends QueryableRdfSourceProxy {
  private state: EngineState;
  private readonly handlers: EngineUpdateProc[] = [];
  private handling: Promise<unknown>;

  constructor(
    private readonly clone: CloneEngine) {
    super();
    this.newState();
    this.clone.dataUpdates.subscribe(this.nextState);
  }

  protected get src(): QueryableRdfSource {
    return this.state;
  }

  follow(handler: EngineUpdateProc) {
    const key = this.handlers.push(handler) - 1;
    return () => { delete this.handlers[key]; };
  }

  /** procedure and handler must not reject */
  read<T>(procedure: EngineStateProc<T>, subs?: Subscription, handler?: EngineUpdateProc) {
    // noinspection JSIgnoredPromiseFromCall – return subscription synchronously
    return this.clone.lock.share<T | typeof followUnsubscribed>(
      'state', 'read', async () => {
        if (subs == null || !subs.closed) {
          if (handler != null)
            subs?.add(this.follow(handler));
          return procedure(this.state);
          // TODO destroy any unsubscribed queries?
        } else {
          return followUnsubscribed;
        }
      }
    );
  }

  write(procedure: EngineStateProc): Promise<unknown> {
    return this.clone.lock.exclusive(
      'state',
      'write',
      () => procedure(this.state)
    );
  }

  private nextState = (update: MeldUpdate) => {
    const state = this.newState();
    // Run all the handlers for the new state, ensuring lock coverage
    // noinspection JSIgnoredPromiseFromCall
    this.clone.lock.extend(
      'state',
      'next',
      this.handling = Promise.all(Object.values(this.handlers)
        .map(handler => handler(update, state)))
    );
  };

  private newState() {
    const engine = this;
    const state = new class extends QueryableRdfSourceProxy implements EngineState {
      protected get src(): CloneEngine {
        if (engine.state !== state)
          throw new Error('State has been de-scoped.');
        return engine.clone;
      }
      read(request: Read) {
        return this.src.read(request);
      }
      async write(request: EngineWrite) {
        // Ensure all read handlers are complete before changing state
        await engine.handling;
        await this.src.write(request);
        // At this point, there should be a new state from the data update, but
        // not if the write was a no-op
        return engine.state;
      }
      ask(pattern: Query): Promise<boolean> {
        return this.src.ask(pattern);
      }
    };
    return this.state = state;
  }
}
