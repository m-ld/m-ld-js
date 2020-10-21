import { Context, Subject, Describe, Pattern, Update, Read, Write } from '../jrql-support';
import { Observable, Subscription } from 'rxjs';
import { map, take } from 'rxjs/operators';
import { flatten } from 'jsonld';
import { MeldUpdate, MeldState, Resource, any, MeldReadState, MeldStateMachine, ReadResult, readResult, StateProc, UpdateProc } from '../api';
import { SharableLock } from "./locks";

/** Simplified clone engine with only the basic requirements of an engine */
export interface CloneEngine {
  readonly lock: SharableLock<'state'>;
  /** An update MUST only happen during a write OR when 'state' is exclusively locked */
  readonly dataUpdates: Observable<MeldUpdate>;

  read(request: Read): Observable<Subject>;
  write(request: Write): Promise<unknown>;
}

interface EngineState {
  read(request: Read): Observable<Subject>;
  write(request: Write): Promise<EngineState>;
}

type EngineUpdateProc = (update: MeldUpdate, state: EngineState) => PromiseLike<unknown> | void;
type EngineStateProc = (state: EngineState) => PromiseLike<unknown> | void;

/**
 * Gates access to a {@link CloneEngine} such that its state is immutable during
 * read and write procedures
 */
class StateEngine {
  private state: EngineState;
  private readonly handlers: EngineUpdateProc[] = [];
  private handling: Promise<unknown>;

  constructor(
    private readonly engine: CloneEngine) {
    this.newState();
    this.engine.dataUpdates.subscribe(this.nextState);
  }

  follow(handler: EngineUpdateProc): Subscription {
    const key = this.handlers.push(handler);
    return new Subscription(() => { delete this.handlers[key]; });
  }

  /** procedure and handler must not reject */
  read(procedure: EngineStateProc, handler?: EngineUpdateProc): Subscription {
    const subs = new Subscription;
    this.engine.lock.share('state', async () => {
      if (!subs.closed) {
        if (handler != null)
          this.follow(handler);
        await procedure(this.state);
      }
    });
    return subs;
  }

  write(procedure: EngineStateProc): Promise<unknown> {
    return this.engine.lock.exclusive('state', () => procedure(this.state));
  }

  private nextState = async (update: MeldUpdate) => {
    // TODO: Assert that the lock is currently exclusive
    const state = this.newState();
    // Run all the handlers for the new state, ensuring lock coverage
    this.engine.lock.extend('state',
      this.handling = Promise.all(Object.values(this.handlers)
        .map(handler => handler(update, state))));
  }

  private newState() {
    const state: EngineState = {
      read: (request: Read) => gateEngine().read(request),
      write: async (request: Write) => {
        // Ensure all read handlers are complete before changing state
        await this.handling;
        await gateEngine().write(request);
        // At this point, there should be a new state from the data update
        if (state === this.state)
          throw new Error('Write did not produce a new state');
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

abstract class ApiState implements MeldState {
  constructor(
    protected readonly context: Context,
    private readonly state: EngineState) {
  }

  read<R extends Read = Read, S = Subject>(request: R): ReadResult<Resource<S>> {
    const result = this.state.read(this.applyRequestContext(request));
    return readResult(result.pipe(map((subject: Subject) =>
      // Strip the domain context from each subject
      <Resource<S>>this.stripSubjectContext(subject))));
  }

  async write<W = Write>(request: W): Promise<MeldState> {
    return this.construct(await this.state.write(this.applyRequestContext(request)));
  }

  delete(id: string): Promise<MeldState> {
    const asSubject: Subject = { '@id': id, [any()]: any() };
    const asObject: Subject = { '@id': any(), [any()]: { '@id': id } };
    return this.write<Update>({
      '@delete': [asSubject, asObject],
      '@where': { '@union': [asSubject, asObject] }
    });
  }

  get<S = Subject>(id: string): Promise<Resource<S> | undefined> {
    return this.read<Describe, S>({ '@describe': id }).pipe(take(1)).toPromise();
  }

  protected abstract construct(state: EngineState): MeldState;

  private applyRequestContext<P extends Pattern>(request: P): P {
    return {
      ...request,
      // Apply the domain context to the request, explicit context wins
      '@context': { ...this.context, ...request['@context'] || {} }
    };
  }

  private stripSubjectContext(jsonld: Subject): Subject {
    const { '@context': context, ...rtn } = jsonld;
    if (context)
      Object.keys(this.context).forEach((k: keyof Context) => delete context[k]);
    return context && Object.keys(context).length ? { ...rtn, '@context': context } : rtn;
  }
}

class ImmutableState extends ApiState {
  protected construct(state: EngineState): MeldState {
    return new ImmutableState(this.context, state);
  }
}

/**
 * Applies a context to all state
 */
export class ApiStateMachine extends ApiState implements MeldStateMachine {
  private readonly engine: StateEngine;

  constructor(context: Context, engine: CloneEngine) {
    const stateEngine = new StateEngine(engine);
    super(context, {
      read: (request: Read): Observable<Subject> => {
        return new Observable(subs => {
          const subscription = new Subscription;
          // This does not wait for results before returning control
          stateEngine.read(async state => subscription.add(state.read(request).subscribe(subs)));
          return subscription;
        });
      },
      write: async (request: Write): Promise<EngineState> => {
        await stateEngine.write(state => state.write(request));
        return this;
      }
    });
    this.engine = stateEngine;
  }

  private updateHandler(handler: UpdateProc): EngineUpdateProc {
    return async (update, state) =>
      handler(await this.applyUpdateContext(update), new ImmutableState(this.context, state))
  }

  follow(handler: UpdateProc): Subscription {
    return this.engine.follow(this.updateHandler(handler));
  }

  read(procedure: StateProc, handler?: UpdateProc): Subscription;
  read<R extends Read = Read, S = Subject>(request: R): ReadResult<Resource<S>>;
  read(request: Read | StateProc, handler?: UpdateProc) {
    if (typeof request == 'function') {
      return this.engine.read(
        state => request(new ImmutableState(this.context, state)),
        handler != null ? this.updateHandler(handler) : undefined);
    } else {
      return super.read(request);
    }
  }

  write(procedure: StateProc<MeldState>): Promise<unknown>;
  write<W = Write>(request: W): Promise<MeldState>;
  async write(request: Write | StateProc<MeldState>) {
    if (typeof request == 'function') {
      await this.engine.write(state => request(new ImmutableState(this.context, state)));
      return this;
    } else {
      return super.write(request);
    }
  }

  protected construct(): MeldState {
    // For direct read and write, we are mutable
    return this;
  }

  private async applyUpdateContext(update: MeldUpdate) {
    return {
      '@ticks': update['@ticks'],
      '@delete': await this.regroup(update['@delete']),
      '@insert': await this.regroup(update['@insert'])
    }
  }

  private async regroup(subjects: Subject[]): Promise<Subject[]> {
    const graph: any = await flatten(subjects, this.context);
    return graph['@graph'];
  }
}
