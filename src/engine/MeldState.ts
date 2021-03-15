import { Subject, Describe, Update, Read, Write } from '../jrql-support';
import { Observable, Subscription } from 'rxjs';
import { take } from 'rxjs/operators';
import {
  MeldState, Resource, any, MeldStateMachine,
  ReadResult, StateProc, UpdateProc, readResult, GraphSubject
} from '../api';
import { CloneEngine, EngineState, EngineUpdateProc, StateEngine } from './StateEngine';

abstract class ApiState implements MeldState {
  constructor(
    private readonly state: EngineState) {
  }

  read<R extends Read = Read>(request: R): ReadResult {
    return readResult(this.state.read(request));
  }

  async write<W = Write>(request: W): Promise<MeldState> {
    return this.construct(await this.state.write(request));
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
    return this.read<Describe>({ '@describe': id }).pipe(take<Resource<S>>(1)).toPromise();
  }

  protected abstract construct(state: EngineState): MeldState;
}

class ImmutableState extends ApiState {
  protected construct(state: EngineState): MeldState {
    return new ImmutableState(state);
  }
}

/**
 * Applies a context to all state
 */
export class ApiStateMachine extends ApiState implements MeldStateMachine {
  private readonly engine: StateEngine;

  constructor(engine: CloneEngine) {
    const stateEngine = new StateEngine(engine);
    super({
      read: (request: Read): Observable<GraphSubject> => {
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
    return async (update, state) => handler(update, new ImmutableState(state));
  }

  follow(handler: UpdateProc): Subscription {
    return this.engine.follow(this.updateHandler(handler));
  }

  read(procedure: StateProc, handler?: UpdateProc): Subscription;
  read<R extends Read = Read, S = Subject>(request: R): ReadResult;
  read(request: Read | StateProc, handler?: UpdateProc) {
    if (typeof request == 'function') {
      return this.engine.read(
        state => request(new ImmutableState(state)),
        handler != null ? this.updateHandler(handler) : undefined);
    } else {
      return super.read(request);
    }
  }

  write(procedure: StateProc<MeldState>): Promise<unknown>;
  write<W = Write>(request: W): Promise<MeldState>;
  async write(request: Write | StateProc<MeldState>) {
    if (typeof request == 'function') {
      await this.engine.write(state => request(new ImmutableState(state)));
      return this;
    } else {
      return super.write(request);
    }
  }

  protected construct(): MeldState {
    // For direct read and write, we are mutable
    return this;
  }
}
