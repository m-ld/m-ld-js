import { Describe, Read, Subject, Update, Write } from '../jrql-support';
import { defaultIfEmpty, firstValueFrom, Observable, Subscription } from 'rxjs';
import {
  any, GraphSubject, MeldState, MeldStateMachine, readResult, ReadResult, StateProc, UpdateProc
} from '../api';
import { CloneEngine, EngineState, EngineUpdateProc, StateEngine } from './StateEngine';
import { QueryableRdfSourceProxy } from './quads';

abstract class ApiState extends QueryableRdfSourceProxy implements MeldState {
  constructor(
    private readonly state: EngineState) {
    super(state);
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

  get(id: string): Promise<GraphSubject | undefined> {
    return firstValueFrom(this.read<Describe>({ '@describe': id }).pipe(defaultIfEmpty(undefined)));
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
    super(Object.assign(new QueryableRdfSourceProxy(stateEngine), {
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
    }));
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
