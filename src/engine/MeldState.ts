import { Query, Read, Subject, SubjectProperty, Update, Write } from '../jrql-support';
import { Subscription } from 'rxjs';
import {
  any, GraphSubject, MeldState, MeldStateMachine, readResult, ReadResult, StateProc, UpdateProc,
  WriteOptions
} from '../api';
import { CloneEngine, EngineState, EngineUpdateProc, StateEngine } from './StateEngine';
import { QueryableRdfSourceProxy } from './quads';
import { Consumable } from 'rx-flowable';
import { first, Future, inflateFrom } from './util';
import { QueryableRdfSource } from '../rdfjs-support';
import { constructProperties, describeId } from './jrql-util';

abstract class ApiState extends QueryableRdfSourceProxy implements MeldState {
  constructor(
    private readonly state: EngineState) {
    super();
  }

  protected get src(): QueryableRdfSource {
    return this.state;
  }

  read<R extends Read = Read>(request: R): ReadResult {
    return readResult(this.state.read(request));
  }

  async write<W = Write>(request: W, opts?: WriteOptions): Promise<MeldState> {
    return this.construct(await this.state.write(request, opts));
  }

  delete(id: string): Promise<MeldState> {
    const asSubject: Subject = { '@id': id, [any()]: any() };
    const asObject: Subject = { '@id': any(), [any()]: { '@id': id } };
    return this.write<Update>({
      '@delete': [asSubject, asObject],
      '@where': { '@union': [asSubject, asObject] }
    });
  }

  async get(id: string, ...properties: SubjectProperty[]) {
    return first(this.read(properties.length === 0 ?
      describeId(id) : constructProperties(id, properties)));
  }

  ask(pattern: Query): Promise<boolean> {
    return this.state.ask(pattern);
  }

  protected abstract construct(state: EngineState): MeldState;
}

class ImmutableState extends ApiState {
  protected construct(state: EngineState): MeldState {
    return new ImmutableState(state);
  }
}

export class ApiStateMachine extends ApiState implements MeldStateMachine {
  private readonly engine: StateEngine;

  constructor(engine: CloneEngine) {
    const stateEngine = new StateEngine(engine);
    // The API state machine also pretends to be a state
    const asEngineState = new (class extends QueryableRdfSourceProxy implements EngineState {
      get src(): QueryableRdfSource {
        return stateEngine;
      }
      read(request: Read): Consumable<GraphSubject> {
        // The read itself must be in a state procedure, so indirect
        const result = new Future<Consumable<GraphSubject>>();
        stateEngine.read(state => result.resolve(state.read(request)));
        return inflateFrom(result);
      }
      async write(request: Write, opts?: WriteOptions): Promise<this> {
        await stateEngine.write(state => state.write(request, opts));
        return this;
      }
      ask(pattern: Query): Promise<boolean> {
        // The read itself must be in a state procedure, so indirect
        return new Promise((resolve, reject) =>
          stateEngine.read(state => state.ask(pattern).then(resolve, reject)));
      }
    });
    super(asEngineState);
    this.engine = stateEngine;
  }

  private updateHandler(handler: UpdateProc): EngineUpdateProc {
    return async (update, state) =>
      handler(update, new ImmutableState(state));
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
  write<W = Write>(request: W, opts?: WriteOptions): Promise<MeldState>;
  async write(request: Write | StateProc<MeldState>, opts?: WriteOptions) {
    if (typeof request == 'function') {
      await this.engine.write(state => request(new ImmutableState(state)));
      return this;
    } else {
      return super.write(request, opts);
    }
  }

  protected construct(): MeldState {
    // For direct read and write, we are mutable
    return this;
  }
}
