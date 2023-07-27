import { Query, Read, Subject, SubjectProperty, Update, Write } from '../jrql-support';
import { Subscription } from 'rxjs';
import { any, MeldState, MeldStateMachine, ReadResult, StateProc, UpdateProc } from '../api';
import { CloneEngine, EngineState, EngineUpdateProc, StateEngine } from './StateEngine';
import { QueryableRdfSourceProxy } from './quads';
import { first, inflateFrom } from './util';
import { QueryableRdfSource } from '../rdfjs-support';
import { constructProperties, describeId } from './jrql-util';
import { readResult } from './api-support';
import async from './async';

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

  async write<W extends Write = Write>(request: W): Promise<MeldState> {
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
    function _read<Rtn>(
      proc: (state: EngineState) => Rtn,
      toRtn: (willRtn: Promise<Rtn>) => Rtn
    ): Rtn {
      // Any direct read must be in a state procedure, so indirect
      return toRtn(new Promise((resolve, reject) => {
        stateEngine.read(state => {
          // possibly an over-abundance of caution, because the proc should
          // signal error through its return type (promise, observable, or stream)
          try { resolve(proc(state)); } catch (e) { reject(e); }
        });
      }));
    }
    // The API state machine also pretends to be a state
    super({
      async write(request: Write) {
        await stateEngine.write(state => state.write(request));
        return this;
      },
      read(request: Read) {
        return _read(state => state.read(request), inflateFrom);
      },
      ask(pattern: Query) {
        return _read(state => state.ask(pattern), async rtn => rtn);
      },
      countQuads(...args) {
        return _read(state => state.countQuads(...args), async rtn => rtn);
      },
      match(...args) {
        return _read(state => state.match(...args), async.wrap);
      },
      // @ts-ignore TS can't cope with the overloads
      query(query) {
        // @ts-ignore TS can't cope with the overloads
        return _read(state => state.query(query), async.wrap);
      }
    });
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
