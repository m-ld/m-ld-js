import { Query, Read, Subject, SubjectProperty, Update, Write } from '../jrql-support';
import { EmptyError, Subscription } from 'rxjs';
import {
  any, MeldReadState, MeldState, MeldStateMachine, MeldStateSubscription, MeldUpdate, ReadResult,
  StateProc, UpdateProc
} from '../api';
import {
  CloneEngine, EngineState, EngineUpdateProc, EngineWrite, followUnsubscribed, StateEngine
} from './StateEngine';
import { QueryableRdfSourceProxy } from './quads';
import { first, inflateFrom, settled } from './util';
import { BaseDeleteInsert, QueryableRdfSource } from '../rdfjs-support';
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
    return this.construct(await this.state.write({ jrql: request }));
  }

  async updateQuads(update: BaseDeleteInsert): Promise<MeldState> {
    return this.construct(await this.state.write({ rdf: update }));
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
        return stateEngine.read(state => {
          // possibly an over-abundance of caution, because the proc should
          // signal error through its return type (promise, observable, or stream)
          try { resolve(proc(state)); } catch (e) { reject(e); }
        });
      }));
    }
    // The API state machine also pretends to be a state
    super({
      async write(request: EngineWrite) {
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

  follow(handler?: UpdateProc): MeldStateSubscription {
    const subs = new ApiStateSubscription(this.engine);
    if (handler != null)
      subs.add(this.engine.follow(this.updateHandler(handler)));
    return subs;
  }

  read<T>(procedure: StateProc<MeldReadState, T>, handler?: UpdateProc): MeldStateSubscription<T>;
  read<R extends Read = Read, S = Subject>(request: R): ReadResult;
  read<T>(
    request: Read | StateProc<MeldReadState, T>,
    handler?: UpdateProc
  ): ReadResult | MeldStateSubscription<T> {
    if (typeof request == 'function') {
      const subs = new ApiStateSubscription<T>(this.engine);
      subs.ready = this.engine.read(
        state => request(new ImmutableState(state)),
        subs,
        handler != null ? this.updateHandler(handler) : undefined
      );
      return subs;
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

  private updateHandler(handler: UpdateProc): EngineUpdateProc {
    return async (update, state) =>
      handler(update, new ImmutableState(state));
  }
}

class ApiStateSubscription<T = never> extends Subscription
  implements AsyncGenerator<[MeldUpdate, MeldReadState]>, PromiseLike<T> {
  private iteratorSubs?: Subscription;
  private doneUsingState?: () => void;
  private nextState?: (args: [MeldUpdate, MeldReadState]) => void;

  public ready?: Promise<T | typeof followUnsubscribed>;

  constructor(
    readonly engine: StateEngine
  ) {
    super(() => this.doneUsingState?.());
  }

  [Symbol.asyncIterator](): AsyncGenerator<[MeldUpdate, MeldReadState]> {
    return this;
  }

  async next() {
    // By calling next, the consumer indicates they're done with the prior state
    this.doneUsingState?.();
    if (this.closed)
      return { value: null, done: this.closed };
    if (this.ready != null)
      await settled(this.ready); // We don't care about the ready result
    if (this.closed)
      return { value: null, done: this.closed };
    // The first time next is called, we know for sure that iteration is wanted
    this.ensureFollowing();
    return ({
      value: await new Promise<[MeldUpdate, MeldReadState]>(
        resolve => this.nextState = resolve),
      done: false
    });
  }

  async return(value: any): Promise<IteratorResult<any>> {
    this.doneUsingState?.();
    this.iteratorSubs?.unsubscribe();
    return { value: value, done: true };
  }

  throw(err: any) {
    this.doneUsingState?.();
    this.iteratorSubs?.unsubscribe();
    return Promise.reject(err);
  }

  then: PromiseLike<T>['then'] = (onfulfilled, onrejected) =>
    this.ready != null ? this.ready.then(
      value => value === followUnsubscribed ?
        Promise.reject(EmptyError).then(onfulfilled, onrejected) :
        Promise.resolve(value).then(onfulfilled, onrejected),
      onrejected
    ) : Promise.reject(EmptyError).then(onfulfilled, onrejected);

  private ensureFollowing() {
    if (this.iteratorSubs == null)
      this.add(this.iteratorSubs = new Subscription(this.engine.follow(this.handler)));
  }

  private handler: EngineUpdateProc = async (update, state) => {
    this.nextState?.([update, new ImmutableState(state)]);
    await new Promise<void>(resolve => this.doneUsingState = resolve);
  };
}