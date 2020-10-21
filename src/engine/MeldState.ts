import { Context, Subject, Describe, Pattern, Update, Read, Write } from '../jrql-support';
import { Observable, Subscription } from 'rxjs';
import { map, take } from 'rxjs/operators';
import { flatten } from 'jsonld';
import { MeldUpdate, MeldState, Resource, any, MeldStateMachine, ReadResult, readResult, StateProc, UpdateProc } from '../api';
import { CloneEngine, EngineState, EngineUpdateProc, StateEngine } from './StateEngine';

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
