import { QuadStoreDataset } from './engine/dataset';
import { DatasetEngine } from './engine/dataset/DatasetEngine';
import { ApiStateMachine } from './engine/MeldState';
import { DomainContext } from './engine/MeldEncoding';
import { Context } from './jrql-support';
import { MeldClone, MeldExtensions } from './api';
import { CloneExtensions, MeldRemotes } from './engine';
import type { LiveStatus } from '@m-ld/m-ld-spec';
import type { MeldApp, MeldConfig } from './config';
import type { AbstractLevelDOWN } from 'abstract-leveldown';
import { Stopwatch } from './engine/util';

export {
  Pattern, Reference, Context, Variable, Value, Describe, Construct,
  Group, Query, Read, Result, Select, Subject, Update,
  isRead, isWrite
} from './jrql-support';

export * from './util';
export * from './api';
export * from './config';
export * from './updates';
export * from './subjects';

/**
 * Constructor for a driver for connecting to remote m-ld clones on the domain.
 */
type ConstructRemotes = new (config: MeldConfig, extensions: MeldExtensions) => MeldRemotes;

/**
 * Create or initialise a local clone, depending on whether the given LevelDB
 * database already exists. This function returns as soon as it is safe to begin
 * transactions against the clone; this may be before the clone has received all
 * updates from the domain. You can wait until the clone is up-to-date using the
 * {@link MeldClone.status} property.
 *
 * @param backend an instance of a leveldb backend
 * @param constructRemotes remotes constructor
 * @param config the clone configuration
 * @param [app] runtime options
 */
export async function clone(
  backend: AbstractLevelDOWN,
  constructRemotes: ConstructRemotes,
  config: MeldConfig,
  app: MeldApp = {}
): Promise<MeldClone> {
  const { backendEvents } = app;
  if (backendEvents != null)
    Stopwatch.timingEvents.on('timing', e => backendEvents.emit('timing', e));
  const sw = new Stopwatch('clone', config['@id']);

  sw.next('extensions');
  const context = new DomainContext(config['@domain'], config['@context']);
  const extensions = await CloneExtensions.initial(config, app, context);
  const remotes = new constructRemotes(config, extensions);

  sw.next('dataset');
  const dataset = await new QuadStoreDataset(
    backend, context, backendEvents).initialise(sw.lap);
  const engine = new DatasetEngine({
    dataset, remotes, extensions, config, context
  });

  sw.next('engine');
  await engine.initialise(sw.lap);
  sw.stop();
  return new DatasetClone(engine, extensions);
}

/** @internal */
class DatasetClone extends ApiStateMachine implements MeldClone {
  constructor(
    private readonly dataset: DatasetEngine,
    readonly extensions: MeldExtensions
  ) {
    super(dataset);
  }

  get status(): LiveStatus {
    return this.dataset.status;
  }

  close(err?: any): Promise<unknown> {
    return this.dataset.close(err);
  }
}