import { MeldClone, MeldExtensions } from './api';
import { QuadStoreDataset } from './engine/dataset';
import { CloneExtensions } from './engine/CloneExtensions';
import { ApiStateMachine } from './engine/MeldState';
import { DatasetEngine } from './engine/dataset/DatasetEngine';
import { DomainContext } from './engine/MeldEncoding';
import type { InitialApp, MeldConfig } from './config';
import type { MeldRemotes } from './engine';
import type { LiveStatus } from '@m-ld/m-ld-spec';
import type { AbstractLevel } from 'abstract-level';
import { Stopwatch } from './engine/Stopwatch';

/**
 * Core API exports. Extension exports can be found in package.json/exports
 */
export * from './util';
export * from './api';
export * from './config';
export * from './updates';
export * from './subjects';
export * from './js-support';
export * from './jrql-support';

/**
 * Constructor for a driver for connecting to remote m-ld clones on the domain.
 * @internal
 */
export type ConstructRemotes = new (
  config: MeldConfig,
  extensions: () => Promise<MeldExtensions>
) => MeldRemotes;

/**
 * Create or initialise a local clone, depending on whether the given backend
 * already contains m-ld data. This function returns as soon as it is safe to
 * begin transactions against the clone; this may be before the clone has
 * received all updates from the domain. You can wait until the clone is
 * up-to-date using the {@link MeldClone.status} property.
 *
 * @param backend an instance of a leveldb backend
 * @param constructRemotes remotes constructor
 * @param config the clone configuration
 * @param [app] runtime options
 * @category API
 */
export async function clone(
  backend: AbstractLevel<any, any, any>,
  constructRemotes: ConstructRemotes,
  config: MeldConfig,
  app: InitialApp = {}
): Promise<MeldClone> {
  const { backendEvents } = app;
  if (backendEvents != null)
    Stopwatch.timingEvents.on('timing', e => backendEvents.emit('timing', e));
  const sw = new Stopwatch('clone', config['@id']);

  sw.next('dependencies');
  const context = new DomainContext(config['@domain'], config['@context']);
  const extensions = await CloneExtensions.initial(config, app, context);
  const remotes = new constructRemotes(config, extensions.ready);

  sw.next('dataset');
  const dataset = await new QuadStoreDataset(
    config['@domain'], backend, backendEvents).initialise(sw.lap);
  const engine = new DatasetEngine({
    dataset, remotes, extensions, config, app, context
  });

  sw.next('engine');
  await engine.initialise(sw.lap);
  sw.stop();

  return new DatasetClone(engine);
}

/** @internal */
class DatasetClone extends ApiStateMachine implements MeldClone {
  constructor(
    private readonly dataset: DatasetEngine
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