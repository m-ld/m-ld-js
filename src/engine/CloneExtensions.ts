import {
  GraphSubject, MeldConstraint, MeldExtensions, MeldPreUpdate, MeldReadState, StateManaged
} from '../api';
import { Context, Subject } from '../jrql-support';
import { constraintFromConfig } from '../constraints';
import { DefaultList } from '../constraints/DefaultList';
import { InitialApp, MeldApp, MeldConfig } from '../config';
import { M_LD } from '../ns';
import { Logger } from 'loglevel';
import { OrmDomain, OrmSubject, OrmUpdating } from '../orm/index';
import { ExtensionSubject } from '../orm';
import { getIdLogger } from './logging';

/**
 * Top-level aggregation of extensions. Created from the configuration and
 * runtime app initially; thereafter tracks extensions declared in the domain
 * data and installs them as required.
 */
export class CloneExtensions extends OrmDomain implements StateManaged<MeldExtensions> {
  static async initial(
    config: MeldConfig,
    app: InitialApp,
    context: Context
  ) {
    return new CloneExtensions({
      constraints: await this.constraintsFromConfig(config, app.constraints, context),
      transportSecurity: app.transportSecurity
    }, config, app);
  }

  private static async constraintsFromConfig(
    config: MeldConfig,
    initial: Iterable<MeldConstraint> | undefined,
    context: Context
  ) {
    // noinspection JSDeprecatedSymbols - initial constraints, if provided
    return ([...initial ?? []]).concat(await Promise.all((config.constraints ?? [])
      .map(item => constraintFromConfig(item, context))));
  }

  private readonly log: Logger;
  /** Represents the `@list` of the global `M_LD.extensions` list subject  */
  private readonly extensionSubjects: ManagedExtensionSubject[];

  ready = () => this.upToDate().then(() => {
    const id = this.config['@id'];
    return Promise.all(this.extensions()).then(extensions => ({
      get constraints() {
        return constraints(extensions, id);
      },
      get agreementConditions() {
        return agreementConditions(extensions);
      },
      get transportSecurity() {
        return transportSecurity(extensions);
      }
    }));
  });

  private constructor(
    private readonly initial: MeldExtensions,
    config: MeldConfig,
    app: MeldApp
  ) {
    super(config, app);
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.extensionSubjects = [];
  }

  initialise(state: MeldReadState) {
    return this.updating(state, async orm => {
      // Load the top-level extensions subject into our cache
      await this.loadExtensions(orm);
      // Now initialise all extensions
      for (let extSubject of this.extensionSubjects)
        await extSubject.initialiseOrUpdate(state);
    });
  }

  onUpdate(update: MeldPreUpdate, state: MeldReadState) {
    return this.updating(state, async orm => {
      // This will update the extension list and all the extension subjects
      await orm.updated(update, deleted => {
        if (deleted.src['@id'] === M_LD.extensions)
          // The whole extensions object has been deleted!
          this.extensionSubjects.length = 0;
      }, async insert => {
        // This catches the case there were no extensions in the initialise
        if (insert['@id'] === M_LD.extensions)
          // OrmSubject cannot cope with list update syntax, so load from state
          await this.loadExtensions(orm);
      });
      // Update or initialise the extensions themselves
      for (let extSubject of this.extensionSubjects)
        await extSubject.initialiseOrUpdate(state, update);
    });
  }

  private async loadExtensions(orm: OrmUpdating) {
    await orm.get({ '@id': M_LD.extensions }, src => {
      const { extensionSubjects } = this;
      return new class extends OrmSubject {
        constructor(src: GraphSubject) {
          super(src);
          this.initList(src, Subject, extensionSubjects, {
            get: i => extensionSubjects[i].src,
            set: async (i, v: GraphSubject) => extensionSubjects[i] = await orm.get(v,
              src => new ManagedExtensionSubject({ src, orm }))
          });
        }
      }(src);
    });
    await orm.updated();
  }

  private *extensions() {
    yield this.initial;
    for (let extSubject of this.extensionSubjects) {
      try {
        yield extSubject.singleton.ready();
      } catch (e) {
        this.log.warn('Failed to load extension', extSubject.className, e);
      }
    }
  }
}

class ManagedExtensionSubject extends ExtensionSubject<StateManaged<MeldExtensions>> {
  private initialised = false;

  protected setUpdated(result: unknown | Promise<unknown>) {
    this.initialised = false;
    super.setUpdated(result);
  }

  async initialiseOrUpdate(state: MeldReadState, update?: MeldPreUpdate) {
    await this.updated;
    if (!this.initialised) {
      await this.singleton.initialise?.(state);
      this.initialised = true;
    } else if (update != null) {
      await this.singleton.onUpdate?.(update, state);
    } else {
      throw new Error('No update available');
    }
  }
}

function *constraints(
  extensions: Iterable<MeldExtensions>,
  id: string
) {
  let foundDefaultList = false;
  for (let ext of extensions) {
    for (let constraint of ext.constraints ?? []) {
      yield constraint;
      foundDefaultList ||= constraint instanceof DefaultList;
    }
  }
  // Ensure the default list constraint exists
  if (!foundDefaultList)
    yield new DefaultList(id);
}

function *agreementConditions(extensions: Iterable<MeldExtensions>) {
  for (let ext of extensions)
    yield *ext.agreementConditions ?? [];
}

function transportSecurity(extensions: Awaited<MeldExtensions>[]) {
  for (let ext of extensions)
    if (ext.transportSecurity != null)
      return ext.transportSecurity;
}
