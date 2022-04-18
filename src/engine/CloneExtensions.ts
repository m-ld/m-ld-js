import {
  GraphSubject, MeldConstraint, MeldExtensions, MeldPreUpdate, MeldReadState, StateManaged
} from '../api';
import { Context, Subject } from '../jrql-support';
import { constraintFromConfig } from '../constraints';
import { DefaultList } from '../constraints/DefaultList';
import { InitialApp, MeldApp, MeldConfig } from '../config';
import { M_LD } from '../ns';
import { getIdLogger } from './util';
import { Logger } from 'loglevel';
import { OrmDomain, OrmState, OrmSubject } from '../orm/index';
import { ExtensionSubject } from '../orm';

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
    // Take the initial constraints if provided
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
    private readonly config: MeldConfig,
    private readonly app: MeldApp
  ) {
    super();
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.extensionSubjects = [];
  }

  async initialise(state: MeldReadState) {
    await this.updating(state, async orm => {
      // Load the top-level extensions subject into our cache
      await this.loadExtensions(orm);
      await orm.update();
      // Now initialise all extensions
      for (let extSubject of this.extensionSubjects)
        await extSubject.initialiseOrUpdate(state);
    });
  }

  async onUpdate(update: MeldPreUpdate, state: MeldReadState) {
    await this.updating(state, async orm => {
      // This will update the extension list and all the extension subjects
      await orm.update(update, deleted => {
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

  private loadExtensions(orm: OrmState) {
    return orm.get({ '@id': M_LD.extensions }, src => {
      const { config, app, extensionSubjects } = this;
      return new class extends OrmSubject {
        constructor(src: GraphSubject) {
          super(src);
          this.initList(src, Subject, extensionSubjects,
            i => extensionSubjects[i].src,
            async (i, v: GraphSubject) => extensionSubjects[i] = await orm.get(v,
              src => new ManagedExtensionSubject(src, { config, app })));
        }
      }(src);
    });
  }

  private *extensions() {
    yield this.initial;
    for (let extSubject of this.extensionSubjects)
      if (extSubject.instance != null)
        yield extSubject.instance.ready();
  }
}

class ManagedExtensionSubject extends ExtensionSubject<StateManaged<MeldExtensions>> {
  private initialised = false;

  protected setUpdated(result: unknown | Promise<unknown>) {
    this.initialised = false;
    super.setUpdated(result);
  }

  async initialiseOrUpdate(state: MeldReadState, update?: MeldPreUpdate) {
    if (!this.initialised) {
      await this.instance.initialise?.(state);
      this.initialised = true;
    } else if (update != null) {
      await this.instance.onUpdate?.(update, state);
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
