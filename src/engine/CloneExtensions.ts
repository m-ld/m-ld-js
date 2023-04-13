import { GraphSubject, MeldConstraint, MeldExtensions, MeldPreUpdate, MeldReadState } from '../api';
import { Context, Subject } from '../jrql-support';
import { constraintFromConfig } from '../constraints';
import { DefaultList } from '../lseq/DefaultList';
import { InitialApp, MeldApp, MeldConfig } from '../config';
import { M_LD } from '../ns';
import { Logger } from 'loglevel';
import { OrmDomain, OrmSubject, OrmUpdating } from '../orm';
import { getIdLogger } from './logging';
import { ExtensionSubjectInstance, SingletonExtensionSubject } from '../orm/ExtensionSubject';
import { StateManaged } from './index';

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

  private constructor(
    private readonly initial: MeldExtensions,
    config: MeldConfig,
    app: MeldApp
  ) {
    super({ config, app });
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.extensionSubjects = [];
    this.scope.on('deleted', deleted => {
      if (deleted.src['@id'] === M_LD.extensions)
        // The whole extensions object has been deleted!
        this.extensionSubjects.length = 0;
    });
    this.scope.on('cacheMiss', async (insert, orm) => {
      // This catches the case there were no extensions in the initialise
      if (insert['@id'] === M_LD.extensions)
        // OrmSubject cannot cope with list update syntax, so load from state
        await this.loadAllExtensions(orm);
    });
  }

  ready = () => this.upToDate().then(() => {
    const id = this.config['@id'];
    return Promise.all(this.iterateExtensions()).then(extensions => ({
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

  initialise(state: MeldReadState) {
    return this.updating(state, async orm => {
      // Load the top-level extensions subject into our cache
      await this.loadAllExtensions(orm);
    });
  }

  onUpdate(update: MeldPreUpdate, state: MeldReadState) {
    return this.updating(state, async orm => {
      // This will update the extension list and all the extension subjects
      await orm.updated(update);
    });
  }

  private async loadAllExtensions(orm: OrmUpdating) {
    await orm.get({ '@id': M_LD.extensions }, src => {
      const { extensionSubjects } = this;
      return new class extends OrmSubject {
        constructor(src: GraphSubject) {
          super(src);
          this.initSrcList(src, Subject, extensionSubjects, {
            get: i => extensionSubjects[i].src,
            set: async (i, v: GraphSubject) => extensionSubjects[i] = await orm.get(v,
              src => new ManagedExtensionSubject(src, orm))
          });
        }
      }(src);
    });
    await orm.updated();
  }

  private *iterateExtensions() {
    yield this.initial;
    for (let extSubject of this.extensionSubjects) {
      try {
        yield extSubject.singleton;
      } catch (e) {
        this.log.warn('Failed to load extension', extSubject.className, e);
      }
    }
  }
}

class ManagedExtensionSubject
  extends SingletonExtensionSubject<MeldExtensions & ExtensionSubjectInstance> {
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
