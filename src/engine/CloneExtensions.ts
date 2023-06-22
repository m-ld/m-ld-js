import {
  Datatype, GraphSubject, MeldConstraint, MeldExtensions, MeldPlugin, MeldPreUpdate, MeldReadState
} from '../api';
import { Context, Subject } from '../jrql-support';
import { constraintFromConfig } from '../constraints';
import { DefaultList } from '../lseq/DefaultList';
import { combinePlugins, InitialApp, MeldApp, MeldConfig } from '../config';
import { M_LD, RDF, XS } from '../ns';
import { Logger } from 'loglevel';
import { OrmDomain, OrmSubject, OrmUpdating } from '../orm';
import { getIdLogger } from './logging';
import { ExtensionSubjectInstance, SingletonExtensionSubject } from '../orm/ExtensionSubject';
import { StateManaged } from './index';
import { byteArrayDatatype, jsonDatatype } from '../datatype';
import { Iri } from '@m-ld/jsonld';
import { JsonldContext } from './jsonld';

/**
 * Top-level aggregation of extensions. Created from the configuration and
 * runtime app initially; thereafter tracks extensions declared in the domain
 * data and installs them as required.
 */
export class CloneExtensions extends OrmDomain implements StateManaged, MeldExtensions {
  static async initial(
    config: MeldConfig,
    app: InitialApp,
    context: Context
  ) {
    const activeCtx = await JsonldContext.active(context);
    app.setExtensionContext?.({ config, app, context: activeCtx });
    const { indirectedData, agreementConditions, transportSecurity } = app;
    const constraints = await this.constraintsFromConfig(config, app.constraints, context);
    return new CloneExtensions({
      constraints,
      indirectedData: indirectedData?.bind(app),
      agreementConditions,
      transportSecurity
    }, config, app, activeCtx);
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
  private readonly combinedPlugins: MeldExtensions;
  private _extensions: MeldPlugin[];
  private _defaultList: DefaultList;

  private constructor(
    private readonly initial: MeldPlugin,
    config: MeldConfig,
    app: MeldApp,
    readonly context: JsonldContext
  ) {
    super({ config, app, context });
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
    this.combinedPlugins = combinePlugins(this._extensions = [initial]);
  }

  /**
   * Get the current or next available value, ready for use (or a rejection,
   * e.g. if the clone is shutting down). This might be called while one of the
   * update methods is in-progress.
   */
  ready = () => this.upToDate().then(() => this);

  onInitial(state: MeldReadState) {
    return this.updating(state, async orm => {
      // Load the top-level extensions subject into our cache
      await this.loadAllExtensions(orm);
      await this.updateExtensions();
    });
  }

  onUpdate(update: MeldPreUpdate, state: MeldReadState) {
    return this.updating(state, async orm => {
      // This will update the extension list and all the extension subjects
      await orm.updated(update);
      await this.updateExtensions();
    });
  }

  get constraints() {
    return withDefaults(this.combinedPlugins.constraints, {
      is: constraint => constraint instanceof DefaultList, get: () =>
        this._defaultList ??= new DefaultList(this.config['@id'])
    });
  }

  indirectedData = (datatype: Iri, property: Iri): Datatype | undefined => {
    const dt = this.combinedPlugins.indirectedData?.(datatype, property);
    if (dt == null) {
      if (datatype === RDF.JSON) return jsonDatatype;
      if (datatype === XS.base64Binary) return byteArrayDatatype;
    }
    return dt;
  };

  get agreementConditions() {
    return this.combinedPlugins.agreementConditions;
  }

  get transportSecurity() {
    return this.combinedPlugins.transportSecurity;
  }

  private async updateExtensions() {
    // Leave the 'initial' extensions in place
    this._extensions.splice(1, this._extensions.length,
      ...(await Promise.all(this.iterateExtensions())));
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
    for (let extSubject of this.extensionSubjects) {
      yield Promise.resolve(extSubject.singleton).catch(e => {
        this.log.warn('Failed to load extension', extSubject.className, e);
        return {}; // Empty extensions
      });
    }
  }
}

class ManagedExtensionSubject
  extends SingletonExtensionSubject<MeldPlugin & ExtensionSubjectInstance> {
  protected newInstance(src: GraphSubject, orm: OrmUpdating) {
    const extensions = super.newInstance(src, orm);
    extensions.setExtensionContext?.(orm.domain);
    return extensions;
  }
}

function *withDefaults<T>(
  extensions: Iterable<T> | undefined,
  ...defaults: { is: (ext: T) => boolean, get: () => T }[]
) {
  const foundDefault = Array<boolean>(defaults.length);
  for (let ext of extensions ?? []) {
    yield ext;
    defaults.forEach((d, i) => foundDefault[i] ||= defaults[i].is(ext));
  }
  // Ensure the defaults exist
  for (let i = 0; i < defaults.length; i++)
    if (!foundDefault[i])
      yield defaults[i].get();
}
