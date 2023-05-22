import { GraphSubject, MeldConstraint, MeldExtensions, MeldPreUpdate, MeldReadState } from '../api';
import { Context, Subject } from '../jrql-support';
import { constraintFromConfig } from '../constraints';
import { DefaultList } from '../lseq/DefaultList';
import { InitialApp, MeldApp, MeldAppContext, MeldConfig } from '../config';
import { M_LD, RDF } from '../ns';
import { Logger } from 'loglevel';
import { OrmDomain, OrmSubject, OrmUpdating } from '../orm';
import { getIdLogger } from './logging';
import { ExtensionSubjectInstance, SingletonExtensionSubject } from '../orm/ExtensionSubject';
import { StateManaged } from './index';
import { iterable } from './util';
import { jsonDatatype } from '../datatype';
import { Iri } from '@m-ld/jsonld';

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
    app.setExtensionContext?.({ config, app });
    const { datatypes, agreementConditions, transportSecurity } = app;
    const constraints = await this.constraintsFromConfig(config, app.constraints, context);
    return new CloneExtensions({
      constraints,
      datatypes: datatypes?.bind(app),
      agreementConditions,
      transportSecurity
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
  private _extensions: MeldExtensions[];
  private _defaultList: DefaultList;

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
    this._extensions = [this.initial];
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
    return iterable(() => this.getConstraints());
  }

  *getConstraints() {
    yield *withDefaults(this._extensions, ext => ext.constraints, {
      is: constraint => constraint instanceof DefaultList, get: () =>
        this._defaultList ??= new DefaultList(this.config['@id'])
    });
  }

  datatypes = (id: Iri) => {
    for (let ext of this._extensions) {
      const dt = ext.datatypes?.(id);
      if (dt)
        return dt;
    }
    if (id === RDF.JSON)
      return jsonDatatype;
  }

  get agreementConditions() {
    return iterable(() => this.getAgreementConditions());
  }

  *getAgreementConditions() {
    for (let ext of this._extensions)
      yield *ext.agreementConditions ?? [];
  }

  get transportSecurity() {
    for (let ext of this._extensions)
      if (ext.transportSecurity != null)
        return ext.transportSecurity;
  }

  private async updateExtensions() {
    return this._extensions = await Promise.all(this.iterateExtensions());
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
  private readonly context: MeldAppContext;

  constructor(src: GraphSubject, orm: OrmUpdating) {
    super(src, orm);
    const { config, app } = orm.domain;
    this.context = { config, app };
  }

  protected newInstance() {
    const extensions = super.newInstance();
    extensions.setExtensionContext?.(this.context);
    return extensions;
  }
}

function *withDefaults<T>(
  extensions: Iterable<MeldExtensions>,
  getOfType: (ext: MeldExtensions) => Iterable<T> | undefined,
  ...defaults: { is: (ext: T) => boolean, get: () => T }[]
) {
  const foundDefault = Array<boolean>(defaults.length);
  for (let ext of extensions) {
    for (let extOfType of getOfType(ext) ?? []) {
      yield extOfType;
      defaults.forEach((d, i) => foundDefault[i] ||= defaults[i].is(extOfType));
    }
  }
  // Ensure the defaults exist
  for (let i = 0; i < defaults.length; i++)
    if (!foundDefault[i])
      yield defaults[i].get();
}
