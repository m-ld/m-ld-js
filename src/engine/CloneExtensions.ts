import {
  ConstructMeldExtensions, MeldConstraint, MeldExtensions, MeldReadState, MeldUpdateBid
} from '../api';
import { Construct, Context, isList, List, Reference, Subject } from '../jrql-support';
import { constraintFromConfig } from '../constraints';
import { DefaultList } from '../constraints/DefaultList';
import { InitialApp, MeldApp, MeldConfig } from '../config';
import { M_LD } from '../ns';
import { getIdLogger } from './util';
import { Logger } from 'loglevel';
import { firstValueFrom } from 'rxjs';
import { castPropertyValue, propertyValue } from '../subjects';
import { updateSubject } from '../updates';

/**
 * Top-level aggregation of extensions. Created from the configuration and
 * runtime app initially; thereafter tracks extensions declared in the domain
 * data and installs them as required.
 */
export class CloneExtensions implements MeldExtensions {
  static async initial(config: MeldConfig, app: InitialApp, context: Context) {
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

  private log: Logger;
  private extensionDefList: List & Reference = { '@id': M_LD.extensions, '@list': [] };

  private constructor(
    private initial: MeldExtensions,
    private config: MeldConfig,
    private app: MeldApp
  ) {
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
  }

  get constraints() {
    return this._constraints();
  }

  private *_constraints() {
    let foundDefaultList = false;
    for (let ext of this.extensions()) {
      for (let constraint of ext.constraints ?? []) {
        yield constraint;
        foundDefaultList ||= constraint instanceof DefaultList;
      }
    }
    // Ensure the default list constraint exists
    if (!foundDefaultList)
      yield new DefaultList(this.config['@id']);
  }

  get agreementConditions() {
    return this._agreementConditions();
  }

  private *_agreementConditions() {
    for (let ext of this.extensions())
      yield *ext.agreementConditions ?? [];
  }

  get transportSecurity() {
    for (let ext of this.extensions())
      if (ext.transportSecurity != null)
        return ext.transportSecurity;
  }

  async initialise(state: MeldReadState) {
    // Load data-declared extensions
    const defList = await firstValueFrom(state.read<Construct>({
      '@construct': {
        '@id': M_LD.extensions,
        '@list': {
          '?': { // Variable index means load all indexes
            '@id': '?',
            '@type': M_LD.JS.commonJsModule,
            [M_LD.JS.require]: '?',
            [M_LD.JS.className]: '?'
          }
        }
      }
    }), { defaultValue: undefined });
    if (defList != null) {
      if (isList(defList))
        this.extensionDefList = defList;
      else
        this.log.warn(`${M_LD.extensions} is not a List: no extensions loaded`);
      // Instantiate the declared extensions
      this.instantiateNewModules();
    }
    // Now initialise all extensions
    for (let ext of this.extensions())
      await ext.initialise?.(state);
  }

  async onUpdate(update: MeldUpdateBid, state: MeldReadState) {
    // Capture any changes to the extensions
    updateSubject(this.extensionDefList, update);
    // Instantiate any new declared extensions, permissively
    // TODO: check for changed modules, not just new ones
    this.instantiateNewModules({ permissive: true });
    // Update the extensions themselves
    for (let ext of this.extensions())
      await ext.onUpdate?.(update, state);
  }

  private instantiateNewModules({ permissive }: { permissive?: true } = {}) {
    for (let cjsModuleDef of this.extCjsModuleDefs) {
      if (cjsModuleDef.__instance == null) {
        try {
          const cjsModule = propertyValue(cjsModuleDef, M_LD.JS.require, String);
          const className = propertyValue(cjsModuleDef, M_LD.JS.className, String);
          const construct: ConstructMeldExtensions = require(cjsModule)[className];
          cjsModuleDef.__instance = <any>new construct(this.config, this.app);
        } catch (e) {
          if (permissive) {
            this.log.warn(`CommonJS module ${cjsModuleDef[M_LD.JS.require]} ` +
              `class ${cjsModuleDef[M_LD.JS.className]} cannot be instantiated`);
            cjsModuleDef.__instance = {};
          } else {
            throw e;
          }
        }
      }
    }
  }

  private get extCjsModuleDefs() {
    return castPropertyValue(this.extensionDefList, Array, Subject);
  }

  private *extensions() {
    yield this.initial;
    for (let cjsModule of this.extCjsModuleDefs)
      yield (cjsModule.__instance ?? {}) as MeldExtensions;
  }
}