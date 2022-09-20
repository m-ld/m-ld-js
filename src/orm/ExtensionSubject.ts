import { OrmSubject } from './OrmSubject';
import { GraphSubject } from '../api';
import { M_LD } from '../ns';
import { Optional } from '../js-support';
import { MeldApp, MeldConfig } from '../config';

export type ExtensionInstanceConstructor<T> = { new(subject: ExtensionSubject<T>): T };
export type ExtensionEnvironment = { readonly config: MeldConfig, readonly app: MeldApp };

export class ExtensionSubject<T> extends OrmSubject {
  moduleType: string[];
  cjsModule: string | undefined;
  className: string;
  private _instance?: T;
  /** ERR_MODULE_NOT_FOUND or any constructor errors */
  private _loadErr: any;

  // noinspection JSUnusedGlobalSymbols – env is part of the interface
  constructor(
    src: GraphSubject,
    readonly env: ExtensionEnvironment
  ) {
    super(src);
    this.initSrcProperty(src, '@type', [Array, String], { local: 'moduleType' });
    this.initSrcProperty(src, M_LD.JS.require, [Optional, String], { local: 'cjsModule' });
    this.initSrcProperty(src, M_LD.JS.className, String, { local: 'className' });
  }

  /**
   * The loaded class instance. Only call this after updates have been applied.
   * @see OrmSubject.updated
   */
  get instance(): T {
    if (this._instance == null && this._loadErr == null) {
      if (!this.moduleType.includes(M_LD.JS.commonJsModule)) {
        this._loadErr = `${this.src['@id']}: Extension type ${this.moduleType} not supported.`;
      } else if (this.cjsModule == null) {
        this._loadErr = `${this.src['@id']}: CommonJS module declared with no id.`;
      } else {
        try {
          const Instance: ExtensionInstanceConstructor<T> =
            require(this.cjsModule)[this.className];
          this._instance = new Instance(this);
        } catch (e) {
          this._loadErr = e;
        }
      }
    }
    if (this._loadErr != null)
      throw this._loadErr;
    else
      return this._instance!;
  }

  protected setUpdated(result: unknown | Promise<unknown>) {
    delete this._instance;
    delete this._loadErr;
    super.setUpdated(result);
  }
}