import { OrmSubject } from './OrmSubject';
import { GraphSubject } from '../api';
import { M_LD } from '../ns';
import { Optional, propertyValue } from '../js-support';
import { OrmUpdating } from './OrmDomain';
import { Subject } from '../jrql-support';
import { Iri } from '@m-ld/jsonld';

/** @internal */
export type ExtensionInstanceConstructor<T> = {
  new(params: ExtensionInstanceParams): T
};
/** @internal */
type ExtensionInstanceParams = {
  src: GraphSubject,
  orm: OrmUpdating
};

/**
 * An extension subject defines a way to declare that subjects in the domain
 * should be represented by a certain Javascript class.
 *
 * Extension subjects can be declared using {@link declare}, and are used in one
 * of two modes:
 *
 * 1. To load a singleton from a graph subject having type
 * `http://js.m-ld.org/CommonJSExport`. In this case the extension subject (or a
 * subclass) should be instantiated directly using {@link OrmUpdating#get}, and
 * the singleton object obtained with {@link singleton}, e.g. with data:
 *   ```JSON
 *   {
 *     "@id": "myCustomSingleton",
 *     "@type": "http://js.m-ld.org/CommonJSExport",
 *     "http://js.m-ld.org/#require": "my-custom-module",
 *     "http://js.m-ld.org/#class": "MyExtClass"
 *   }
 *   ```
 *   The single custom instance for this class might be loaded with:
 *   ```typescript
 *   const extType = orm.get('myCustomSingleton',
 *     src => new ExtensionSubject<MyExtType>({ src, orm });
 *   const ext: MyExtType = extType.singleton;
 *   ```
 *
 * 2. To load an instance from a graph subject having a type that is itself of
 * type `http://js.m-ld.org/CommonJSExport`. This indirection allows for many
 * graph subjects to have custom type without re-declaring the module & export.
 * The {@link instance} method is provided to load the instance, e.g. with data:
 *   ```JSON
 *   {
 *     "@id": "myCustomInstance",
 *     "@type": {
 *       "@id": "myCustomClass",
 *       "@type": "http://js.m-ld.org/CommonJSExport",
 *       "http://js.m-ld.org/#require": "my-custom-module",
 *       "http://js.m-ld.org/#class": "MyExtClass"
 *     }
 *   }
 *   ```
 *   A custom instance for this class might be loaded with:
 *   ```typescript
 *   const ext: MyExtType = orm.get('myCustomInstance',
 *     src => ExtensionSubject.instance<MyExtType>({ src, orm });
 *   ```
 *
 * @typeParam T the expected instance type
 * @noInheritDoc
 * @category Experimental
 * @experimental
 */
export class ExtensionSubject<T> extends OrmSubject {
  /**
   * Extension subject declaration. Insert into the domain data to register a
   * module export to be instantiated by an extension subject.
   *
   * For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(ExtensionSubject.declare(
   *   'myCustomClass',
   *   'my-custom-module',
   *   'MyExtClass'
   * ));
   * ```
   *
   * @param [id] the new extension subject's identity. Omit to auto-generate.
   * @param moduleId the CommonJS module (must be accessible using `require`)
   * @param className the constructor name exported by the module
   */
  static declare = (id: Iri | undefined, moduleId: string, className: string): Subject => ({
    '@id': id,
    '@type': M_LD.JS.commonJsExport,
    [M_LD.JS.require]: moduleId,
    [M_LD.JS.className]: className
  });

  /**
   * Obtain an instance of a custom class declared in the data as the type of
   * the given graph subject.
   *
   * @param src the graph subject for which to obtain a custom subject instance
   * @param orm an ORM domain updating state to use for loading the type
   * @throws TypeError if the given graph subject does not have an extension type
   */
  static async instance<T extends OrmSubject>(
    { src, orm }: ExtensionInstanceParams
  ): Promise<T> {
    // Look for a type that is a module
    const allTypes = await orm.latch(state => Promise.all(
      propertyValue(src, '@type', Array, String).map(t => state.get(t))));
    const moduleTypes = allTypes.filter((src): src is GraphSubject => src != null &&
      propertyValue(src, '@type', Array, String).includes(M_LD.JS.commonJsExport));
    if (moduleTypes.length === 1)
      return (await orm.get(moduleTypes[0], src =>
        new ExtensionSubject<T>({ src, orm }))).instance(src);
    throw new TypeError(`Cannot resolve extension type for ${src['@id']}`);
  }

  /** @internal */
  moduleType: string[];
  /** @internal */
  cjsModule: string | undefined;
  /** @internal */
  className: string;

  private readonly orm: OrmUpdating;
  private _loaded?: {
    construct?: ExtensionInstanceConstructor<T>,
    singleton?: T,
    /** ERR_MODULE_NOT_FOUND or any constructor errors */
    err?: any,
  };

  /**
   * @type ExtensionInstanceConstructor<*>
   */
  constructor({ src, orm }: ExtensionInstanceParams) {
    super(src);
    this.orm = orm;
    this.initSrcProperty(src, '@type', [Array, String], { local: 'moduleType' });
    this.initSrcProperty(src, M_LD.JS.require, [Optional, String], { local: 'cjsModule' });
    this.initSrcProperty(src, M_LD.JS.className, String, { local: 'className' });
    // Caution: no async properties are allowed here, so that instance() can be
    // called synchronously
  }

  /**
   * @returns an instance of the loaded Javascript class
   * @see OrmSubject.updated
   */
  get singleton(): T {
    if (this.loaded.err != null)
      throw this.loaded.err;
    if (this.loaded.singleton == null) {
      const { src, orm } = this;
      this.loaded.singleton =
        new this.loaded.construct!({ src, orm });
    }
    return this.loaded.singleton;
  }

  protected setUpdated(result: unknown | Promise<unknown>) {
    delete this._loaded;
    // TODO: what about created instances, not just the singleton
    super.setUpdated(result);
  }

  /**
   * @returns an instance of the loaded Javascript class
   * @see OrmSubject.updated
   */
  private instance(src: GraphSubject, orm = this.orm): T {
    if (this.loaded.err != null)
      throw this.loaded.err;
    return new this.loaded.construct!({ src, orm });
  }

  private get loaded() {
    if (this._loaded == null) {
      this._loaded = {};
      if (!this.moduleType.includes(M_LD.JS.commonJsExport)) {
        this._loaded.err =
          `${this.src['@id']}: Extension type ${this.moduleType} not supported.`;
      } else if (this.cjsModule == null) {
        this._loaded.err =
          `${this.src['@id']}: CommonJS module declared with no id.`;
      } else {
        try {
          this._loaded.construct = require(this.cjsModule)[this.className];
        } catch (e) {
          this._loaded.err = e;
        }
      }
    }
    return this._loaded;
  }
}