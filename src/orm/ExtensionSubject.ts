import { OrmSubject, property } from './OrmSubject';
import { GraphSubject } from '../api';
import { M_LD } from '../ns';
import { JsAtomType, JsType, noMerge, Optional } from '../js-support';
import { OrmUpdating } from './OrmDomain';
import { Subject, SubjectProperty } from '../jrql-support';
import type { Iri } from '@m-ld/jsonld';

/**
 * An extension, created via an {@link ExtensionSubject}
 */
export interface ExtensionSubjectInstance {
  /**
   * Initialise the instance from domain data
   * @param src the graph subject of the instance
   * @param orm an ORM domain updating state
   * @param ext the extension type subject
   */
  initFromData?(
    src: GraphSubject,
    orm: OrmUpdating,
    ext: ExtensionSubject<this>
  ): void;
  /**
   * This instance is being discarded. Implement to dispose resources.
   */
  invalidate?(): void;
}

/**
 * Dynamically require the given CommonJS module. In some packagers, like
 * browserify, a call to `require` only resolves static modules. In that case,
 * allow for a global `require` on the window object.
 *
 * @param cjsModule
 */
function dynamicRequire(cjsModule: string) {
  try {
    return require(cjsModule);
  } catch (e) {
    if (e.code === 'MODULE_NOT_FOUND' && typeof globalThis.require == 'function')
      return globalThis.require(cjsModule);
    throw e;
  }
}

/**
 * An extension subject defines a way to declare that subjects in the domain
 * should be represented by a certain Javascript class.
 *
 * Extension subjects can be declared using {@link declare}, and are used in one
 * of two modes:
 *
 * 1. To load a singleton from a graph subject having type
 * `http://js.m-ld.org/CommonJSExport`. In this case a SingletonExtensionSubject
 * (or a subclass) should be instantiated directly using {@link
  * OrmUpdating#get}, and the singleton object obtained with {@link singleton},
 * e.g. with data:
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
 *     src => new SingletonExtensionSubject<MyExtType>({ src, orm });
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
export class ExtensionSubject<T extends ExtensionSubjectInstance> extends OrmSubject {
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
   * @param properties additional properties
   */
  static declare = (
    id: Iri | undefined,
    moduleId: string,
    className: string,
    properties?: Subject
  ): Subject => ({
    '@id': id,
    '@type': M_LD.JS.commonJsExport,
    [M_LD.JS.require]: moduleId,
    [M_LD.JS.className]: className,
    ...properties
  });

  /**
   * Shorthand declaration for m-ld extensions in this package
   * @internal
   */
  static declareMeldExt = (
    extModule: string,
    className: string,
    properties?: Subject
  ) => ExtensionSubject.declare(
    M_LD.EXT.extensionType(extModule, className),
    `@m-ld/m-ld/ext/${extModule}`,
    className,
    properties);

  /**
   * Obtain an instance of a custom class declared in the data as the type of
   * the given graph subject.
   * @throws TypeError if the given graph subject does not have an extension type
   */
  static async instance<T>(
    src: GraphSubject,
    orm: OrmUpdating
  ): Promise<T> {
    // Look for a type that is a module
    const allTypes = await orm.latch(state => Promise.all(
      OrmSubject['@type'].value(src).map(t => state.get(t))));
    const moduleTypes = allTypes.filter((src): src is GraphSubject => src != null &&
      OrmSubject['@type'].value(src).includes(M_LD.JS.commonJsExport));
    if (moduleTypes.length === 1) {
      return (await orm.get(moduleTypes[0], src =>
        new ExtensionSubject<T & ExtensionSubjectInstance>(src))).instance(src, orm);
    }
    throw new TypeError(`Cannot resolve extension type for ${src['@id']}`);
  }

  /** @internal */
  @property(JsType.for(Array, String), '@type')
  moduleType: string[];
  /** @internal */
  @property(JsType.for(Optional, String), M_LD.JS.require)
  cjsModule: string | undefined;
  /** @internal */
  @property(new JsAtomType(String, noMerge), M_LD.JS.className)
  className: string;

  protected static factoryProps: SubjectProperty[] =
    ['moduleType', 'cjsModule', 'className'];

  private _factory?: {
    construct?: new () => T,
    /** ERR_MODULE_NOT_FOUND or any constructor errors */
    err?: any,
  };

  protected constructor(src: GraphSubject) {
    super(src);
    // Caution: no async properties are allowed here, so that .singleton and
    // .instance() can be called synchronously
    this.initSrcProperties(src);
  }

  protected onPropertyUpdated(property: SubjectProperty, result: unknown | Promise<unknown>) {
    if (ExtensionSubject.factoryProps.includes(property))
      this.invalidate();
    // TODO: invalidate created instances
    super.onPropertyUpdated(property, result);
  }

  protected invalidate() {
    delete this._factory;
  }

  protected get factory() {
    if (this._factory == null) {
      this._factory = {};
      if (!this.moduleType.includes(M_LD.JS.commonJsExport)) {
        this._factory.err =
          `${this.src['@id']}: Extension type ${this.moduleType} not supported.`;
      } else if (this.cjsModule == null) {
        this._factory.err =
          `${this.src['@id']}: CommonJS module declared with no id.`;
      } else {
        try {
          this._factory.construct = dynamicRequire(this.cjsModule)[this.className];
        } catch (e) {
          this._factory.err = e;
        }
      }
    }
    return this._factory;
  }

  /**
   * @returns an instance of the loaded Javascript class
   * @see OrmSubject.updated
   */
  private instance(src: GraphSubject, orm: OrmUpdating): T | Promise<T> {
    if (this.factory.err != null)
      throw this.factory.err;
    return this.initialisedInstance(src, orm);
  }

  /** Override to provide custom post-initialisation behaviour */
  protected initialisedInstance(src: GraphSubject, orm: OrmUpdating) {
    const inst = this.newInstance();
    inst.initFromData?.(src, orm, this);
    return inst;
  }

  /** Override to provide custom pre-initialisation behaviour */
  protected newInstance() {
    return new this.factory.construct!();
  }
}

/**
 * Directly-instantiable extension subject for singleton extensions.
 * @see ExtensionSubject
 */
export class SingletonExtensionSubject<T extends ExtensionSubjectInstance>
  extends ExtensionSubject<T> {
  private _singleton?: T | Promise<T>;

  constructor(src: GraphSubject, orm: OrmUpdating) {
    super(src);
    this.reload(src, orm);
  }

  get singleton() {
    return this._singleton!;
  }

  onPropertiesUpdated(orm: OrmUpdating) {
    this.reload({ ...this.src }, orm);
  }

  protected invalidate() {
    super.invalidate();
    Promise.resolve(this._singleton!)
      .then(instance => instance.invalidate?.())
      .catch(); // Singleton failed to initialise
    delete this._singleton;
  }

  private reload(src: GraphSubject, orm: OrmUpdating) {
    if (this.factory.err != null)
      throw this.factory.err;
    if (this._singleton == null)
      this._singleton = this.initialisedInstance(src, orm);
  }
}