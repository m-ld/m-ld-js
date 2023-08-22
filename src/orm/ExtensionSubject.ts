import { OrmSubject, property } from './OrmSubject';
import { GraphSubject } from '../api';
import { M_LD } from '../ns';
import { JsAtomType, JsProperty, JsType, noMerge, Optional } from '../js-support';
import { OrmUpdating } from './OrmDomain';
import { Subject, SubjectProperty } from '../jrql-support';
import type { Iri } from '@m-ld/jsonld';

/**
 * An extension, created via an {@link ExtensionSubject}
 * @category Experimental
 * @experimental
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
 * @internal
 */
async function dynamicRequire(cjsModule: string): Promise<unknown> {
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
   * @param requireId the CommonJS module, must be accessible using `require`
   * @param importId the ECMAScript module, must be accessible using `import`
   * @param className the constructor name exported by the module
   * @param properties additional properties
   */
  static declare = (
    id: Iri | undefined,
    requireId: string | undefined,
    importId: string | undefined,
    className: string,
    properties?: Subject
  ): Subject => ({
    '@id': id,
    [M_LD.JS.require]: requireId,
    [M_LD.JS.module]: importId,
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
    `@m-ld/m-ld/ext/${extModule}.js`,
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
    // Look for a type that we can load
    const allTypes = await orm.latch(state => Promise.all(
      OrmSubject['@type'].value(src).map(t => state.get(t))));
    const moduleTypes = allTypes.filter((src): src is GraphSubject =>
      src != null && (M_LD.JS.require in src || M_LD.JS.module in src));
    if (moduleTypes.length === 1) {
      return (await orm.get(moduleTypes[0], src =>
        new ExtensionSubject<T & ExtensionSubjectInstance>(src))).instance(src, orm);
    }
    throw new TypeError(`Cannot resolve extension type for ${src['@id']}`);
  }

  /** Utility property definition for CommonJs `require` key */
  static '@type' = new JsProperty('@type', JsType.for(Array, String));

  /** @internal */
  @property(JsType.for(Optional, String), M_LD.JS.require)
  cjsModule: string | undefined;
  /** @internal */
  @property(JsType.for(Optional, String), M_LD.JS.module)
  esmModule: string | undefined;
  /** @internal */
  @property(new JsAtomType(String, noMerge), M_LD.JS.className)
  className: string;

  protected static factoryProps: SubjectProperty[] =
    ['moduleType', 'cjsModule', 'esmModule', 'className'];

  /** May reject with ERR_MODULE_NOT_FOUND or any constructor errors */
  private _factory?: Promise<new () => T>;

  protected constructor(src: GraphSubject) {
    super(src);
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
      const tryEsm = (e: any = `${this.src['@id']}: Extension declared with no module.`) => {
        if (this.esmModule != null)
          return import(this.esmModule);
        else
          throw e;
      };
      this._factory = (this.cjsModule != null ?
        dynamicRequire(this.cjsModule).catch(tryEsm) : tryEsm())
        .then(module => (<any>module)[this.className] as new () => T);
      this._factory.catch(); // Ensure no unhandled exceptions
    }
    return this._factory;
  }

  /**
   * Override to provide custom post-initialisation behaviour
   * @returns an instance of the loaded Javascript class
   * @see OrmSubject.updated
   */
  protected async instance(src: GraphSubject, orm: OrmUpdating): Promise<T> {
    const inst = await this.newInstance(src, orm);
    inst.initFromData?.(src, orm, this);
    return inst;
  }

  /** Override to provide custom pre-initialisation behaviour */
  protected async newInstance(_src: GraphSubject, _orm: OrmUpdating) {
    return new (await this.factory)();
  }
}

/**
 * Extension subject for singleton extensions.
 *
 * Note care must be taken not to expose this subject to updates before the
 * delegate instance (the singleton) has had a chance to attach its source
 * properties. Thus, constructing an instance of this class should always await
 * {@link ready}, even if the delegate instance is not immediately required.
 *
 * @see ExtensionSubject
 * @category Experimental
 * @experimental
 */
export abstract class SingletonExtensionSubject<T extends ExtensionSubjectInstance>
  extends ExtensionSubject<T> {
  private _singleton?: Promise<T>;

  protected constructor(src: GraphSubject, orm: OrmUpdating) {
    super(src);
    this.reload(src, orm);
  }

  get ready(): Promise<this> {
    return this.singleton.then(() => this);
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
    if (this._singleton == null)
      this._singleton = this.instance(src, orm);
  }
}