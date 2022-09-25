## Extensions

**m-ld** supports extensions to its core engine. You can choose which extensions to use in an app; some are bundled in the engine package, others you can write yourself.

Some extensions must be pre-selected by the app in order to connect a new clone to a domain of information. Other extensions can be declared in the data and loaded dynamically by the engine at runtime. This allows authors to add features to the shared information, without the need for a central authority over features. This is _decentralised extensibility_, similar to how a web page can declare scripts that extend its features in the browser. See our [introductory short paper](https://bit.ly/realtime-rdf-paper) for more about this vision.

The Javascript engine package bundles the following extensions – follow the links to learn more:

- [Remotes](#remotes) are pre-selected in the call to [clone](#clone), as described above.
- [Lists](https://spec.m-ld.org/#lists) have a default implementation, which is replaceable.
- [ACL Transport Security](/classes/meldacltransportsecurity.html) allows an app to encrypt and apply digital signatures to m-ld protocol network traffic.
- [Statutes](/classes/statutory.html) allow an app to require that certain changes, such as changes to access controls, are _agreed_ before they are shared in the domain.
- [Write Permissions](/classes/writepermitted.html) allow an app to add fine-grained access controls based on data [shapes](/classes/shape.html).

The extension's code module must be available to a global CommonJS-style `require` method in all clones using the Javascript engine. For bundled extensions:
- In Node.js, the module is packaged in `@m-ld/m-ld`; no additional configuration is required.
- In the browser, `require` is typically provided by the bundler. Since the module will be loaded dynamically, the bundler may need to be configured to guarantee the module is bundled, since it _may_ not be referenced statically by any code.

> ⚠️ Note that while it's possible to change extensions at runtime (by changing their declarations in the data), this may require coordination between clones, to prevent outdated clones from acting incorrectly in ways that could cause data corruption or compromise security. Consult the extension's documentation for safe operation.

### Writing Extensions

Extension code is executed as required by the core engine or by another extension. Besides remotes, there are currently three types of custom extension called by the core engine, defined in the [MeldExtensions](/interfaces/meldextensions.html) API interface. To write an extension to the core, you must implement one or more of these types.

Extensions can then be installed in two ways:
1. By providing the implementation in the `app` parameter of the [clone function](#clone).
2. By declaring a module in the domain information.

The first option is suitable when the data is always going to be used by the same app – because the app will always know it has to include the extension.

The second option is more suitable if the data may be shared to other apps, because the need for the extension is declared in the data itself, and apps can load it dynamically as required.

To write an extension to be declared in the domain data, you must additionally implement [MeldExtensions](/interfaces/meldextensions.html) with one or more of its methods, in a Javascript class. Then, to declare the extension in the domain, you write:

```json
{
  "@id": "http://m-ld.org/extensions",
  "@list": {
    "≪priority≫": {
      "@id": "≪your-extension-iri≫",
      "@type": "http://js.m-ld.org/CommonJSExport",
      "http://js.m-ld.org/#require": "≪your-extension-module≫",
      "http://js.m-ld.org/#class": "≪your-extension-class-name≫"
    }
  }
}
```

Breaking this down:
- `"http://m-ld.org/extensions"` is the identity of the extensions list. This is a constant that **m-ld** knows about (it's in the `m-ld.org` domain).
- The extensions list is declared as a [List](https://spec.m-ld.org/#lists), with the `@list` keyword, because extensions are ordered by priority.
- The value you give for `≪priority≫` (a number) will determine where in the list your extension appears. The highest priority is `"0"`. The value is decided by you, based on what you know about your extensions. In most cases it won't matter, since extensions shouldn't interfere with each other.
- The `"@type"` identifies this extension as a Javascript module loaded in [CommonJS](https://nodejs.org/docs/latest/api/modules.html) style. (We'll support ES6 modules in future.)
- The `"http://js.m-ld.org/#require"` and `"http://js.m-ld.org/#class"` properties tell the module loader how to instantiate your module class. It will call a global `require` function with the first, dereference the second and call `new` on it.

Most extensions will provide a static convenience function to generate this JSON, which can be called in the genesis clone ([example](/classes/writepermitted.html#declare)).

> 🚧 Please do [contact us](https://m-ld.org/hello/) if you would like to understand more about extensions.