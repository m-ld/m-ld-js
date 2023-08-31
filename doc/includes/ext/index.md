## Extensions

**m-ld** supports extensions to its core engine. You can choose which extensions to use in an app; some are bundled in the engine package, others you can write yourself.

Some extensions must be pre-selected by the app in order to connect a new clone to a domain of information. Other extensions can be declared in the data and loaded dynamically by the engine at runtime. This allows authors to add features to the shared information, without the need for a central authority over features. This is _decentralised extensibility_, similar to how a web page can declare scripts that extend its features in the browser. See our [introductory short paper](https://bit.ly/realtime-rdf-paper) for more about this vision.

The Javascript engine package bundles the following extensions – follow the links to learn more:

- [Remotes](#remotes) are pre-selected in the call to [clone](#clone), as described above.
- [Lists](https://spec.m-ld.org/#lists) have a default implementation, which is replaceable.
- [Shapes](/classes/shapeconstrained.html) are used to enforce a 'schema' or 'object model' on the domain's data.
- Subject properties in the domain can be declared as [collaborative text](/classes/tseqtext.html).
- [Transport Security](/classes/meldacltransportsecurity.html) allows an app to encrypt and apply digital signatures to **m-ld** protocol network traffic.
- [Statutes](/classes/statutory.html) allow an app to require that certain changes, such as changes to access controls, are _agreed_ before they are shared in the domain.
- [Write Permissions](/classes/writepermitted.html) allow an app to add fine-grained access controls based on data [shapes](/classes/shape.html).

The extension's code module must be available to a global CommonJS-style `require` method in all clones using the Javascript engine. For bundled extensions:
- In Node.js, the module is packaged in `@m-ld/m-ld`; no additional configuration is required.
- In the browser, `require` is typically provided by the bundler. Since the module will be loaded dynamically, the bundler may need to be configured to guarantee the module is bundled, since it _may_ not be referenced statically by any code.

> 💡 While it's possible to change extensions at runtime (by changing their declarations in the data), this may require coordination between clones, to prevent outdated clones from acting incorrectly in ways that could cause data corruption or compromise security. Consult the extension's documentation for safe operation.

### Writing Extensions

Extension code is executed as required by the core engine or by another extension. Besides remotes, there are currently four types of custom extension called by the core engine, defined in the [MeldExtensions](/interfaces/meldextensions.html) API interface. To write an extension to the core, you must implement one or more of these types.

> 💡 Please do [contact us](https://m-ld.org/hello/) if you would like to understand more about extensions.