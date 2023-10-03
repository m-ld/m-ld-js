Versions with a major version number of 0 are developer pre-releases.

> ⚠️ Unless otherwise stated, pre-release versions are not backwards-compatible
with prior versions for storage or network transmission, so they must be used
with data persisted using the same version, and other clones of the same
version. Please [get in touch](https://m-ld.org/hello) if you need help upgrading.

## v0.10

### [new]

- Overhaul of the Getting Started guide on [the website](https://js.m-ld.org/), including live coding examples.
- JavaScript [module](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Modules) bundles are now served on the `js.m-ld.org/ext` and `edge.js.m-ld.org/ext` websites.
- (The **m-ld** [Gateway](https://gw.m-ld.org/) provides messaging and backup, also helping with getting started.)
- Support for [live collaborative text](https://js.m-ld.org/classes/tseqtext.html) in **m-ld** domains.
- New extension point for custom [Datatypes](https://js.m-ld.org/interfaces/datatype.html) and [Shared Datatypes](https://js.m-ld.org/interfaces/shareddatatype.html), which allow embedding of custom atomic value types, which can be mutable CRDTs.
- Uint8Array (and Buffer) subject [property values](https://js.m-ld.org/globals.html#atom) are now supported in the API, without wrapping in a `base64Binary` JSON-LD value object. These are also stored efficiently in the backend.
- A new clause key, `@update`, is available when [writing Updates](https://js.m-ld.org/interfaces/update.html#_update), reducing boilerplate when replacing the contents of a subject property. It is also used for issuing updates to a shared datatype.
- Expanded RDF/JS support, including [writing RDF](https://js.m-ld.org/interfaces/updatablerdf.html) directly to a domain, and receiving [Quads in domain updates](https://js.m-ld.org/modules/graphsubjects.html#quads).

### [breaking]

- Major change to the inter-clone protocol, removing separate `newClock` requests, for integrity.
- The built Javascript is now to ES2021 standard.

### [bugfix]

- [#148](https://github.com/m-ld/m-ld-js/issues/148) Time ... is from a different process group
- [Bugfixes in v0.9.1](https://github.com/m-ld/m-ld-js/milestone/1?closed=1)
- [Bugfixes in v0.9.2](https://github.com/m-ld/m-ld-js/milestone/2?closed=1)

## v0.9

### [new]

- API: `ask` query in a read state to optimally discover if some pattern exists
- API: `propertyValue` type options include explicit `Subject`, `Reference` and `VocabReference`
  - **[breaking]**: `Object` is no longer allowed
 
### [breaking]

- Upgraded to the new generation of `*-level` packages (see [upgrade guide](https://github.com/Level/community#how-do-i-upgrade-to-abstract-level))
  - `MeldMemDown` is no longer required or available, instead please use `MemoryLevel` from [`memory-level`](https://github.com/Level/memory-level).
- The `dist` package, containing remotes and other extensions, has been renamed to `ext`. For example, now use `import '@m-ld/m-ld/ext/socket.io'`.
- Various protocol and persistence improvements & fixes.

### [bugfix]

- [#103](https://github.com/m-ld/m-ld-js/issues/103) Pre-snapshot duplicated inserts can cause divergence
- [#95](https://github.com/m-ld/m-ld-js/issues/95) Cannot change context per-session

### [experimental]

> If these features sound useful, please [contact us](https://m-ld.org/hello/) to discuss your use-case.

- [Object-Resource Mapping](https://edge.js.m-ld.org/classes/ormdomain.html) is a higher-level API to support idiomatic use of object-oriented Javascript with the core m-ld API.
- [Extensions](https://edge.js.m-ld.org/#extensions) can be declared in the data and loaded dynamically using CommonJS.
- An [Agreement](https://edge.js.m-ld.org/interfaces/update.html#_agree) is a new concurrency primitive that forces convergence on a specific data state.

## v0.8

- Query usability improvements: more intuitive
  [delete-where](https://github.com/m-ld/m-ld-spec/issues/76) &
  [vocabulary&nbsp;references](https://github.com/m-ld/m-ld-spec/issues/77)
- API support for native RDFJS Dataset
  [Source](https://rdf.js.org/stream-spec/#source-interface) and queries using
  [SPARQL&nbsp;algebra](https://github.com/joachimvh/SPARQLAlgebra.js#algebra-object),
  for projects using RDF natively.
- Engine performance improvements, including faster `@describe` queries on
  larger datasets.
- API support for back-pressuring read results. This better supports
  asynchronous results consumers, such as agents that update remote data sinks
  like databases.
- Experimental support for whole-domain read/write
  [access&nbsp;control](https://github.com/m-ld/m-ld-js/pull/85), with users
  registered in the domain data.

## v0.7

- Protocol support for journal compaction, allowing much-reduced storage 
  costs with compaction and truncation strategies. A 'balanced' journaling 
  strategy with
  [simple options](https://js.m-ld.org/interfaces/journalconfig.html) is the 
  default for the Javascript engine.
- [Socket.io remotes](https://js.m-ld.org/#socketio-remotes) added for apps 
  with live web servers.

## v0.6

- Expanded **json-rql** support: `@construct`, `@filter` (with operators) and
  `@values`.
- [#69](https://github.com/m-ld/m-ld-js/issues/69) List updates are given using
  the `@list` key and indexed-object notation, with explicit slots [breaking].
- Performance improvements to all operations by use of synchronous JSON-LD
  compaction and expansion.
- WebRTC support for the Ably remotes engine [experimental].

## v0.5

This version includes an overhaul of the experimental Constraints API and a
prototype implementation of multi-collaborator editable Lists.

This version is backwards-compatible with previous versions for network format
but not storage format.

## v0.4

This version introduces a new pattern for reads and writes to the clone, that
better represents clone data state immutability guarantees, without relying on
the Javascript runtime behaviour. See the
[concurrency&nbsp;documentation](https://js.m-ld.org/#concurrency).

## v0.3

This version includes some improvements to how **m-ld** stores and transmits
changes, which drops the bandwidth overhead considerably for transactions that
impact more data. The storage is backwards-compatible, but the messaging is not,
so you need to ensure that if any clone uses the v0.3 engine, all the other
clones do too.