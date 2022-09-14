Version with a major version number of 0 are developer pre-releases.

> ⚠️ Unless otherwise stated, pre-release versions are not backwards-compatible
with prior versions for storage or network transmission, so they must be used
with data persisted using the same version, and other clones of the same
version. Please [get in touch](https://m-ld.org/hello) if you need help upgrading.

## v0.9

- _[new]_ API improvements
  - `propertyValue` type options include explicit `Subject`, `Reference` and `VocabReference` (**[breaking]**: `Object` is no longer allowed)
  - `ask` query in a read state to optimally discover if some pattern exists
- **[breaking]** upgraded to the new generation of `*-level` packages (https://github.com/Level/community#how-do-i-upgrade-to-abstract-level)
  - `MeldMemDown` is no longer required or available, instead please use `MemoryLevel` from [`memory-level`](https://github.com/Level/memory-level).
- **[breaking]** the `dist` package, containing remotes and other extensions, has been renamed to `ext`. For example, now use `import '@m-ld/m-ld/ext/socket.io'`.

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