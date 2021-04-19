## v0.6
- Expanded **json-rql** support: `@construct`, `@filter` (with operators) and
  `@values`.
- [#69](https://github.com/m-ld/m-ld-js/issues/69) List updates are given using
  the `@list` key and indexed-object notation, with explicit slots [breaking].
- Performance improvements to all operations by use of synchronous JSON-LD
  compaction and expansion.
- WebRTC support for the Ably remotes engine [experimental].

This version is not backwards-compatible with previous versions for storage or
network transmission, so it cannot be used with old persisted data or with other
non-upgraded clones.

## v0.5
This version includes an overhaul of the experimental Constraints API and a
prototype implementation of multi-collaborator editable Lists.

Due to changes in the persistence library this version is not
backwards-compatible with previous versions for storage, so it cannot be used
with old persisted data or with other non-upgraded clones.

## v0.4
This version introduces a new pattern for reads and writes to the clone, that
better represents clone data state immutability guarantees, without relying on
the Javascript runtime behaviour. See the
[concurrency&nbsp;documentation](https://js.m-ld.org/#concurrency).

This version is not backwards-compatible with previous versions for storage or
network transmission, so it cannot be used with old persisted data or with other
non-upgraded clones.

## v0.3
This version includes some improvements to how **m-ld** stores and transmits
changes, which drops the bandwidth overhead considerably for transactions that
impact more data. The storage is backwards-compatible, but the messaging is not,
so you need to ensure that if any clone uses the v0.3 engine, all the other
clones do too.