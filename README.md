<pre></pre>
<!--suppress HtmlDeprecatedAttribute -->
<p align="center">
  <a href="https://m-ld.org/">
    <img alt="m-ld" src="https://m-ld.org/m-ld.svg" width="300em" />
  </a>
</p>
<pre></pre>

# **m-ld** Javascript Engine
**m-ld** is a decentralised live information sharing component with a JSON-based
API.

This repository is the code of the Javascript engine for **m-ld**, for node.js,
modern browsers and other Javascript platforms. Typescript is supported and
recommended.

- [Discussion](https://github.com/m-ld/m-ld-spec/discussions)
- [Change Log](./CHANGELOG.md)

## ci & website
The project and [documentation](./doc) are built using typedoc, and delivered to
the engine documentation website at https://js.m-ld.org/ using
[Vercel](https://vercel.com/m-ld/m-ld-js). This build includes unit testing.

## work in progress
- Issues relating to this engine are logged on the Issues tab.
- Contributions are welcome! Contributed work is governed according to a
  [CAA](./CONTRIBUTING), the GitHub Community
  [Guidelines](https://docs.github.com/articles/github-community-guidelines),
  and the [privacy](https://m-ld.org/privacy/) policy.
  
## extensions
Plug-ins such as remotes implementations in this repository are structured as:
- Directories under `src`
- Entries in `package.json#exports` like `"./ext/socket.io": "./ext/socket.
  io/index.js"`
- Dependencies in `package.json#peerDependencies` and marked `optional` in
  `package.json#peerDependenciesMeta`. Also in `package.json#devDependencies` 
  for unit tests.

## scripts
Scripts are run with `npm`.
- The `build` script cleans, compiles, tests and generates documentation.
- The `dev` script compiles and unit tests, and watches for changes (use
  `dev+log` to see console output).
- The `compliance` script runs spec compliance tests (long-running).
- The `doc-dev` script can be used after `build` to create a local web server
  watching for documentation changes.

## publishing (team only)
This project uses semantic versioning. There are two main branches.
- The `edge` branch is for pre-releases. Docs are delivered to edge.js.m-ld.org.
  A merge into `edge` should be immediately followed by a pre-release if it
  affects versioned components.
- The `master` branch is for releases. Docs are delivered to js.m-ld.org. A
  merge into `master` should be immediately followed by a release if it affects
  versioned components.

Only the unit tests are included in the build. Prior to publish remember to run
the `compliance` tests script ([ticket](https://github.com/m-ld/m-ld-js/issues/19)).

`VERSION=? npm publish` builds the project, increments the version as specified
with the VERSION variable (e.g. `patch`), pushes the code and publishes the
package. *Ensure the repo is up-to-date and on* master *(release) or* edge
*(pre-release)*

