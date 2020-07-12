[![npm](https://img.shields.io/badge/npm-private-red)](https://www.npmjs.com/package/@m-ld/m-ld)
[![build](https://img.shields.io/badge/build-vercel-green)](https://vercel.com/m-ld/m-ld-js)

# m-ld-js
m-ld Javascript native

## test
Only the unit tests are included in the build. Prior to publish remember to run
the `compliance` tests script.

## publish
`VERSION=? npm publish` builds the project, increments the version as specified with the VERSION variable (e.g. `patch`), pushes the code and publishes the package.
*Ensure the repo is up-to-date and on master*