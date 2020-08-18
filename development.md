# m-ld-js
m-ld Javascript native

## test
Only the unit tests are included in the build. Prior to publish remember to run
the `compliance` tests script.

## ci
https://vercel.com/m-ld/m-ld-js

## publish
`VERSION=? npm publish` builds the project, increments the version as specified with the VERSION variable (e.g. `patch`), pushes the code and publishes the package.
*Ensure the repo is up-to-date and on master*