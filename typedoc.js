
const path = require('path');
module.exports = {
  mode: 'file',
  readme: 'doc/index.md',
  out: path.join('_site', process.env.npm_package_version),
  theme: 'minimal',
  includes: 'doc',
  ignoreCompilerErrors: true,
  includeDeclarations: true,
  excludeExternals: true,
  disableSources: true,
  includeVersion: true
}