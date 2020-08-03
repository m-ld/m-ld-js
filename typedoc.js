
module.exports = {
  mode: 'file',
  readme: './doc/index.md',
  readmeToc: require('./doc/toc.json'),
  out: '_site',
  theme: 'node_modules/@m-ld/typedoc-theme/bin/minimal',
  includes: './doc/includes',
  media: './doc/media',
  ignoreCompilerErrors: true,
  includeDeclarations: true,
  excludeExternals: true,
  excludePrivate: true,
  excludeProtected: true,
  disableSources: true,
  includeVersion: true
}