
module.exports = {
  mode: 'file',
  readme: 'doc/index.md',
  out: '_site',
  theme: 'node_modules/@m-ld/typedoc-theme/bin/minimal',
  includes: 'doc/includes',
  media: 'doc/media',
  ignoreCompilerErrors: true,
  includeDeclarations: true,
  excludeExternals: true,
  disableSources: true,
  includeVersion: true
}