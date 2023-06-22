module.exports = {
  mode: 'file',
  readme: './doc/index.md',
  readmeToc: require('./doc/toc.json'),
  out: '_site',
  theme: 'node_modules/@m-ld/typedoc-theme/bin/minimal',

  includes: './doc/includes',
  exclude: [
    './src/engine/**',
    './src/types/**',
    './src/ns/**',
    './src/lseq/**',
    './src/tseq/**',
    './src/ably/**',
    './src/mqtt/**',
    './src/wrtc/**',
    './src/socket.io/**'
  ],
  excludePrivate: true,
  disableSources: true,
  includeVersion: true,
  stripInternal: true,

  categorizeByGroup: true,
  categoryOrder: [
    'Configuration',
    'API',
    'json-rql',
    'RDFJS',
    'Utility',
    '*',
    'Experimental'
  ]
};