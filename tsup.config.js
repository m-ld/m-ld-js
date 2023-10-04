// noinspection JSUnusedGlobalSymbols

import { defineConfig } from 'tsup';
import { resolveNode } from '@m-ld/io-js-build/esbuild';

const { exports } = require('./package.json');

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    ...Object.fromEntries(Object.keys(exports)
      .filter(ext => ext.startsWith('./ext/'))
      .filter(ext => !ext.includes('server'))
      .map(extPath => {
        const module = extPath.slice('./ext/'.length);
        return [module, `src/${module}/index.ts`];
      })),
    'memory-level': 'tsup/memory-level.js'
  },
  format: 'esm',
  platform: 'browser',
  outDir: '_site/ext',
  esbuildPlugins: [resolveNode([
    'events',
    'buffer', // Also injects global
    'url' // Used by mqtt module
  ])],
  sourcemap: true,
  noExternal: [/(.*)/], // Bundle all dependencies
  clean: true
});