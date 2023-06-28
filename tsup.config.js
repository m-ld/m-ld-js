// noinspection JSUnusedGlobalSymbols

import { defineConfig } from 'tsup';
import resolve from 'resolve';

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
  esbuildPlugins: [{
    name: 'resolve-nodejs',
    setup({ onResolve, initialOptions }) {
      for (let shim of [
        'events',
        'buffer', // See also inject of global
        'url' // Used by mqtt module
      ]) {
        const shimPath = resolve.sync(shim, { includeCoreModules: false });
        const filter = new RegExp(`^${shim}$`);
        onResolve({ filter }, () => ({ path: shimPath }));
      }
      (initialOptions.inject ??= []).push(resolve.sync('./tsup/Buffer'));
    }
  }],
  sourcemap: true,
  noExternal: [/(.*)/], // Bundle all dependencies
  clean: true
});