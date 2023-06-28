// noinspection JSUnusedGlobalSymbols

import { defineConfig } from 'tsup';
import resolve from 'resolve';

const resolveNodeJsModules = /**@type {import('esbuild').Plugin}*/{
  name: 'resolve-nodejs',
  setup({ onResolve, initialOptions }) {
    const shims = [
      'events',
      'buffer', // See also inject of global
      'url' // Used by mqtt module
    ];
    for (let shim of shims) {
      const shimPath = resolve.sync(shim, { includeCoreModules: false });
      const filter = new RegExp(`^${shim}$`);
      onResolve({ filter }, () => ({ path: shimPath }));
    }
    (initialOptions.inject ??= []).push(resolve.sync('./tsup/Buffer'));
  }
};

const { exports } = require('./package.json');

export default defineConfig({
  entry: Object.values(exports)
    .filter(path => !path.includes('server'))
    .map(path => path
      .replace(/^\.\/ext/, 'src')
      .replace(/\.js$/, '.ts')),
  format: 'esm',
  platform: 'browser',
  outDir: '_site/media',
  esbuildPlugins: [resolveNodeJsModules],
  sourcemap: true,
  noExternal: [
    /(.*)/
  ],
  clean: true
});