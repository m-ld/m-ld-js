const { ClassicLevel } = require('classic-level');
const { clone } = require('@m-ld/m-ld');
const { MqttRemotes } = require('@m-ld/m-ld/ext/mqtt');
const { createSign } = require('crypto');
const LOG = require('loglevel');
const { EventEmitter } = require('events');
const { createWriteStream, mkdirSync, existsSync } = require('fs');
const { join } = require('path');
const { CloneProcess } = require('@m-ld/m-ld-test');
const { Observable } = require('rxjs');

Error.stackTraceLimit = Infinity;

new CloneProcess(process, async (config, tmpDirName) => {
  const meld = await clone(new ClassicLevel(tmpDirName), MqttRemotes, config, createApp(config));
  // A m-ld-js clone does not quite conform to a spec MeldClone
  return /**@type {import('@m-ld/m-ld-spec').MeldClone}*/{
    read: meld.read.bind(meld),
    write: meld.write.bind(meld),
    follow: () =>
      new Observable(subs =>
        meld.follow(update => subs.next(update))),
    status: meld.status,
    close: meld.close.bind(meld)
  }
});

/** @returns {InitialApp} */
function createApp(config) {
  const app = {};
  const { principal, transportSecurity } = config;
  // 1. Create an app principal
  if (principal) {
    delete config.principal;
    // noinspection JSUnusedGlobalSymbols
    app.principal = {
      '@id': principal['@id'],
      // Assuming privateKeyEncoding: { type: 'pkcs1', format: 'pem' }
      sign: data => createSign('RSA-SHA256')
        .update(data).sign(principal.privateKey)
    };
  }
  // 2. Create transport security
  if (transportSecurity) {
    delete config.transportSecurity;
    const TsClass = require(transportSecurity.require)[transportSecurity.class];
    app.transportSecurity = new TsClass(config, app.principal);
  }
  // 3. Performance timings
  if (LOG.getLevel() <= LOG.levels.TRACE) {
    app.backendEvents = new EventEmitter();
    const outDir = join(__dirname, 'logs');
    if (!existsSync(outDir))
      mkdirSync(outDir);
    const out = new console.Console(
      createWriteStream(join(outDir, `${config['@id']}.csv`)));
    out.log(['name', 'startTime', 'duration'].join(','));
    app.backendEvents.on('timing', event => out.log([
      event.names, Math.ceil(event.startTime), Math.ceil(event.duration)
    ].join(',')));
    app.backendEvents.on('error', err => LOG.warn('Backend error', err));
  }
  return app;
}