const leveldown = require('leveldown');
const { clone, isRead } = require('@m-ld/m-ld');
const { MqttRemotes } = require('@m-ld/m-ld/dist/mqtt');
const { createSign } = require('crypto');
const LOG = require('loglevel');
const { EventEmitter } = require('events');
const { createWriteStream, mkdirSync, existsSync } = require('fs');
const { join } = require('path');

Error.stackTraceLimit = Infinity;

process.send({ '@type': 'active' });

process.on('message', startMsg => {
  if (startMsg['@type'] !== 'start')
    return;

  const { config, tmpDirName, requestId } = startMsg;
  LOG.setLevel(config.logLevel);
  LOG.debug(logTs(), config['@id'], 'config is', JSON.stringify(config));
  clone(leveldown(tmpDirName), MqttRemotes, config, createApp(config)).then(meld => {
    send(requestId, 'started', { cloneId: config['@id'] });

    const handler = message => {
      function settle(work, resMsgType, terminal) {
        work.then(() => send(message.id, resMsgType))
          .catch(errorHandler(message))
          .then(() => !terminal || process.off('message', handler));
      }
      switch (message['@type']) {
        case 'transact':
          if (isRead(message.request))
            meld.read(message.request).subscribe({
              next: subject => send(message.id, 'next', { body: subject }),
              complete: () => send(message.id, 'complete'),
              error: errorHandler(message)
            });
          else
            settle(meld.write(message.request), 'complete');
          break;
        case 'stop':
          settle(meld.close(), 'stopped', true);
          break;
        case 'destroy':
          settle(meld.close(), 'destroyed', true);
          break;
        default:
          sendError(message.id, `No handler for ${message['@type']}`);
      }
    };
    process.on('message', handler);

    meld.follow(update => send(requestId, 'updated', { body: update }));

    meld.status.subscribe({
      next: status => send(requestId, 'status', { body: status }),
      complete: () => send(requestId, 'closed'),
      error: err => sendError(requestId, err)
    });
  }).catch(err => {
    LOG.error(logTs(), config['@id'], err);
    send(requestId, 'unstarted', { err: `${err}` });
  });
});

function createApp(config) {
  const app = {};
  const { principal, transportSecurity } = config;
  // 1. Create an app principal
  if (principal) {
    delete config.principal;
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
    app.transportSecurity =
      new (require(transportSecurity.require)[transportSecurity.export])(config);
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

function send(requestId, type, params) {
  process.send({ requestId, '@type': type, ...params },
    err => err && LOG.warn(logTs(), 'Clone orphaned from orchestrator', err));
}

function errorHandler(message) {
  return err => sendError(message.id, err);
}

function sendError(requestId, err) {
  LOG.error(logTs(), err);
  return send(requestId, 'error', { err: `${err}` });
}

function logTs() {
  return new Date().toISOString();
}