const leveldown = require('leveldown');
const { clone, isRead } = require('@m-ld/m-ld');
const { MqttRemotes } = require('@m-ld/m-ld/dist/mqtt');
const { createSign } = require('crypto');
const LOG = require('loglevel');

Error.stackTraceLimit = Infinity;

const [, , configJson, tmpDirName, requestId] = process.argv;
const config = JSON.parse(configJson);
LOG.setLevel(config.logLevel);
LOG.debug(config['@id'], 'config is', JSON.stringify(config));
clone(leveldown(tmpDirName), MqttRemotes, config, createApp()).then(meld => {
  send(requestId, 'started', { cloneId: config['@id'] });

  const handlers = {
    transact: message => {
      if (isRead(message.request))
        meld.read(message.request).subscribe({
          next: subject => send(message.id, 'next', { body: subject }),
          complete: () => send(message.id, 'complete'),
          error: errorHandler(message)
        });
      else
        meld.write(message.request)
          .then(() => send(message.id, 'complete'))
          .catch(errorHandler(message));
    },
    stop: message => meld.close()
      .then(() => send(message.id, 'stopped'))
      .catch(errorHandler(message)),
    destroy: message => meld.close()
      .then(() => send(message.id, 'destroyed'))
      .catch(errorHandler(message))
  };

  process.on('message', message => {
    if (message['@type'] in handlers)
      handlers[message['@type']](message);
    else
      sendError(message.id, `No handler for ${message['@type']}`);
  });

  meld.follow(update => send(requestId, 'updated', { body: update }));

  meld.status.subscribe({
    next: status => send(requestId, 'status', { body: status }),
    complete: () => send(requestId, 'closed'),
    error: err => sendError(requestId, err)
  });
}).catch(err => {
  LOG.error(config['@id'], err);
  send(requestId, 'unstarted', { err: `${err}` });
});

function createApp() {
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
  return app;
}

function send(requestId, type, params) {
  process.send({ requestId, '@type': type, ...params },
    err => err && LOG.warn('Clone orphaned from orchestrator', err));
}

function errorHandler(message) {
  return err => sendError(message.id, err);
}

function sendError(requestId, err) {
  LOG.error(err);
  return send(requestId, 'error', { err: `${err}` });
}
