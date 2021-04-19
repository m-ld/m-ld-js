const leveldown = require('leveldown');
const { clone, isRead } = require('../dist');
const { MqttRemotes } = require('../dist/mqtt');
const LOG = require('loglevel');

Error.stackTraceLimit = Infinity;

const [, , configJson, tmpDirName, requestId] = process.argv;
const config = JSON.parse(configJson);
LOG.setLevel(config.logLevel);
LOG.debug(config['@id'], 'config is', JSON.stringify(config));
clone(leveldown(tmpDirName), new MqttRemotes(config), config).then(meld => {
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
          .catch(errorHandler(message))
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
      send(message.id, 'error', { err: `No handler for ${message['@type']}` });
  });

  meld.follow(update => send(requestId, 'updated', { body: update }));

  meld.status.subscribe({
    next: status => send(requestId, 'status', { body: status }),
    complete: () => send(requestId, 'closed'),
    error: err => sendError(requestId, err)
  })
}).catch(err => {
  LOG.error(config['@id'], err);
  send(requestId, 'unstarted', { err: `${err}` });
});

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
