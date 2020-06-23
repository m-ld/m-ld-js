const leveldown = require('leveldown');
const { clone } = require('../dist');
const { MqttRemotes } = require('../dist/mqtt');
const LOG = require('loglevel');

Error.stackTraceLimit = Infinity;

const [, , cloneId, domain, genesis, tmpDirName, requestId, mqttPort, logLevel] = process.argv;
LOG.setLevel(Number(logLevel));
console.log(genesis);
const config = {
  '@id': cloneId,
  '@domain': domain,
  genesis: genesis == 'true',
  mqttOpts: {
    host: 'localhost',
    port: Number(mqttPort),
    // Short timeouts as everything is local
    connectTimeout: 100,
    keepalive: 1
  },
  networkTimeout: 1000,
  logLevel: Number(logLevel)
};
LOG.debug(cloneId, 'config is', JSON.stringify(config));
clone(leveldown(tmpDirName), new MqttRemotes(config), config).then(meld => {
  send(requestId, 'started', { cloneId });

  const handlers = {
    transact: message => meld.transact(message.request).subscribe({
      next: subject => send(message.id, 'next', { body: subject }),
      complete: () => send(message.id, 'complete'),
      error: errorHandler(message)
    }),
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

  meld.follow().subscribe({
    next: update => send(requestId, 'updated', { body: update }),
    complete: () => send(requestId, 'closed'),
    error: err => sendError(requestId, err)
  });
}).catch(err => {
  LOG.error(cloneId, err);
  send(requestId, 'unstarted', { err: `${err}` });
});

function send(requestId, type, params) {
  process.send({ requestId, '@type': type, ...params });
}

function errorHandler(message) {
  return err => sendError(message.id, err);
}

function sendError(requestId, err) {
  return send(requestId, 'error', { err: `${err}` });
}
