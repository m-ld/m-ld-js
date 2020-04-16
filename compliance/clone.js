const leveldown = require('leveldown');
const { clone } = require('../dist');

Error.stackTraceLimit = Infinity;

const [, , cloneId, domain, tmpDirName, requestId] = process.argv;

clone(leveldown(tmpDirName), {
  '@id': cloneId, '@domain': domain,
  mqttOpts: { host: 'localhost', port: 1883 }
}).then(meld => {
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
}).catch(err => {
  console.error(`${cloneId}: ${err}`);
  send(requestId, 'unstarted', { err: `${err}` });
});

function send(requestId, type, params) {
  process.send({ requestId, '@type': type, ...params });
}

function errorHandler(message) {
  return err => send(message.id, 'error', { err: `${err}` });
}
