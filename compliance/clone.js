const leveldown = require('leveldown');
const { clone } = require('../dist');
const { dirSync } = require('tmp');

const [, , cloneId, domain, requestId] = process.argv;
const tmpDir = dirSync({ unsafeCleanup: true })

clone(leveldown(tmpDir.name), {
  '@id': cloneId, '@domain': domain,
  mqttOpts: { host: 'localhost', port: 1883 }
}).then(meld => {
  process.send({ '@type': 'started', cloneId });

  const handlers = {
    transact: message => meld.transact(message.request).subscribe({
      next: subject => process.send({
        requestId: message.id, '@type': 'next', body: subject
      }),
      complete: () => process.send({
        requestId: message.id, '@type': 'complete'
      }),
      error: err => process.send({
        requestId: message.id, '@type': 'error', err: `${err}`
      })
    }),
    destroy: message => meld.close().then(() => {
      tmpDir.removeCallback();
      return process.send({
        requestId: message.id, '@type': 'destroyed'
      });
    }).catch(err => process.send({
      requestId: message.id, '@type': 'error', err: `${err}`
    }))
  };

  process.on('message', message => {
    if (message['@type'] in handlers)
      handlers[message['@type']](message);
    else
      process.send({
        requestId: message.id, '@type': 'error', err: `No handler for ${message['@type']}`
      });
  });
}).catch(err => {
  console.error(err);
  process.send({ requestId, '@type': 'error', err: `${err}` });
});