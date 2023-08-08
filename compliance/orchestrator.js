const { join } = require('path');
const LOG = require('loglevel');
const { Forkestrator, MqttRemoting } = require('@m-ld/m-ld-test');

// Global arguments handling
const [, , logLevel] = process.argv;
LOG.setLevel(Number(logLevel));
LOG.getLogger('aedes').setLevel(LOG.levels.WARN);

const orchestrator = new Forkestrator(
  new MqttRemoting,
  join(__dirname, 'clone.js')
).on('listening', url => {
  process.send(
    { '@type': 'listening', url },
    err => err && LOG.warn('Orchestrator orphaned', err)
  );
  process.on('exit', () => orchestrator.close());
});