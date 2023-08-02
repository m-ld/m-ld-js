const { fork } = require('child_process');
const { createWriteStream } = require('fs');
const { join } = require('path');
const LOG = require('loglevel');
const { runner } = require('@m-ld/m-ld-test/compliance');

let [, , ...args] = process.argv;
const runTests = runner(args, { localTestPath: 'compliance/test' });

LOG.setLevel(process.env.LOG_LEVEL = process.env.LOG_LEVEL || LOG.levels.WARN);

// Fork the orchestrator
const orchestrator = fork(join(__dirname, 'orchestrator.js'),
  [LOG.getLevel().toString()],
  { silent: true });
// Direct orchestrator output to file
const logFile = createWriteStream(join(__dirname, '.log'));
orchestrator.stdout.pipe(logFile);
orchestrator.stderr.pipe(logFile);

orchestrator.on('message', async message => {
  switch (message['@type']) {
    case 'listening':
      runTests(message.url).then(() => {
        process.exit(0);
      }).catch(err => {
        LOG.error(err);
        process.exit(1);
      });
  }
});

orchestrator.on('exit', code => {
  if (code > 0) {
    LOG.warn(`Orchestrator process died with code ${code}`);
    process.exit(code);
  }
});

process.on('exit', () => orchestrator.kill());