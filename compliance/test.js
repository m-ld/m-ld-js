const { fork, spawn } = require('child_process');
const { createWriteStream } = require('fs');
const { join } = require('path');
const inspector = require('inspector');
const LOG = require('loglevel');
const httpUrl = new URL('http://localhost:3000');

LOG.setLevel(LOG.levels.WARN);
let testDebugPort, orchestratorDebugPort, firstCloneDebugPort;
if (inspector.url() != null) {
  let debugPort = Number(new URL(inspector.url()).port);
  testDebugPort = ++debugPort;
  orchestratorDebugPort = ++debugPort;
  firstCloneDebugPort = ++debugPort;
}

// Fork the orchestrator
const orchestrator = fork(join(__dirname, 'orchestrator.js'),
  [httpUrl, firstCloneDebugPort, LOG.getLevel()],
  { execArgv: inspector.url() ? [`--inspect-brk=${orchestratorDebugPort}`] : [], silent: true });
// Direct orchestrator output to file
const logFile = createWriteStream(join(__dirname, '.log'));
orchestrator.stdout.pipe(logFile);
orchestrator.stderr.pipe(logFile);

orchestrator.on('message', message => {
  switch (message) {
    case 'listening':
      spawn('npm', ['run', 'test'], {
        cwd: join(__dirname, '..', 'node_modules', '@m-ld', 'm-ld-spec'),
        env: { ...process.env, MELD_ORCHESTRATOR_URL: httpUrl.toString(), LOG_LEVEL: `${LOG.getLevel()}` },
        stdio: 'inherit',
        execArgv: inspector.url() ? [`--inspect-brk=${testDebugPort}`] : []
      }).on('exit', code => {
        LOG.info(`Test process exited with code ${code}`);
        orchestrator.kill(); // Try to shut down normally
        process.exit(code);
      });
  }
});
orchestrator.on('exit', code => {
  if (code > 0) {
    LOG.warn(`Orchestrator process died with code ${code}`);
    process.exit(code);
  }
});