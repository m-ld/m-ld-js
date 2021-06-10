const { fork } = require('child_process');
const { createWriteStream } = require('fs');
const { join } = require('path');
const inspector = require('inspector');
const LOG = require('loglevel');
const httpUrl = new URL('http://localhost:3000');

const COMPLIANCE_DIR = '../node_modules/@m-ld/m-ld-spec/compliance'.split('/');
const COMPLIANCE_PATH = join(__dirname, ...COMPLIANCE_DIR);
const Jasmine = require(require.resolve('jasmine', { paths: [COMPLIANCE_PATH] }));
const jasmine = new Jasmine();

// Expected spec glob, default "*/*" (everything)
let [, , specs] = process.argv;
specs = (specs || '*/*').replace(/(\d)(?=\/|$)/g, n => `${n}-*`);
console.log('Running specs', specs);

LOG.setLevel(process.env.LOG_LEVEL = process.env.LOG_LEVEL || LOG.levels.WARN);
let orchestratorDebugPort, firstCloneDebugPort;
if (inspector.url() != null) {
  let debugPort = Number(new URL(inspector.url()).port);
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
      try {
        process.env.MELD_ORCHESTRATOR_URL = httpUrl.toString();
        jasmine.loadConfig({
          failFast: true,
          spec_files: [join(COMPLIANCE_PATH, `${specs || '*/*'}.spec.js`)]
        });
        // Try to shut down normally when done
        jasmine.onComplete(() => orchestrator.kill());
        jasmine.execute();
      } catch (err) {
        LOG.error(err);
        orchestrator.kill();
      }
  }
});
orchestrator.on('exit', code => {
  if (code > 0) {
    LOG.warn(`Orchestrator process died with code ${code}`);
    process.exit(code);
  }
});