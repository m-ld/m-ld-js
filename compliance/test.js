const { fork } = require('child_process');
const { createWriteStream } = require('fs');
const { join } = require('path');
const inspector = require('inspector');
const LOG = require('loglevel');

const COMPLIANCE_DIR = '../node_modules/@m-ld/m-ld-spec/compliance'.split('/');
const COMPLIANCE_PATH = join(__dirname, ...COMPLIANCE_DIR);
const Jasmine = require(require.resolve('jasmine', { paths: [COMPLIANCE_PATH] }));
const jasmine = new Jasmine();

let [, , ...jasmineArgs] = process.argv;
const jasmineConfig = {};
let specs = [], filter = undefined;
function specFile(spec) {
  return join(COMPLIANCE_PATH, `${spec}.spec.js`);
}
for (let arg of jasmineArgs) {
  const optionMatch = arg.match(/--([\w-]+)(?:="?([^"]+)"?)?/);
  if (optionMatch != null) {
    switch (optionMatch[1]) {
      case 'reporter':
        const Reporter = require(optionMatch[2]);
        jasmine.addReporter(new Reporter());
        break;
      case 'filter':
        filter = optionMatch[2];
        console.log('Filter', filter);
        break;
      case 'stop-on-failure':
        jasmineConfig.stopOnSpecFailure = (optionMatch[2] === 'true')
        break;
      case 'random':
        jasmineConfig.random = (optionMatch[2] === 'true')
        break;
    }
  } else {
    const spec = arg.replace(/(\d)(?=\/|$)/g, n => `${n}-*`);
    specs.push(specFile(spec));
  }
}
if (!specs.length)
  specs = [specFile('*/*')];
console.log('Running specs', specs, 'with config', jasmineConfig);

LOG.setLevel(process.env.LOG_LEVEL = process.env.LOG_LEVEL || LOG.levels.WARN);
let orchestratorDebugPort, firstCloneDebugPort;
if (inspector.url() != null) {
  let debugPort = Number(new URL(inspector.url()).port);
  orchestratorDebugPort = ++debugPort;
  firstCloneDebugPort = ++debugPort;
}

// Fork the orchestrator
const orchestrator = fork(join(__dirname, 'orchestrator.js'),
  [firstCloneDebugPort, LOG.getLevel()],
  { execArgv: inspector.url() ? [`--inspect-brk=${orchestratorDebugPort}`] : [], silent: true });
// Direct orchestrator output to file
const logFile = createWriteStream(join(__dirname, '.log'));
orchestrator.stdout.pipe(logFile);
orchestrator.stderr.pipe(logFile);

orchestrator.on('message', message => {
  switch (message['@type']) {
    case 'listening':
      try {
        process.env.MELD_ORCHESTRATOR_URL = message.url;
        jasmine.loadConfig(jasmineConfig);
        // Try to shut down normally when done
        jasmine.onComplete(() => orchestrator.kill());
        jasmine.execute(specs, filter);
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