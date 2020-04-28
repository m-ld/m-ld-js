const { spawn } = require('child_process');
const { join } = require('path');
const restify = require('restify');
const orchestrator = require('./orchestrator');
const httpUrl = new URL('http://localhost:3000');
const inspector = require('inspector');
const LOG = require('loglevel');

LOG.setLevel(LOG.levels.WARN);
LOG.getLogger('aedes').setLevel(LOG.levels.WARN);
global.nextDebugPort = 40895;

const http = restify.createServer();
http.use(restify.plugins.queryParser());
http.use(restify.plugins.bodyParser());
Object.entries(orchestrator.routes)
  .forEach(([path, route]) => http.post('/' + path, route));
http.on('after', orchestrator.afterRequest);
http.listen(httpUrl.port, () => {
  LOG.info(`Orchestrator listening on ${httpUrl.port}`);
  const test = spawn('npm', ['run', 'test'], {
    cwd: join(__dirname, '..', 'node_modules', '@gsvarovsky', 'm-ld-spec'),
    env: { ...process.env, MELD_ORCHESTRATOR_URL: httpUrl.toString() },
    stdio: LOG.getLevel() <= LOG.levels.DEBUG ? 'pipe' : 'inherit',
    execArgv: inspector.url() ? [`--inspect-brk=${global.nextDebugPort++}`] : []
  });

  if (LOG.getLevel() <= LOG.levels.DEBUG) {
    test.stdout.pipe(process.stdout);
    test.stderr.pipe(process.stderr);
  }

  test.on('exit', (code) => {
    LOG.info(`Test process exited with code ${code}`);
    orchestrator.onExit();
    http.close();
    process.exit(code);
  });
});