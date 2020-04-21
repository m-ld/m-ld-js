const { spawn } = require('child_process');
const { join } = require('path');
const restify = require('restify');
const orchestrator = require('./orchestrator');
const httpUrl = new URL('http://localhost:3000');
const inspector = require('inspector');

// Debug level - 0=info, 1=debug, 2=trace
global.debug = 0;
global.nextDebugPort = 40895;

const http = restify.createServer();
http.use(restify.plugins.queryParser());
http.use(restify.plugins.bodyParser());
Object.entries(orchestrator.routes)
  .forEach(([path, route]) => http.post('/' + path, route));
http.on('after', orchestrator.afterRequest);
http.listen(httpUrl.port, () => {
  console.log(`Orchestrator listening on ${httpUrl.port}`);
  const test = spawn('npm', ['run', 'test'], {
    cwd: join(__dirname, '..', 'node_modules', '@gsvarovsky', 'm-ld-spec'),
    env: { ...process.env, MELD_ORCHESTRATOR_URL: httpUrl.toString() },
    stdio: global.debug ? 'pipe' : 'inherit',
    execArgv: inspector.url() ? [`--inspect-brk=${global.nextDebugPort++}`] : []
  });

  if (global.debug) {
    test.stdout.pipe(process.stdout);
    test.stderr.pipe(process.stderr);
  }

  test.on('exit', (code) => {
    console.log(`Test process exited with code ${code}`);
    orchestrator.onExit();
    http.close();
    process.exit(code);
  });
});