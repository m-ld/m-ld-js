const { spawn } = require('child_process');
const { join } = require('path');
const { promisify } = require('util');
const restify = require('restify');
const aedes = require('aedes')();
const mqtt = require('net').createServer(aedes.handle);
const mqttPort = 1883;
const httpUrl = new URL('http://localhost:3000');
const orchestrator = require('./orchestrator');

// Debug flag stops Jest from squashing stdout lines
global.debug = false;

const http = restify.createServer();
http.use(restify.plugins.queryParser());
http.use(restify.plugins.bodyParser());
Object.entries(orchestrator.routes)
  .forEach(([path, route]) => http.post('/' + path, route));
http.on('after', orchestrator.afterRequest);
const connectHttp = promisify(http.listen.bind(http));
const connectMqtt = promisify(mqtt.listen.bind(mqtt));
Promise.all([
  connectHttp(httpUrl.port).then(() => console.log(`Orchestrator listening on ${httpUrl.port}`)),
  connectMqtt(mqttPort).then(() => console.log(`MQTT broker listening on ${mqttPort}`))
]).then(() => {
  const test = spawn('npm', ['run', 'test'], {
    cwd: join(__dirname, '..', 'node_modules', '@gsvarovsky', 'm-ld-spec'),
    env: { ...process.env, MELD_ORCHESTRATOR_URL: httpUrl.toString() },
    stdio: global.debug ? 'pipe' : 'inherit'
  });

  if (global.debug) {
    test.stdout.pipe(process.stdout);
    test.stderr.pipe(process.stderr);
  }

  test.on('exit', (code) => {
    console.log(`Test process exited with code ${code}`);
    orchestrator.onExit();
    http.close();
    mqtt.close();
    process.exit(code);
  });
});