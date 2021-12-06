const { fork } = require('child_process');
const { join } = require('path');
const { dirSync: newTmpDir } = require('tmp');
const restify = require('restify');
const { BadRequestError, InternalServerError, NotFoundError } = require('restify-errors');
const { createServer } = require('net');
const LOG = require('loglevel');
const inspector = require('inspector');
const aedes = require('aedes')();

const domains = new Set;
const clones = {/* cloneId: { process, tmpDir, mqtt: { client, server, port } } */ };
const requests = {/* requestId: [res, next] */ };

// Arguments expected
const [, , nextDebugPort, logLevel] = process.argv;

LOG.setLevel(Number(logLevel));
LOG.getLogger('aedes').setLevel(LOG.levels.WARN);
global.nextDebugPort = Number(nextDebugPort);

const http = restify.createServer();
http.use(restify.plugins.queryParser());
http.use(restify.plugins.bodyParser());
Object.entries({ start, transact, stop, kill, destroy, partition })
  .forEach(([path, route]) => http.post('/' + path, route));
http.on('after', req => delete requests[req.id()]);
http.listen(0, () => {
  const url = `http://localhost:${http.address().port}`;
  LOG.info(`Orchestrator listening on ${url}`);
  process.send({ '@type': 'listening', url }, err => err && LOG.warn('Orchestrator orphaned', err));
  process.on('exit', () => {
    LOG.info(`Orchestrator shutting down`);
    Object.values(clones).forEach(
      ({ process }) => process && process.kill());
    http.close();
  });
});

aedes.on('publish', function (packet, client) {
  const log = LOG.getLogger('aedes');
  if (client) {
    const { topic, qos, retain } = packet;
    log.debug(
      client.id, { topic, qos, retain },
      log.getLevel() <= LOG.levels.TRACE ? packet.payload.toString() : '');
  }
});

aedes.on('client', client => {
  const log = LOG.getLogger('aedes');
  if (client.id in clones) {
    log.debug(client.id, 'MQTT client connecting');
    clones[client.id].mqtt.client = client;
  } else {
    log.warn(client.id, 'Unexpected MQTT client');
  }
});

function reportError(client, err) {
  // Don't report if the clone is dead or dying
  const cloneProcess = clones[client.id]?.process;
  if (cloneProcess != null && !cloneProcess.killed)
    LOG.getLogger('aedes').warn(client.id, err);
}
aedes.on('clientError', reportError);
aedes.on('connectionError', reportError);

function start(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId, domain } = req.query;
  let tmpDir;
  if (cloneId in clones) {
    tmpDir = clones[cloneId].tmpDir;
    if (clones[cloneId].process)
      return next(new BadRequestError(`Clone ${cloneId} is already started`));
  } else {
    tmpDir = newTmpDir({ unsafeCleanup: true });
  }
  const genesis = !domains.has(domain);
  LOG.info(`${cloneId}: Starting ${genesis ? 'genesis ' : ''}clone on domain ${domain}`);
  domains.add(domain);

  const mqttServer = createServer(aedes.handle);
  mqttServer.listen(err => {
    if (err)
      return next(new InternalServerError(err));

    const mqttPort = mqttServer.address().port;
    LOG.debug(`${cloneId}: Clone MQTT port is ${mqttPort}`);

    const config = Object.assign({
      '@id': cloneId,
      '@domain': domain,
      genesis,
      mqtt: {
        host: 'localhost',
        port: mqttPort,
        // Short timeouts as everything is local
        connectTimeout: 100,
        keepalive: 1
      },
      networkTimeout: 1000,
      logLevel: LOG.getLevel()
    }, req.body);

    clones[cloneId] = {
      process: fork(join(__dirname, 'clone.js'),
        [JSON.stringify(config), tmpDir.name, req.id()],
        {
          // Strictly disallow unhandled promise rejections for compliance testing
          execArgv: ['--unhandled-rejections=strict']
            .concat(inspector.url() ? [`--inspect-brk=${global.nextDebugPort++}`] : [])
        }),
      tmpDir,
      mqtt: { server: mqttServer, port: mqttPort }
    };

    const handlers = {
      started: message => {
        res.header('transfer-encoding', 'chunked');
        return res.write(JSON.stringify(message));
      },
      updated: message => {
        return res.write(JSON.stringify(message));
      },
      status: message => {
        return res.write(JSON.stringify(message));
      },
      closed: () => {
        res.end();
        next();
      },
      unstarted: message => {
        killCloneProcess(cloneId, new InternalServerError(message.err), res, next);
      },
      next: message => {
        const { requestId, body } = message;
        const [res,] = requests[requestId];
        res.write(JSON.stringify(body));
      },
      complete: message => {
        const { requestId } = message;
        const [res, next] = requests[requestId];
        res.end();
        next();
      },
      error: message => {
        const { requestId, err } = message;
        const [res, next] = requests[requestId];
        if (res.header('transfer-encoding') === 'chunked') {
          // Restify tries to set content-length, which is bad for chunking
          res.status(500);
          res.write(err);
          res.end();
        } else {
          next(new InternalServerError(err));
        }
      },
      destroyed: message => {
        const { requestId } = message;
        const [res, next] = requests[requestId];
        killCloneProcess(cloneId, 'destroyed', res, next);
        destroyAndForget(cloneId, tmpDir);
      },
      stopped: message => {
        const { requestId } = message;
        const [res, next] = requests[requestId];
        killCloneProcess(cloneId, 'stopped', res, next);
      }
    };
    clones[cloneId].process.on('message', message => {
      if (message['@type'] in handlers)
        handlers[message['@type']](message);
    });
  });
}

function transact(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId } = req.query;
  withClone(cloneId, ({ process }) => {
    process.send({
      id: req.id(),
      '@type': 'transact',
      request: req.body
    }, err => err && next(err));
    res.header('transfer-encoding', 'chunked');
  }, next);
}

function stop(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId } = req.query;
  withClone(cloneId, ({ process }) => {
    LOG.debug(`${cloneId}: Stopping clone`);
    process.send({
      id: req.id(), '@type': 'stop'
    }, err => {
      if (err) {
        // This clone process is already killed
        LOG.debug(`${cloneId}: Already killed`, err);
        res.send({ '@type': 'stopped', cloneId });
        next();
      }
    });
  }, next);
}

function kill(req, res, next) {
  const { cloneId } = req.query;
  killCloneProcess(cloneId, 'killed', res, next);
}

function destroy(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId } = req.query;
  withClone(cloneId, ({ process, tmpDir }) => {
    LOG.debug(`${cloneId}: Destroying clone`);
    function done() {
      destroyAndForget(cloneId, tmpDir);
      res.send({ '@type': 'destroyed', cloneId });
      next();
    }
    if (process) {
      process.send({
        id: req.id(), '@type': 'destroy'
      }, err => {
        if (err) {
          // This clone process is already killed
          LOG.debug(`${cloneId}: Already killed`, err);
          done();
        }
      });
    } else {
      done();
    }
  }, next);
}

function partition(req, res, next) {
  const { cloneId, state: stateString } = req.query;
  withClone(cloneId, ({ mqtt }) => {
    LOG.debug(`${cloneId}: Partitioning clone (${stateString})`);
    const state = stateString !== 'false';
    if (state && mqtt.server.listening) {
      if (mqtt.client)
        mqtt.client.conn.destroy();
      mqtt.server.close(err => {
        if (err) {
          next(new InternalServerError(err));
        } else {
          LOG.debug(`${cloneId}: MQTT stopped`);
          res.send({ '@type': 'partitioned', state });
          next();
        }
      });
    } else if (!state && !mqtt.server.listening) {
      mqtt.server.listen(mqtt.port, err => {
        if (err) {
          return next(new InternalServerError(err));
        } else {
          LOG.debug(`${cloneId}: MQTT re-started`);
          res.send({ '@type': 'partitioned', state });
          next();
        }
      });
    } else {
      next(new BadRequestError('Partition request does not match MQTT state'));
    }
  });
}

function destroyAndForget(cloneId, tmpDir) {
  LOG.info(`${cloneId}: Destroying clone data`);
  tmpDir.removeCallback();
  delete clones[cloneId];
}

function registerRequest(req, res, next) {
  requests[req.id()] = [res, next];
}

function withClone(cloneId, op/*(subprocess, tmpDir)*/, next) {
  if (cloneId in clones) {
    op(clones[cloneId]);
  } else {
    next(new NotFoundError(`Clone ${cloneId} not available`));
  }
}

function killCloneProcess(cloneId, reason, res, next) {
  withClone(cloneId, clone => {
    LOG.debug(`${cloneId}: Killing clone process`);
    clone.process.kill();
    clone.process.on('exit', () => {
      clone.process = null;
      function done(err) {
        if (err) {
          next(new InternalServerError(err));
        } else if (reason instanceof Error) {
          next(reason);
        } else {
          res.send({ '@type': reason, cloneId });
          next();
        }
      }
      return clone.mqtt.server.listening ? clone.mqtt.server.close(done) : done();
    });
  }, next);
}