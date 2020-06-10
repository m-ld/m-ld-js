const { fork } = require('child_process');
const { join } = require('path');
const { dirSync } = require('tmp');
const restify = require('restify');
const { BadRequestError, InternalServerError, NotFoundError } = require('restify-errors');
const { createServer } = require('net');
const LOG = require('loglevel');
const inspector = require('inspector');
const aedes = require('aedes')();

const clones = {/* cloneId: { process, tmpDir, mqtt: { client, server, port } } */ };
const requests = {/* requestId: [res, next] */ };

// Arguments expected
const [, , httpUrl, nextDebugPort, logLevel] = process.argv;
const port = new URL(httpUrl).port;

LOG.setLevel(Number(logLevel));
LOG.getLogger('aedes').setLevel(LOG.levels.WARN);
global.nextDebugPort = Number(nextDebugPort);

const http = restify.createServer();
http.use(restify.plugins.queryParser());
http.use(restify.plugins.bodyParser());
Object.entries({ start, transact, stop, kill, destroy, partition })
  .forEach(([path, route]) => http.post('/' + path, route));
http.on('after', req => delete requests[req.id()]);
http.listen(port, () => {
  LOG.info(`Orchestrator listening on ${httpUrl}`);
  process.send('listening');
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
    log.debug(`${client.id}: MQTT client connecting`);
    clones[client.id].mqtt.client = client;
  } else {
    log.warn(`${client.id}: Unexpected MQTT client`);
  }
});

function start(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId, domain } = req.query;
  let tmpDir;
  if (cloneId in clones) {
    tmpDir = clones[cloneId].tmpDir;
    if (clones[cloneId].process)
      return next(new BadRequestError(`Clone ${cloneId} is already started`));
  } else {
    tmpDir = dirSync({ unsafeCleanup: true });
  }
  LOG.info(`${cloneId}: Starting clone on domain ${domain}`);

  const mqttServer = createServer(aedes.handle);
  mqttServer.listen(err => {
    if (err)
      return next(new InternalServerError(err));

    const mqttPort = mqttServer.address().port;
    LOG.debug(`${cloneId}: Clone MQTT port is ${mqttPort}`);
    clones[cloneId] = {
      process: fork(join(__dirname, 'clone.js'),
        [cloneId, domain, tmpDir.name, req.id(), mqttPort, LOG.getLevel()],
        { execArgv: inspector.url() ? [`--inspect-brk=${global.nextDebugPort++}`] : [] }),
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
        if (res.header('transfer-encoding') == 'chunked') {
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
        destroyData(cloneId, tmpDir);
        killCloneProcess(cloneId, 'destroyed', res, next);
        delete clones[cloneId];
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
    });
    res.header('transfer-encoding', 'chunked');
  }, next);
}

function stop(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId } = req.query;
  withClone(cloneId, ({ process }) => {
    LOG.debug(`${cloneId}: Stopping clone`);
    process.send({ id: req.id(), '@type': 'stop' });
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
    if (process) {
      process.send({ id: req.id(), '@type': 'destroy' });
    } else {
      destroyData(cloneId, tmpDir);
      delete clones[cloneId];
      res.send({ '@type': 'destroyed', cloneId });
      next();
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

function destroyData(cloneId, tmpDir) {
  LOG.info(`${cloneId}: Destroying clone data`);
  tmpDir.removeCallback();
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