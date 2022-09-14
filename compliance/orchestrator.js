const { fork } = require('child_process');
const { join } = require('path');
const { dirSync: newTmpDir } = require('tmp');
const restify = require('restify');
const { BadRequestError, InternalServerError, NotFoundError } = require('restify-errors');
const { createServer } = require('net');
const { once } = require('events');
const LOG = require('loglevel');
const aedes = require('aedes')();
const genericPool = require('generic-pool');
const {promisify} = require('util');

////////////////////////////////////////////////////////////////////////////////
// Global arguments handling
const [, , logLevel] = process.argv;
LOG.setLevel(Number(logLevel));
LOG.getLogger('aedes').setLevel(LOG.levels.WARN);

////////////////////////////////////////////////////////////////////////////////
// Tracking of domains, clones, requests and clone workers
const domains = new Set;
const clones = {/* cloneId: { process, tmpDir, mqtt: { client, server, port } } */ };
const requests = {/* requestId: [res, next] */ };
const pool = genericPool.createPool({
  create() {
    const process = fork(join(__dirname, 'clone.js'), {
      // Strictly disallow unhandled promise rejections for compliance testing
      execArgv: ['--unhandled-rejections=strict']
    });
    // Wait for the 'active' message
    return once(process, 'message').then(() => process);
  },
  destroy(process) {
    process.kill();
    return once(process, 'exit');
  },
  async validate(process) {
    return process.connected;
  }
}, {
  min: 5, // Pre-allocation
  max: 100
});

////////////////////////////////////////////////////////////////////////////////
// HTTP server for orchestration commands
const http = restify.createServer();
http.use(restify.plugins.queryParser());
http.use(restify.plugins.bodyParser());
Object.entries({ start, transact, stop, kill, destroy, partition })
  .forEach(([path, route]) => http.post('/' + path, route));
http.on('after', req => { delete requests[req.id()]; });
http.listen(0, () => {
  const url = `http://localhost:${http.address().port}`;
  LOG.info(logTs(), `Orchestrator listening on ${url}`);
  pool.ready().then(() => {
    process.send({ '@type': 'listening', url },
      err => err && LOG.warn('Orchestrator orphaned', err));
    process.on('exit', async () => {
      LOG.info(logTs(), `Orchestrator shutting down`);
      await pool.drain();
      await pool.clear();
      http.close();
    });
  });
});

////////////////////////////////////////////////////////////////////////////////
// MQTT event handling
aedes.on('publish', function (packet, client) {
  const log = LOG.getLogger('aedes');
  if (client) {
    const { topic, qos, retain } = packet;
    log.debug(
      logTs(), client.id, { topic, qos, retain },
      log.getLevel() <= LOG.levels.TRACE ? packet.payload.toString() : '');
  }
});

aedes.on('client', client => {
  const log = LOG.getLogger('aedes');
  if (client.id in clones) {
    log.debug(logTs(), client.id, 'MQTT client connecting');
    clones[client.id].mqtt.client = client;
  } else {
    log.warn(logTs(), client.id, 'Unexpected MQTT client');
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

////////////////////////////////////////////////////////////////////////////////
// Clone command handlers

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
  LOG.info(logTs(), cloneId, `Starting ${genesis ? 'genesis ' : ''}clone on domain ${domain}`);
  domains.add(domain);

  const mqttServer = createServer(aedes.handle);
  mqttServer.listen(err => {
    if (err)
      return next(new InternalServerError(err));

    const mqttPort = mqttServer.address().port;
    LOG.debug(logTs(), cloneId, `Clone MQTT port is ${mqttPort}`);

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

    pool.acquire().then(process => {
      res.header('transfer-encoding', 'chunked');
      const clone = clones[cloneId] = {
        process, tmpDir, mqtt: { server: mqttServer, port: mqttPort }
      };
      function releaseProcess(cb) {
        LOG.debug(logTs(), cloneId, `Releasing clone process`);
        pool.release(process).then(() => {
          process.off('message', msgHandler);
          delete clone.process;
          releaseCloneBroker(cloneId, clone, cb);
        }).catch(err => LOG.error(logTs(), cloneId, err));
      }
      const msgHandler = message => {
        switch (message['@type']) {
          case 'started':
          case 'updated':
          case 'status':
            return res.write(JSON.stringify(message));
          case 'closed':
            res.end();
            next();
            break;
          case 'unstarted':
            releaseProcess(err => {
              res.status(400);
              res.write(JSON.stringify(new BadRequestError(err || message.err)));
              res.end();
            });
            break;
          case 'next':
            return (function respondNext() {
              const { requestId, body } = message;
              const [res] = requests[requestId];
              res.write(JSON.stringify(body));
            })();
          case 'complete':
            return (function respondComplete() {
              const { requestId } = message;
              const [res, next] = requests[requestId];
              res.end();
              next();
            })();
          case 'error':
            return (function respondError() {
              const { requestId, err } = message;
              const [res, next] = requests[requestId];
              if (res.header('transfer-encoding') === 'chunked') {
                // Restify tries to set content-length, which is bad for chunking
                res.status(500);
                res.write(JSON.stringify(new InternalServerError(err)));
                res.end();
              } else {
                next(new InternalServerError(err));
              }
            })();
          case 'destroyed':
            return (function respondDestroyed() {
              const { requestId } = message;
              const [res, next] = requests[requestId];
              releaseProcess(err => {
                if (err)
                  next(new InternalServerError(err));
                else
                  res.send({ '@type': 'destroyed' });
              });
              destroyDataAndForget(cloneId, tmpDir);
            })();
          case 'stopped':
            return (function respondStopped() {
              const { requestId } = message;
              const [res, next] = requests[requestId];
              releaseProcess(err => {
                if (err)
                  next(new InternalServerError(err));
                else
                  res.send({ '@type': 'stopped' });
              });
            })();
        }
      };
      process.send({ '@type': 'start', config, tmpDirName: tmpDir.name, requestId: req.id() });
      process.on('message', msgHandler);
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
    LOG.debug(logTs(), cloneId, `Stopping clone`);
    process.send({
      id: req.id(), '@type': 'stop'
    }, err => {
      if (err) {
        // This clone process is already killed
        LOG.debug(logTs(), cloneId, `Already killed`, err);
        res.send({ '@type': 'stopped', cloneId });
        next();
      }
    });
  }, next);
}

function kill(req, res, next) {
  const { cloneId } = req.query;
  withClone(cloneId, clone => {
    LOG.debug(logTs(), cloneId, `Killing clone process`);
    clone.process.kill();
    clone.process.on('exit', () => {
      pool.destroy(clone.process).then(() => {
        delete clone.process;
        releaseCloneBroker(cloneId, clone, err => {
          if (err) {
            next(new InternalServerError(err));
          } else {
            res.send({ '@type': 'killed', cloneId });
            next();
          }
        }, { unref: true });
      }).catch(err => LOG.error(logTs(), cloneId, err));
    });
  }, next);
}

function destroy(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId } = req.query;
  withClone(cloneId, ({ process, tmpDir }) => {
    LOG.debug(logTs(), cloneId, `Destroying clone`);
    function alreadyDestroyed() {
      destroyDataAndForget(cloneId, tmpDir);
      res.send({ '@type': 'destroyed', cloneId });
      next();
    }
    if (process) {
      process.send({
        id: req.id(), '@type': 'destroy'
      }, err => {
        if (err) {
          // This clone process is already killed
          LOG.debug(logTs(), cloneId, `Already killed`, err);
          alreadyDestroyed();
        }
      });
    } else {
      alreadyDestroyed();
    }
  }, next);
}

function partition(req, res, next) {
  const { cloneId, state: stateString } = req.query;
  withClone(cloneId, ({ mqtt }) => {
    LOG.debug(logTs(), cloneId, `Partitioning clone (${stateString})`);
    const state = stateString !== 'false';
    if (state && mqtt.server.listening) {
      if (mqtt.client)
        mqtt.client.conn.destroy();
      mqtt.server.close(err => {
        if (err) {
          next(new InternalServerError(err));
        } else {
          LOG.debug(logTs(), cloneId, `MQTT stopped`);
          res.send({ '@type': 'partitioned', state });
          next();
        }
      });
    } else if (!state && !mqtt.server.listening) {
      mqtt.server.listen(mqtt.port, err => {
        if (err) {
          return next(new InternalServerError(err));
        } else {
          LOG.debug(logTs(), cloneId, `MQTT re-started`);
          res.send({ '@type': 'partitioned', state });
          next();
        }
      });
    } else {
      next(new BadRequestError('Partition request does not match MQTT state'));
    }
  });
}

function destroyDataAndForget(cloneId, tmpDir) {
  LOG.info(logTs(), cloneId, `Destroying clone data`);
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

function releaseCloneBroker(cloneId, clone, cb, opts) {
  if (clone.mqtt.server.listening) {
    // Give the broker a chance to shut down. If it does not, this usually
    // indicates that the clone has not released its connection. In that case
    // unref the server to allow this process to exit and signal the error,
    // unless opts.unref is set.
    Promise.race([
      promisify(clone.mqtt.server.close).call(clone.mqtt.server),
      new Promise(fin => setTimeout(fin, 1000, 'timed out'))
    ]).then(result => {
      LOG.debug(logTs(), cloneId, 'Clone broker shutdown', result || 'OK');
      if (result === 'timed out') {
        if (!opts?.unref)
          cb(result);
        clone.mqtt.server.unref();
      } else {
        cb(); // Closed OK
      }
    }).catch(cb);
  } else {
    cb();
  }
}

function logTs() {
  return new Date().toISOString();
}