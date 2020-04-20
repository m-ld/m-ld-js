const { fork } = require('child_process');
const { join } = require('path');
const { dirSync } = require('tmp');
const { BadRequestError, InternalServerError, NotFoundError } = require('restify-errors');
const { createServer } = require('net');
const inspector = require('inspector');
const aedes = require('aedes')();
const clones = {/* cloneId: { process, tmpDir, mqtt: { client, server } } */ };
const requests = {/* requestId: [res, next] */ };

exports.routes = { start, transact, stop, kill, destroy, partition };
exports.afterRequest = req => delete requests[req.id()];
exports.onExit = () => Object.values(clones).forEach(
  ({ process }) => process && process.kill());

aedes.on('publish', function (packet, client) {
  if (client) {
    const { topic, qos, retain } = packet;
    global.debug && console.log(
      client.id, { topic, qos, retain },
      global.debug > 1 ? packet.payload.toString() : '');
  }
});

aedes.on('client', client => {
  if (client.id in clones) {
    global.debug && console.debug(`${client.id}: MQTT client connecting`);
    clones[client.id].mqtt.client = client;
  } else {
    console.warn(`${client.id}: Unexpected MQTT client`);
  }
});

function start(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId, domain } = req.query;
  res.header('transfer-encoding', 'chunked');
  let tmpDir;
  if (cloneId in clones) {
    tmpDir = clones[cloneId].tmpDir;
    if (clones[cloneId].process)
      return next(new BadRequestError(`Clone ${cloneId} is already started`));
  } else {
    tmpDir = dirSync({ unsafeCleanup: true });
  }
  console.info(`${cloneId}: Starting clone on domain ${domain}`);

  const mqttServer = createServer(aedes.handle);
  mqttServer.listen(err => {
    global.debug && console.debug(`${cloneId}: Clone MQTT port is ${mqttServer.address().port}`);
    if (err)
      return next(new InternalServerError(err));

    clones[cloneId] = {
      process: fork(join(__dirname, 'clone.js'),
        [cloneId, domain, tmpDir.name, req.id(), mqttServer.address().port],
        { execArgv: inspector.url() ? ['--inspect-brk=40895'] : [] }),
      tmpDir,
      mqtt: { server: mqttServer }
    };

    const handlers = {
      started: message => res.write(JSON.stringify(message)),
      updated: message => res.write(JSON.stringify(message)),
      closed: () => {
        res.end();
        next(false);
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
        next(false);
      },
      error: message => {
        const { requestId, err } = message;
        const [, next] = requests[requestId];
        next(new InternalServerError(err));
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
    global.debug && console.debug(`${cloneId}: Stopping clone`);
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
    global.debug && console.debug(`${cloneId}: Destroying clone`);
    if (process) {
      process.send({ id: req.id(), '@type': 'destroy' });
    } else {
      destroyData(cloneId, tmpDir);
      delete clones[cloneId];
      res.send({ '@type': 'destroyed', cloneId });
      next(false);
    }
  }, next);
}

function partition(req, res, next) {
  const { cloneId } = req.query;
  withClone(cloneId, ({ mqtt }) => {
    global.debug && console.debug(`${cloneId}: Partitioning clone`);
    if (mqtt.server.listening) {
      if (mqtt.client)
        mqtt.client.conn.destroy();
      mqtt.server.close(err => {
        if (err) {
          next(new InternalServerError(err));
        } else {
          res.send({ '@type': 'partitioned' });
          next(false);
        }
      });
    }
  });
}

function destroyData(cloneId, tmpDir) {
  console.info(`${cloneId}: Destroying clone data`);
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
    global.debug && console.debug(`${cloneId}: Killing clone process`);
    clone.process.kill();
    clone.process.on('exit', () => {
      clone.process = null;
      function done(err) {
        if (err) {
          next(new InternalServerError(err));
        } else {
          res.send({ '@type': reason, cloneId });
          next();
        }
      }
      if (reason instanceof Error) {
        next(reason);
      } else {
        if (clone.mqtt.server.listening)
          clone.mqtt.server.close(done);
        else
          done();
      }
    });
  }, next);
}