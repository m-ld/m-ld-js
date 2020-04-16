const { fork } = require('child_process');
const { join } = require('path');
const { dirSync } = require('tmp');
const { BadRequestError, InternalError, NotFoundError } = require('restify-errors');

const clones = {/* cloneId: [subprocess, tmpDir] */ };
const requests = {/* requestId: [res, next] */ };

exports.routes = { start, transact, stop, kill, destroy };
exports.afterRequest = req => delete requests[req.id()];
exports.onExit = () => Object.values(clones).forEach(([p,]) => p && p.kill());

function start(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId, domain } = req.query;
  let tmpDir;
  if (cloneId in clones) {
    tmpDir = clones[cloneId][1];
    if (clones[cloneId][0])
      return next(new BadRequestError(`Clone ${cloneId} is already started`));
  } else {
    tmpDir = dirSync({ unsafeCleanup: true });
  }
  console.info(`${cloneId}: Starting clone on domain ${domain}`);
  const cloneProcess = fork(join(__dirname, 'clone.js'),
    [cloneId, domain, tmpDir.name, req.id()]);
  clones[cloneId] = [cloneProcess, tmpDir];
  const handlers = {
    started: message => {
      res.send(message);
      next(false);
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
      next(new InternalError(err));
    },
    destroyed: message => {
      const { requestId } = message;
      const [res, next] = requests[requestId];
      console.info(`${cloneId}: Destroying clone data`);
      tmpDir.removeCallback();
      killCloneProcess(cloneId, 'destroyed', res, next);
      delete clones[cloneId];
    },
    stopped: message => {
      const { requestId } = message;
      const [res, next] = requests[requestId];
      killCloneProcess(cloneId, 'stopped', res, next);
    }
  };
  cloneProcess.on('message', message => {
    if (message['@type'] in handlers)
      handlers[message['@type']](message);
  });
}

function transact(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId } = req.query;
  withClone(cloneId, cloneProcess => {
    cloneProcess.send({
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
  withClone(cloneId, cloneProcess => {
    global.debug && console.debug(`${cloneId}: Stopping clone`);
    cloneProcess.send({ id: req.id(), '@type': 'stop' });
  }, next);
}

function kill(req, res, next) {
  const { cloneId } = req.query;
  killCloneProcess(cloneId, 'killed', res, next);
}

function destroy(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId } = req.query;
  withClone(cloneId, cloneProcess => {
    global.debug && console.debug(`${cloneId}: Destroying clone`);
    cloneProcess.send({ id: req.id(), '@type': 'destroy' });
  }, next);
}

function registerRequest(req, res, next) {
  requests[req.id()] = [res, next];
}

function withClone(cloneId, op/*(subprocess, tmpDir)*/, next) {
  if (cloneId in clones) {
    op(...clones[cloneId]);
  } else {
    next(new NotFoundError(`Clone ${cloneId} not available`));
  }
}

function killCloneProcess(cloneId, type, res, next) {
  withClone(cloneId, (cloneProcess, tmpDir) => {
    global.debug && console.debug(`${cloneId}: Killing clone process`);
    cloneProcess.kill();
    cloneProcess.on('exit', () => {
      clones[cloneId] = [null, tmpDir];
      res.send({ '@type': type, cloneId });
      next();
    });
  }, next);
}