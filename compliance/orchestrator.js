const { fork } = require('child_process');
const { join } = require('path');

const clones = {/* cloneId: subprocess */ };
const requests = {/* requestId: [res, next] */ };

exports.routes = { start, transact, destroy };
exports.afterRequest = req => delete requests[req.id()];
exports.onExit = () => Object.values(clones).forEach(p => p.kill());

function start(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId, domain } = req.query;
  const cloneProcess = fork(join(__dirname, 'clone.js'), [cloneId, domain, req.id()]);
  clones[cloneId] = cloneProcess;

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
      next(new Error(err));
    },
    destroyed: message => {
      const { requestId } = message;
      const [res, next] = requests[requestId];
      clones[cloneId].kill();
      delete clones[cloneId];
      res.send({ '@type': 'destroyed', cloneId });
      next();
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
  if (cloneId in clones) {
    clones[cloneId].send({
      id: req.id(),
      '@type': 'transact',
      request: req.body
    });
    res.header('transfer-encoding', 'chunked');
  } else {
    next(new Error(`Clone ${cloneId} not available`));
  }
}

function destroy(req, res, next) {
  registerRequest(req, res, next);
  const { cloneId } = req.query;
  if (cloneId in clones) {
    global.debug && console.debug(`Destroying clone ${cloneId}`);
    clones[cloneId].send({ id: req.id(), '@type': 'destroy' });
  }
}

function registerRequest(req, res, next) {
  requests[req.id()] = [res, next];
}

