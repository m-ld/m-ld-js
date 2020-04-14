const { fork } = require('child_process');
const { join } = require('path');

const clones = {/* cloneId: subprocess */ };
const requests = {/* requestId: [res, next] */ };

exports.afterRequest = req => delete requests[req.id()];
exports.onExit = () => Object.values(clones).forEach(p => p.kill());

exports.start = (req, res, next) => {
  const { cloneId, domain } = req.query;
  const cloneProcess = fork(join(__dirname, 'clone.js'), [cloneId, domain]);
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
    }
  };

  cloneProcess.on('message', message => {
    if (message['@type'] in handlers)
      handlers[message['@type']](message);
  });
}

exports.transact = (req, res, next) => {
  const requestId = registerRequest(req, res, next);
  const { cloneId } = req.query;
  if (cloneId in clones) {
    clones[cloneId].send({
      id: requestId,
      '@type': 'transact',
      request: req.body
    });
    res.header('transfer-encoding', 'chunked');
  } else {
    next(new Error(`Clone ${cloneId} not available`));
  }
}

exports.destroy = (req, res, next) => {
  const { cloneId } = req.query;
  global.debug && console.debug(`Destroying clone ${cloneId}`);
  if (cloneId in clones) {
    clones[cloneId].kill();
    delete clones[cloneId];
  }
  res.send({ '@type': 'destroyed', cloneId });
  next();
}

function registerRequest(req, res, next) {
  requests[req.id()] = [res, next];
  return requestId;
}

