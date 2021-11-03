import { MeldMemDown } from '../src/memdown';

test('m-ld MemDown', done => {
  const tape = require('tape');
  const suite = require('abstract-leveldown/test');
  tape.onFinish(done);
  tape.onFailure(() => done('Tape tests failed'));
  // noinspection JSUnusedGlobalSymbols
  suite({
    test: tape,
    factory: () => new MeldMemDown(),
    bufferKeys: false,
    // Test options copied from memdown
    // https://github.com/Level/memdown/blob/master/test.js
    clear: true,
    getMany: true,
    createIfMissing: false,
    errorIfExists: false
  });
}, 10000);