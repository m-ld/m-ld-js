const { generateKeyPairSync, generateKeySync } = require('crypto');
const Clone = require('@m-ld/m-ld-spec/compliance/clone');

/** @returns {{ publicKey: Buffer, privateKey: string }} */
const genPrincipalKeys = () => generateKeyPairSync('rsa', {
  modulusLength: 1024,
  publicKeyEncoding: { type: 'spki', format: 'der' },
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' }
});

const transportSecurity = {
  require: '@m-ld/m-ld/dist/security',
  export: 'MeldAccessControlList'
};

const secret = generateKeySync('aes', { length: 128 })
  .export({ format: 'buffer' }).toString('base64');

/**
 * Compliance tests for access control lists
 */
describe('Genesis clone with ACL', () => {
  let aliceClone, bobClone;
  let alice, bob;

  beforeAll(() => {
    alice = { '@id': 'https://alice.example/profile#me', ...genPrincipalKeys() };
    bob = { '@id': 'https://bob.example/profile#me', ...genPrincipalKeys() };
  });

  beforeEach(async () => {
    aliceClone = new Clone({
      principal: {
        '@id': alice['@id'],
        privateKey: alice.privateKey
      },
      transportSecurity
    });
    await aliceClone.start();
  });

  it('rejects clone with no security', async () => {
    bobClone = new Clone();
    await expectAsync(bobClone.start())
      .toBeRejectedWithError(/rejected/);
  });

  it('cannot start clone with no user', async () => {
    bobClone = new Clone({ transportSecurity });
    await expectAsync(bobClone.start())
      .toBeRejectedWithError(/No signature possible/);
  });

  it('rejects clone for unregistered user', async () => {
    bobClone = new Clone({
      principal: {
        '@id': bob['@id'],
        privateKey: bob.privateKey
      },
      transportSecurity
    });
    await expectAsync(bobClone.start())
      .toBeRejectedWithError(/rejected/);
  });

  describe('with registered user', () => {
    beforeEach(async () => {
      await aliceClone.transact({
        '@id': alice['@id'],
        'http://m-ld.org/#publicKey': {
          '@type': 'http://www.w3.org/2001/XMLSchema#base64Binary',
          '@value': alice.publicKey.toString('base64')
        }
      });
      await aliceClone.transact({
        '@id': bob['@id'],
        'http://m-ld.org/#publicKey': {
          '@type': 'http://www.w3.org/2001/XMLSchema#base64Binary',
          '@value': bob.publicKey.toString('base64')
        }
      });
      bobClone = new Clone({
        principal: {
          '@id': bob['@id'],
          privateKey: bob.privateKey
        },
        transportSecurity
      });
    });

    it('accepts clone for registered user', async () => {
      await expectAsync(bobClone.start()).toBeResolved();
    });

    describe('with operation encryption secret', () => {
      beforeEach(async () => {
        await bobClone.start();
        aliceClone.transact({
          '@id': `http://${aliceClone.domain}/`,
          'http://m-ld.org/#secret': {
            '@type': 'http://www.w3.org/2001/XMLSchema#base64Binary',
            '@value': secret
          }
        });
      });

      it('shares the secret', async () => {
        await expectAsync(bobClone.updated('@insert', secret)).toBeResolved();
      });

      it('can make further transactions', async () => {
        await bobClone.updated('@insert', secret);
        await aliceClone.transact({ '@id': 'fred', name: 'Fred' });
        await expectAsync(bobClone.updated('@insert', 'fred')).toBeResolved();
      });
    });
  });

  afterEach(() => Promise.all([aliceClone, bobClone].map(c => c?.destroy())));
});