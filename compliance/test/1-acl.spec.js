const { generateKeyPairSync, randomBytes } = require('crypto');
const Clone = require('@m-ld/m-ld-spec/compliance/clone');

/**
 * Generates a public/private key pair suitable for request signing and
 * verification. The public key will be entered into the domain data, acting as
 * a registration of a domain user. The private key will be used locally by the
 * app to sign requests. Note that since this test animates the app via an HTTP
 * API, this private key must first be passed to the [clone](../clone.js) in the
 * test setup. This is acceptable here only because the HTTP API is a
 * contrivance for platform-independent compliance testing. Real-world m-ld apps
 * have the clone in-process and therefore do not need transfer of private
 * credentials over a network connection.
 *
 * @returns {{ publicKey: Buffer, privateKey: string }}
 */
const genPrincipalKeys = () => generateKeyPairSync('rsa', {
  modulusLength: 1024,
  publicKeyEncoding: { type: 'spki', format: 'der' },
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' }
});

/**
 * Configuration for transport security. The given module and export will be
 * instantiated in the app just prior to creating the [clone](../clone.js). This
 * indirection is only present because the HTTP API is a contrivance for
 * platform-independent compliance testing. Nevertheless, it is suggestive of a
 * data structure to be used in future for registration of the extension in the
 * domain itself.
 *
 * @type {{require: string, export: string}}
 */
const transportSecurity = {
  require: '@m-ld/m-ld/dist/security',
  export: 'MeldAccessControlList'
};

// generateKeySync('aes') was only added in Node 15
const secret = randomBytes(16).toString('base64');

/**
 * Compliance tests for access control lists using the above transport security
 * extension and principal keys.
 */
describe('Genesis clone with ACL', () => {
  let aliceClone, bobClone;
  let alice, bob;

  beforeAll(() => {
    // Create the principals (users) with @id, publicKey and privateKey
    alice = { '@id': 'https://alice.example/profile#me', ...genPrincipalKeys() };
    bob = { '@id': 'https://bob.example/profile#me', ...genPrincipalKeys() };
  });

  beforeEach(async () => {
    // Initialise Alice's clone with the Alice principal and transport security
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
    // No principal OR transport security on Bob's clone
    bobClone = new Clone();
    await expectAsync(bobClone.start())
      .toBeRejectedWithError(/rejected/);
  });

  it('cannot start clone with no user', async () => {
    // No principal on Bob's clone
    bobClone = new Clone({ transportSecurity });
    await expectAsync(bobClone.start())
      .toBeRejectedWithError(/No signature possible/);
  });

  it('rejects clone for unregistered user', async () => {
    // Bob has a principal and transport security set up on his clone, but has
    // not been registered as a user.
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
      // Add both Alice and Bob as registered users in the domain data (by
      // adding them in Alice's clone)
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
      // Bob has a principal and transport security set up on his clone, and has
      // been registered as a user, so we expect the clone will start
      // successfully.
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
        // Set up the domain secret to be used for encrypting operations
        aliceClone.transact({
          '@id': `http://${aliceClone.domain}/`,
          'http://m-ld.org/#secret': {
            '@type': 'http://www.w3.org/2001/XMLSchema#base64Binary',
            '@value': secret
          }
        });
      });

      it('shares the secret', async () => {
        // Checking that the secret is received (i.e. this operation is not
        // encrypted) – note that we assume TLS on the messaging system
        await expectAsync(bobClone.updated('@insert', secret)).toBeResolved();
      });

      it('can make further transactions', async () => {
        await bobClone.updated('@insert', secret);
        await aliceClone.transact({ '@id': 'fred', name: 'Fred' });
        // Checking that encrypted operations like this one are received
        await expectAsync(bobClone.updated('@insert', 'fred')).toBeResolved();
      });
    });
  });

  afterEach(() => Promise.all([aliceClone, bobClone].map(c => c?.destroy())));
});