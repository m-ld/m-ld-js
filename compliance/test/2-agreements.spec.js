const Clone = require('@m-ld/m-ld-spec/compliance/clone');

describe('ACL Statutes and Permissions', () => {
  let aliceClone;

  beforeEach(async () => {
    aliceClone = new Clone({
      '@context': {
        'mld': 'http://m-ld.org/',
        'sh': 'http://www.w3.org/ns/shacl#'
      },
      principal: { '@id': 'https://alice.example/profile#me' }
    });
    await aliceClone.start();
    // Setup extensions and permissions statute
    await aliceClone.transact({
      '@graph': [{
        // m-ld extensions for statutes and write permissions
        '@id': 'mld:extensions',
        '@list': [{
          '@id': 'http://ext.m-ld.org/constraints/Statutory',
          '@type': 'http://js.m-ld.org/CommonJSModule',
          'http://js.m-ld.org/#require': '@m-ld/m-ld/ext/constraints/Statutory',
          'http://js.m-ld.org/#class': 'Statutory'
        }, {
          '@id': 'http://ext.m-ld.org/constraints/WritePermitted',
          '@type': 'http://js.m-ld.org/CommonJSModule',
          'http://js.m-ld.org/#require': '@m-ld/m-ld/ext/constraints/WritePermitted',
          'http://js.m-ld.org/#class': 'WritePermitted'
        }]
      }, {
        // Declare a shape for permissions
        '@id': 'has-permission-shape',
        'sh:path': { '@id': 'mld:#has-permission' }
      }, {
        // Permissions are statutory, and require authority
        '@type': 'mld:Statute',
        'mld:#statutory-shape': { '@id': 'has-permission-shape' },
        'mld:#sufficient-condition': { '@id': 'mld:#has-authority' }
      }, {
        // Alice has authority over permissions
        '@id': 'https://alice.example/profile#me',
        'mld:#has-authority': { '@id': 'has-permission-shape' }
      }]
    });
  });

  it('allows ACL setup', async () => {
    // Now, if we add a permission...
    aliceClone.transact({
      '@id': 'https://alice.example/profile#me',
      'mld:#has-permission': {
        '@type': 'mld:WritePermission',
        'mld:#controlled-shape': { 'sh:path': { '@vocab': 'invoice-state' } }
      }
    });
    // The update should be an agreement
    const update = await aliceClone.updated('@insert', 'https://alice.example/profile#me');
    expect(update['@agree']).toBe(true);
  });

  it('disallows unauthorised agreement', async () => {
    const bobClone = new Clone({
      '@context': {
        'mld': 'http://m-ld.org/',
        'sh': 'http://www.w3.org/ns/shacl#'
      },
      principal: { '@id': 'https://bob.example/profile#me' }
    });
    await bobClone.start();
    // Bob tries to give himself permission
    await expectAsync(bobClone.transact({
      '@id': 'https://bob.example/profile#me',
      'mld:#has-permission': {
        '@type': 'mld:WritePermission',
        'mld:#controlled-shape': { 'sh:path': { '@vocab': 'invoice-state' } }
      }
    })).toBeRejectedWithError(/not provable/);
  });

  it('disallows unauthorised change', async () => {
    await aliceClone.transact({
      '@id': 'https://alice.example/profile#me',
      'mld:#has-permission': {
        '@type': 'mld:WritePermission',
        'mld:#controlled-shape': { 'sh:path': { '@vocab': 'invoice-state' } }
      }
    });
    const bobClone = new Clone({ principal: { '@id': 'https://bob.example/profile#me' } });
    await bobClone.start();
    // Bob tries to create an invoice with a state
    await expectAsync(bobClone.transact({
      '@id': 'my-invoice',
      'invoice-state': 'BOB'
    })).toBeRejectedWithError(/Unauthorised/);
  });

  it('allows a newly-authorised change', async () => {
    await aliceClone.transact({
      '@id': 'https://alice.example/profile#me',
      'mld:#has-permission': {
        '@id': 'invoiceStatePermission',
        '@type': 'mld:WritePermission',
        'mld:#controlled-shape': { 'sh:path': { '@vocab': 'invoice-state' } }
      }
    });
    const bobClone = new Clone({ principal: { '@id': 'https://bob.example/profile#me' } });
    await bobClone.start();
    // Now give Bob permission
    await aliceClone.transact({
      '@id': 'https://bob.example/profile#me',
      'mld:#has-permission': { '@id': 'invoiceStatePermission' }
    });
    await bobClone.updated('@insert', 'https://bob.example/profile#me');
    // Bob tries to create an invoice with a state
    await expectAsync(bobClone.transact({
      '@id': 'my-invoice',
      'invoice-state': 'BOB'
    })).toBeResolved();
  });

  describe('voiding', () => {
    beforeEach(async () => {
      // Start with both users having permission
      await aliceClone.transact({
        '@graph': [{
          '@id': 'invoiceStatePermission',
          '@type': 'mld:WritePermission',
          'mld:#controlled-shape': { 'sh:path': { '@vocab': 'invoice-state' } }
        }, {
          '@id': 'https://alice.example/profile#me',
          'mld:#has-permission': { '@id': 'invoiceStatePermission' }
        }, {
          '@id': 'https://bob.example/profile#me',
          'mld:#has-permission': { '@id': 'invoiceStatePermission' }
        }, {
          '@id': 'my-invoice',
          'invoice-state': 'ALICE'
        }]
      });
    });

    it('voids an incompatible change', async () => {
      const bobClone = new Clone({ principal: { '@id': 'https://bob.example/profile#me' } });
      await bobClone.start();
      // Simultaneously revoke permission, and make the permitted change
      await Promise.all([
        bobClone.transact({
          '@id': 'my-invoice',
          'invoice-state': 'BOB'
        }),
        aliceClone.transact({
          '@delete': {
            '@id': 'https://bob.example/profile#me',
            'mld:#has-permission': { '@id': 'invoiceStatePermission' }
          }
        }),
        // (Wait for the revocation to get to Bob's clone)
        bobClone.updated('@delete', 'https://bob.example/profile#me')
      ]);
      // Expect the invoice to have the former state in both clones
      for (let clone of [aliceClone, bobClone]) {
        const [invoice] = await clone.transact({ '@describe': 'my-invoice' });
        expect(invoice['invoice-state']).toBe('ALICE');
      }
    });

    it('voids incompatible caused changes', async () => {
      const bobClone = new Clone({ principal: { '@id': 'https://bob.example/profile#me' } });
      await bobClone.start();
      const claireClone = new Clone();
      await claireClone.start();
      // Isolate Alice
      await aliceClone.partition();
      await aliceClone.transact({
        '@delete': {
          '@id': 'https://bob.example/profile#me',
          'mld:#has-permission': { '@id': 'invoiceStatePermission' }
        }
      });
      // Claire gets Bob's change
      await Promise.all([
        bobClone.transact({
          '@id': 'my-invoice',
          'invoice-state': 'BOB'
        }),
        claireClone.updated('invoice-state', 'BOB')
      ]);
      // and makes a further change (not permission-controlled)
      await claireClone.transact({
        '@id': 'my-invoice',
        'quantity': 3
      });
      // Bring Alice back online, and wait for Claire and Bob to react
      await Promise.all([
        aliceClone.partition(false),
        bobClone.updated('@delete', 'https://bob.example/profile#me'),
        claireClone.updated('@delete', 'https://bob.example/profile#me')
      ]);
      // Expect the invoice to have the former state in all clones
      for (let clone of [aliceClone, bobClone, claireClone]) {
        const [invoice] = await clone.transact({ '@describe': 'my-invoice' });
        expect(invoice['invoice-state']).toBe('ALICE');
        expect(invoice['quantity']).toBeUndefined();
      }
    });
  });
});