<!--
Class documentation for @m-ld/ext/security MeldAclExtensions
-->

The **m-ld** Access Control List (**m-ld** ACL) extensions provide authorisation controls on a m-ld domain using principals (users) and permissions registered in the domain data.

### Domain Installation

The extensions are installed in a domain by writing metadata into the domain using the provided static declaration. This only needs to be done once, typically by the app instance that creates the domain, having instantiated a 'genesis' clone. The metadata includes a global AES secret key used to encrypt operation messages.

<!-- TODO: Secret key rotation -->

For example, assuming a **m-ld** `clone` object:

```typescript
await clone.write(MeldAclExtensions.declare(0, 'test.m-ld.org', randomBytes(16)));
```

The `@m-ld/m-ld/ext/security` code module must be available to a global CommonJS-style `require` method in all clones using the Javascript engine.
- In Node.js, the module is packaged in `@m-ld/m-ld`; no additional configuration is required.
- In the browser, `require` is typically provided by the bundler. Since the module will be loaded dynamically, the bundler may need to be configured to guarantee the module is bundled, since it _may_ not be referenced statically by any code.

> 🚧 ES6 modules will be supported in a future release.

### Clone Initialisation

For **every clone**, the following must be provided to the clone initialisation function as members of the `MeldApp`:

1. An instance of the `MeldAclTransportSecurity` class, as the `transportSecurity` member (this is because transport security must be available to the clone _before_ it can connect to an access-controlled domain).
1. An {@link AppPrincipal} object as the `principal` member, which represents the current logged-in user. This object will sign data using [RSASSA-PKCS1-v1_5](https://datatracker.ietf.org/doc/html/rfc3447) on the extension's request.

In the example code in the Principals section, this would be performed using `aliceKeys.privateKey`:

```typescript
sign = (data: Buffer) => createSign('RSA-SHA256')
  .update(data).sign(aliceKeys.privateKey)
```

### Registering Principals (Users)

This extension requires each authorised principal to be written into the domain, having an identity and RSA public key.

The following illustrates how to register a principal, using the Node.js `crypto` module:

```typescript
const aliceKeys = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding: { type: 'spki', format: 'der' },
  privateKeyEncoding: { type: 'pkcs1', format: 'pem' }
});
await clone.write(MeldAclExtensions.registerPrincipal(
  'https://alice.example/profile#me', aliceKeys.publicKey));
```
