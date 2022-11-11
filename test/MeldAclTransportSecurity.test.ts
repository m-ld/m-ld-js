import { MeldAclTransportSecurity } from '../src/security';
import { MockGraphState, testConfig, testContext } from './testClones';
import { MeldMessageType } from '../src/ns/m-ld';
import { subtle } from '../src/engine/local';

describe('m-ld access control list', () => {
  let acl: MeldAclTransportSecurity;
  let aliceKeys: CryptoKeyPair;
  const data = Buffer.from('data');
  
  beforeAll(async () => {
    aliceKeys = await subtle.generateKey(
      {
        name: 'RSASSA-PKCS1-v1_5',
        modulusLength: 2048,
        publicExponent: new Uint8Array([1, 0, 1]),
        hash: 'SHA-256'
      },
      true,
      ['sign', 'verify']
    );
  });

  beforeEach(async () => {
    acl = new MeldAclTransportSecurity(testConfig(), {
      '@id': 'http://ex.org/Alice',
      async sign(data: Buffer) {
        return Buffer.from(await subtle.sign('RSASSA-PKCS1-v1_5', aliceKeys.privateKey!, data));
      }
    });
  });

  test('does not encrypt if no key', async () => {
    const wired = await acl.wire(data, MeldMessageType.operation, 'out', null);
    expect(wired.equals(data)).toBe(true);
  });

  test('does not decrypt if no key', async () => {
    const unwired = await acl.wire(data, MeldMessageType.operation, 'in', null);
    expect(unwired.equals(data)).toBe(true);
  });

  describe('crypto with metadata', () => {
    let state: MockGraphState;

    beforeEach(async () => {
      state = await MockGraphState.create({ context: testContext });
    });

    afterEach(() => state.close());

    test('encrypts with domain secret', async () => {
      const key = await subtle.generateKey(
        { name: 'AES-CBC', length: 128 },
        true,
        ['encrypt', 'decrypt']
      );
      await state.write(MeldAclTransportSecurity.declareSecret('test.m-ld.org',
        Buffer.from(await subtle.exportKey('raw', key))));
      const wired = await acl.wire(
        data, MeldMessageType.operation, 'out', state.graph.asReadState);
      expect(wired.equals(data)).toBe(false); // Dunno, but not the same!
      // Apply symmetric decryption
      const unwired = await acl.wire(
        wired, MeldMessageType.operation, 'in', state.graph.asReadState);
      expect(unwired.equals(data)).toBe(true);
    });

    test('signs with principal', async () => {
      const rawPublicKey = Buffer.from(await subtle.exportKey('spki', aliceKeys.publicKey!));
      await state.write(
        MeldAclTransportSecurity.registerPrincipal('http://ex.org/Alice', rawPublicKey));
      const attr = await acl.sign(data, state.graph.asReadState);
      expect(attr.sig.length).toBeGreaterThan(0);
      // Asymmetric verify does not throw
      await expect(acl.verify(data, attr, state.graph.asReadState)).resolves.not.toThrow();
    });
  });
});