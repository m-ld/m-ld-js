import { MeldAccessControlList } from '../src/security';
import { memStore, testConfig } from './testClones';
import { MeldMessageType } from '../src/ns/m-ld';
import { M_LD, XS } from '../src/ns';
import { subtle } from '../src/engine/local';
import { JrqlGraph } from '../src/engine/dataset/JrqlGraph';
import { Dataset } from '../src/engine/dataset/index';
import { ActiveContext } from 'jsonld/lib/context';
import { initialCtx } from '../src/engine/jsonld';

describe('m-ld access control list', () => {
  let acl: MeldAccessControlList;
  const data = Buffer.from('data');

  beforeEach(() => {
    acl = new MeldAccessControlList(testConfig());
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
    let dataset: Dataset;
    let unlock: () => void;
    let graph: JrqlGraph;
    let ctx: ActiveContext;

    beforeEach(async () => {
      dataset = await memStore();
      unlock = await dataset.lock.acquire('state', 'test', 'share');
      graph = new JrqlGraph(dataset.graph());
      ctx = initialCtx();
    });

    afterEach(() => unlock());

    test('encrypts with domain secret', async () => {
      const key = await subtle.generateKey(
        { name: 'AES-CBC', length: 128 },
        true,
        ['encrypt', 'decrypt']
      );
      const rawKey = Buffer.from(await subtle.exportKey('raw', key));
      await dataset.transact({
        prepare: async () => ({
          patch: await graph.write({
            '@id': 'http://test.m-ld.org/',
            [M_LD.secret]: {
              '@type': XS.base64Binary,
              '@value': rawKey.toString('base64')
            }
          }, ctx)
        })
      });
      const wired = await acl.wire(
        data, MeldMessageType.operation, 'out', graph.asReadState);
      expect(wired.equals(data)).toBe(false); // Dunno, but not the same!
      // Apply symmetric decryption
      const unwired = await acl.wire(
        wired, MeldMessageType.operation, 'in', graph.asReadState);
      expect(unwired.equals(data)).toBe(true);
    });

    test('signs with principal', async () => {
      const keyPair = await subtle.generateKey(
        {
          name: 'RSASSA-PKCS1-v1_5',
          modulusLength: 2048,
          publicExponent: new Uint8Array([1, 0, 1]),
          hash: 'SHA-256'
        },
        true,
        ['sign', 'verify']
      );
      acl.setPrincipal({
        '@id': 'http://ex.org/Fred',
        async sign(data: Buffer) {
          return Buffer.from(await subtle.sign('RSASSA-PKCS1-v1_5', keyPair.privateKey!, data));
        }
      });
      const rawPublicKey = Buffer.from(await subtle.exportKey('spki', keyPair.publicKey!));
      await dataset.transact({
        prepare: async () => ({
          patch: await graph.write({
            '@id': 'http://ex.org/Fred',
            [M_LD.publicKey]: {
              '@type': XS.base64Binary,
              '@value': rawPublicKey.toString('base64')
            }
          }, ctx)
        })
      });
      const wired = await acl.wire(
        data, MeldMessageType.request, 'out', graph.asReadState);
      expect(wired.equals(data)).toBe(false); // Dunno, but not the same!
      // Apply asymmetric verify
      const unwired = await acl.wire(
        wired, MeldMessageType.request, 'in', graph.asReadState);
      expect(unwired.equals(data)).toBe(true);
    });
  });
});