import { AppPrincipal, Attribution, MeldReadState, MeldTransportSecurity } from '../api';
import { propertyValue } from '../index';
import { MeldMessageType } from '../ns/m-ld';
import * as MsgPack from '../engine/msgPack';
import { Logger } from 'loglevel';
import { M_LD, XS } from '../ns';
import { getRandomValues, subtle } from '../engine/local';
import { MeldError } from '../engine/MeldError';
import { MeldConfig } from '../config';
import { Iri } from '@m-ld/jsonld';
import { Write } from '../jrql-support';
import { getIdLogger } from '../engine/logging';

/** @internal */
const ALGO = {
  ENCRYPT: 'AES-CBC',
  SIGN: 'RSASSA-PKCS1-v1_5'
};

/**
 * [[include:ext/acl.md]]
 *
 * An instance of this class must be included as the `transportSecurity` member
 * of the `MeldApp` for an access-controlled domain. This is because transport
 * security must be available _before_ the clone can connect to the domain.
 *
 * @category Experimental
 * @experimental
 */
export class MeldAclTransportSecurity implements MeldTransportSecurity {
  /**
   * Shared secret declaration. Insert into the domain data to install the
   * extension. For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(MeldAclExtensions.declareSecret('test.m-ld.org', randomBytes(16)));
   * ```
   *
   * @param domainName as declared in the `MeldConfig` of the clone
   * @param aesKey a raw AES key, e.g. `randomBytes(32)`
   */
  static declareSecret = (domainName: string, aesKey: Buffer): Write => ({
    '@id': `http://${domainName}/`,
    [M_LD.secret]: {
      '@type': XS.base64Binary,
      '@value': `${aesKey.toString('base64')}`
    }
  });

  /**
   * Use to register each principal with access to the domain, for example
   * (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(MeldAclTransportSecurity.registerPrincipal(
   *   'https://alice.example/profile#me', alicePublicKeySpki));
   * ```
   *
   * @param principalIri the principal's identity. As for all domain data, the
   * principal's IRI can be relative (e.g. `'fred'`).
   * @param rsaPublicKeySpki DER & SPKI encoded public key belonging to the principal
   */
  static registerPrincipal = (principalIri: Iri, rsaPublicKeySpki: Buffer): Write => ({
    '@id': principalIri,
    [M_LD.publicKey]: {
      '@type': XS.base64Binary,
      '@value': `${rsaPublicKeySpki.toString('base64')}`
    }
  });

  private readonly log: Logger;
  private readonly domainId: string;
  private readonly principal: AppPrincipal;

  constructor(config: MeldConfig, principal: AppPrincipal) {
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel ?? 'info');
    this.domainId = `http://${config['@domain']}/`;
    this.principal = principal;
  }

  async wire(
    data: Buffer,
    type: MeldMessageType,
    direction: 'in' | 'out',
    state: MeldReadState | null
  ): Promise<Buffer> {
    switch (type) {
      case MeldMessageType.operation:
        const key = state != null && await this.getSecretKey(state);
        if (!key)
          this.log.debug('No key available for message encryption');
        else if (direction === 'out')
          return this.encryptOperation(data, key);
        else
          return this.decryptOperation(data, key);
    }
    // Anything else is left alone
    return data;
  }

  async sign(data: Buffer, state: MeldReadState | null): Promise<Attribution> {
    if (this.principal?.sign == null) {
      throw new Error('No signature possible for request');
    } else {
      return {
        pid: this.principal['@id'],
        sig: await this.principal.sign(data)
      };
    }
  }

  async verify(
    data: Buffer,
    attr: Attribution | null,
    state: MeldReadState | null
  ): Promise<void> {
    if (state == null) {
      throw new MeldError('Request rejected', 'No state available to verify signature');
    } else if (attr == null) {
      throw new MeldError('Request rejected', 'Request is not signed');
    } else {
      // Load the identified principal's public key
      const key = await this.getPublicKey(attr.pid, state);
      if (!(await subtle.verify(ALGO.SIGN, key, attr.sig, data)))
        throw new MeldError('Request rejected', 'Signature invalid');
    }
  }

  private async getSecretKey(state: MeldReadState) {
    const domain = await state.get(this.domainId, M_LD.secret);
    if (domain != null) {
      const rawKey = propertyValue(domain, M_LD.secret, Uint8Array);
      return subtle.importKey(
        'raw', rawKey, ALGO.ENCRYPT, false, ['encrypt', 'decrypt']);
    }
  }

  protected async encryptOperation(data: Buffer, key: CryptoKey) {
    const iv = Buffer.from(getRandomValues(new Uint8Array(16)));
    const enc = Buffer.from(await subtle.encrypt({ name: ALGO.ENCRYPT, iv }, key, data));
    return MsgPack.encode({ '@type': M_LD.Encrypted, iv, enc });
  }

  protected async decryptOperation(data: Buffer, key: CryptoKey) {
    const { '@type': type, iv, enc } = MsgPack.decode(data);
    if (type === M_LD.Encrypted) {
      this.log.debug(`Decrypting operation with length ${enc.length}`);
      return Buffer.from(await subtle.decrypt({ name: ALGO.ENCRYPT, iv }, key, enc));
    } else {
      return data;
    }
  }

  protected async getPublicKey(principalId: Iri, state: MeldReadState) {
    const principal = await state.get(principalId, M_LD.publicKey);
    if (principal == null)
      throw new MeldError('Request rejected', 'Signature principal unavailable');
    const rawKey = propertyValue(principal, M_LD.publicKey, Uint8Array);
    return subtle.importKey('spki', rawKey,
      { name: ALGO.SIGN, hash: 'SHA-256' }, false, ['verify']);
  }
}