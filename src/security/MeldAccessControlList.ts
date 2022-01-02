import { AppPrincipal, MeldReadState, MeldTransportSecurity } from '../api';
import { propertyValue } from '../index';
import { MeldMessageType } from '../ns/m-ld';
import { getIdLogger, MsgPack } from '../engine/util';
import { Logger } from 'loglevel';
import { M_LD } from '../ns';
import { getRandomValues, subtle } from '../engine/local';
import { MeldError } from '../engine/MeldError';
import { MeldConfig } from '../config';
import { Iri } from 'jsonld/jsonld-spec';

const ALGO = {
  ENCRYPT: 'AES-CBC',
  SIGN: 'RSASSA-PKCS1-v1_5'
};

/**
 * The m-ld Access Control List (m-ld ACL) extension provides authorisation
 * controls on a m-ld domain using principals and permissions registered in the
 * domain data.
 *
 * At present the implemented control is whole-domain read/write authorisation.
 *
 * This extension requires an {@link AppPrincipal} object to be available in the
 * app, which signs data using
 * [RSASSA-PKCS1-v1_5](https://datatracker.ietf.org/doc/html/rfc3447).
 *
 * The following pattern should be used for the domain data:
 *
 * ```typescript
 * domain = {
 *   '@id': `http://${domain-name}/`,
 *   'http://m-ld.org/#secret': {
 *     '@type': 'http://www.w3.org/2001/XMLSchema#base64Binary',
 *     '@value': `${base-64-encoded-AES-key}`
 *   }
 * }
 *
 * principal = {
 *   '@id': `${principal-iri}`,
 *   'http://m-ld.org/#publicKey': {
 *     '@type': 'http://www.w3.org/2001/XMLSchema#base64Binary',
 *     '@value': `${base-64-encoded-RSA-public-key-spki}`
 *   }
 * }
 * ```
 */
export class MeldAccessControlList implements MeldTransportSecurity {
  private readonly log: Logger;
  private readonly domainId: string;
  private principal?: AppPrincipal;

  constructor(config: MeldConfig) {
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel ?? 'info');
    this.domainId = `http://${config['@domain']}/`;
  }

  setPrincipal(principal: AppPrincipal | undefined) {
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
        return this.cryptOperation(data, direction, state);
      case MeldMessageType.request:
        if (direction === 'out')
          return this.signRequest(data);
        else
          return this.verifyRequest(data, state);
    }
    return data;
  }

  private async getSecretKey(state: MeldReadState) {
    const domain = await state.get(this.domainId, M_LD.secret);
    if (domain != null) {
      const rawKey = propertyValue(domain, M_LD.secret, Uint8Array);
      return subtle.importKey(
        'raw', rawKey, ALGO.ENCRYPT, false, ['encrypt', 'decrypt']);
    }
  }

  private async cryptOperation(
    data: Buffer,
    direction: 'in' | 'out',
    state: MeldReadState | null
  ): Promise<Buffer> {
    const key = state != null ? await this.getSecretKey(state) : undefined;
    if (key == null) {
      this.log.debug('No key available for message encryption');
      return data;
    } else {
      if (direction === 'out') {
        return this.encryptOperation(data, key);
      } else {
        return this.decryptOperation(data, key);
      }
    }
  }

  protected async encryptOperation(data: Buffer, key: CryptoKey) {
    const iv = Buffer.from(getRandomValues(new Uint8Array(16)));
    const enc = Buffer.from(await subtle.encrypt({ name: ALGO.ENCRYPT, iv }, key, data));
    return MsgPack.encode({ '@type': M_LD.encrypted, iv, enc });
  }

  protected async decryptOperation(data: Buffer, key: CryptoKey) {
    const { '@type': type, iv, enc } = MsgPack.decode(data);
    if (type === M_LD.encrypted) {
      this.log.debug(`Decrypting operation with length ${enc.length}`);
      return Buffer.from(await subtle.decrypt({ name: ALGO.ENCRYPT, iv }, key, enc));
    } else {
      return data;
    }
  }

  protected async signRequest(data: Buffer): Promise<Buffer> {
    if (this.principal?.sign == null) {
      throw new Error('No signature possible for request');
    } else {
      return MsgPack.encode({
        '@type': M_LD.signed,
        data,
        pid: this.principal['@id'],
        sig: await this.principal.sign(data)
      });
    }
  }

  protected async verifyRequest(signedData: Buffer, state: MeldReadState | null): Promise<Buffer> {
    if (state == null)
      throw new MeldError('Request rejected', 'No state available to verify signature');
    const { '@type': type, data, pid, sig } = MsgPack.decode(signedData);
    if (type === M_LD.signed && typeof pid == 'string') {
      // Load the identified principal's public key
      const key = await this.getPublicKey(pid, state);
      if (!(await subtle.verify(ALGO.SIGN, key, sig, data)))
        throw new MeldError('Request rejected', 'Signature invalid');
      return data;
    } else {
      throw new MeldError('Request rejected', 'Request is not signed');
    }
  }

  protected async getPublicKey(principalId: Iri, state: MeldReadState) {
    const principal = await state.get(principalId, M_LD.publicKey);
    if (principal == null)
      throw new MeldError('Request rejected', 'Signature principal unavailable');
    const rawKey = propertyValue(principal, M_LD.publicKey, Uint8Array);
    return subtle.importKey(
      'spki', rawKey, { name: ALGO.SIGN, hash: 'SHA-256' }, false, ['verify']);
  }
}