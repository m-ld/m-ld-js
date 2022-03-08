import { AppPrincipal, Attribution, MeldReadState, MeldTransportSecurity } from '../api';
import { propertyValue } from '../index';
import { MeldMessageType } from '../ns/m-ld';
import { getIdLogger, MsgPack } from '../engine/util';
import { Logger } from 'loglevel';
import { M_LD } from '../ns';
import { getRandomValues, subtle } from '../engine/local';
import { MeldError } from '../engine/MeldError';
import { MeldConfig } from '../config';
import { Iri } from 'jsonld/jsonld-spec';

/** @internal */
const ALGO = {
  ENCRYPT: 'AES-CBC',
  SIGN: 'RSASSA-PKCS1-v1_5'
};

/**
 * An instance of this class must be included as the `transportSecurity` member
 * of the `MeldApp` for an access-controlled domain. This is because transport
 * security must be available _before_ the clone can connect to the domain.
 *
 * @category Experimental
 * @experimental
 */
export class MeldAclTransportSecurity implements MeldTransportSecurity {
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

  protected async getPublicKey(principalId: Iri, state: MeldReadState) {
    const principal = await state.get(principalId, M_LD.publicKey);
    if (principal == null)
      throw new MeldError('Request rejected', 'Signature principal unavailable');
    const rawKey = propertyValue(principal, M_LD.publicKey, Uint8Array);
    return subtle.importKey('spki', rawKey,
      { name: ALGO.SIGN, hash: 'SHA-256' }, false, ['verify']);
  }
}