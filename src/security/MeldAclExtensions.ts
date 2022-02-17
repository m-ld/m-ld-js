import { MeldExtensions } from '../api';
import { M_LD, XS } from '../ns';
import { Iri } from 'jsonld/jsonld-spec';
import { Write } from '../jrql-support';

/**
 * [[include:ext/acl.md]]
 *
 * @category Experimental
 * @experimental
 */
export class MeldAclExtensions implements MeldExtensions {
  /**
   * Extension declaration. Insert into the domain data to install the
   * extension. For example (assuming a **m-ld** `clone` object):
   *
   * ```typescript
   * clone.write(MeldAclExtensions.declare(0, 'test.m-ld.org', randomBytes(16)));
   * ```
   *
   * @param priority the preferred index into the existing list of extensions
   * (lower value is higher priority).
   * @param domainName as declared in the `MeldConfig` of the clone
   * @param aesKey a raw AES key, e.g. `randomBytes(32)`
   */
  static declare = (priority: number, domainName: string, aesKey: Buffer): Write => ({
    '@graph': [{
      '@id': M_LD.extensions,
      '@list': {
        [priority]: {
          '@id': `${M_LD.EXT.$base}security/MeldAclExtensions`,
          '@type': M_LD.JS.commonJsModule,
          [M_LD.JS.require]: '@m-ld/m-ld/dist/security',
          [M_LD.JS.className]: 'MeldAclExtensions'
        }
      }
    }, {
      '@id': `http://${domainName}/`,
      [M_LD.secret]: {
        '@type': XS.base64Binary,
        '@value': `${aesKey.toString('base64')}`
      }
    }]
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
}