import { Datatype, MeldPlugin } from '../api';
import { RDF, XS } from '../ns';
import { sha1 } from '../engine/local';
import { isValueObject, Value } from '../jrql-support';
import type { BinaryLike } from 'crypto';
import { Iri } from '@m-ld/jsonld';

/**
 * This module contains basic datatypes in common use in m-ld.
 * Extension modules may also declare custom datatypes.
 * @module datatypes
 */

/**
 * JSON datatype for atomic JSON values.
 * @internal
 */
export const jsonDatatype = new class implements Datatype, MeldPlugin {
  '@id' = RDF.JSON;

  /**
   * Approximate sizes of JSON objects, populated as possible using
   * JSON.stringify when data transitions through this datatype. Assumes largely
   * immutable objects.
   */
  _sizeCache = new WeakMap<object, number>();

  indirectedData(datatype: Iri): Datatype | undefined {
    if (datatype === RDF.JSON)
      return this;
  }

  validate(value: Value) {
    if (isValueObject(value) && value['@type'] === RDF.JSON) {
      const json = JSON.parse(value['@value']);
      this.cacheLength(json, value['@value']);
      return json;
    }
    return JSON.parse(this.lexical(value));
  }

  lexical(data: unknown) {
    const asString = JSON.stringify(data);
    this.cacheLength(data, asString);
    return asString;
  }

  cacheLength(data: unknown, asString: string) {
    if (data != null && typeof data == 'object')
      this._sizeCache.set(data, asString.length * 2); // UTF-16
  }

  toValue(json: unknown) {
    return { '@type': RDF.JSON, '@value': json };
  }

  /**
   * Hashing the stringified JSON can lead to different hashes for the same
   * logical content – this can sometimes lead to false negatives when
   * comparing. We accept this over expensive canonicalisation.
   */
  getDataId(data: unknown) {
    return binaryId(this.lexical(data));
  }

  sizeOf(data: unknown): number {
    // No sizeof operator in Javascript
    return (data != null && typeof data == 'object' && this._sizeCache.get(data))
      || this.lexical(data).length * 2; // UTF-16
  }
}();

/**
 * Datatype for Uint8Array/Buffer values.
 * @internal
 */
export const byteArrayDatatype: Datatype<Buffer> & MeldPlugin = {
  indirectedData: type => {
    if (type === XS.base64Binary)
      return byteArrayDatatype;
  },
  '@id': XS.base64Binary,
  validate(value) {
    if (isValueObject(value) && value['@type'] === XS.base64Binary)
      return Buffer.from(value['@value'], 'base64');
    return Buffer.from(<any>value); // Will throw if unacceptable
  },
  toValue(data: Buffer): Value {
    // TODO: This is a hack for JSON APIs, e.g. in compliance tests
    data.toJSON = () => (<any>{
      '@type': XS.base64Binary,
      '@value': data.toString('base64')
    });
    return data;
  },
  getDataId: binaryId,
  sizeOf: data => data.length
};

/** @internal */
function binaryId(data: BinaryLike) {
  return sha1().update(data).digest().toString('base64');
}
