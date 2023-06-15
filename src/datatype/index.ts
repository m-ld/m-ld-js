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
 */
export const jsonDatatype = new class implements Datatype, MeldPlugin {
  '@id' = RDF.JSON;

  /**
   * Approximate sizes of JSON objects, populated as possible using
   * JSON.stringify when data transitions through this datatype. Assumes largely
   * immutable objects.
   */
  _sizeCache = new WeakMap<object, number>();

  indirectedData(property: Iri, datatype: Iri): Datatype | undefined {
    if (datatype === RDF.JSON)
      return this;
  }

  validate(value: Value) {
    if (isValueObject(value) && value['@type'] === RDF.JSON)
      return JSON.parse(this.stringify(value['@value']));
  }

  stringify(data: unknown) {
    const asString = JSON.stringify(data);
    if (data != null && typeof data == 'object')
      this._sizeCache.set(data, asString.length * 2); // UTF-16
    return asString;
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
    return binaryId(this.stringify(data));
  }

  sizeOf(data: unknown): number {
   // No sizeof operator in Javascript
    return (data != null && typeof data == 'object' && this._sizeCache.get(data))
      || this.stringify(data).length * 2; // UTF-16
  }
}();

/**
 * Datatype for Uint8Array/Buffer values.
 */
export const byteArrayDatatype: Datatype<Buffer> & MeldPlugin = {
  indirectedData: (_, type) => {
    if (type === XS.base64Binary)
      return byteArrayDatatype;
  },
  '@id': XS.base64Binary,
  validate(value) {
    if (isValueObject(value) && value['@type'] === XS.base64Binary)
      value = value['@value'];
    return Buffer.from(<any>value); // Will throw if unacceptable
  },
  getDataId: binaryId,
  sizeOf: data => data.length
};

function binaryId(data: BinaryLike) {
  return sha1().update(data).digest().toString('base64');
}
