import { IndirectedDatatype, MeldPlugin } from '../api';
import { RDF, XS } from '../ns';
import { sha1 } from '../engine/local';
import { isValueObject } from '../jrql-support';
import type { BinaryLike } from 'crypto';

/**
 * This module contains basic datatypes in common use in m-ld.
 * Extension modules may also declare custom datatypes.
 * @module datatypes
 */

/**
 * JSON datatype for atomic JSON values.
 */
export const jsonDatatype: IndirectedDatatype & MeldPlugin = {
  indirectedData: (_, type) => {
    if (type === RDF.JSON)
      return jsonDatatype;
  },
  '@id': RDF.JSON,
  validate(value) {
    if (isValueObject(value) && value['@type'] === RDF.JSON)
      return JSON.parse(JSON.stringify(value['@value']));
  },
  toValue: json => ({ '@type': RDF.JSON, '@value': json }),
  /**
   * Hashing the stringified JSON can lead to different hashes for the same
   * logical content – this can sometimes lead to false negatives when
   * comparing. We accept this over expensive canonicalisation.
   */
  getDataId: data => binaryId(JSON.stringify(data))
};

/**
 * Datatype for Uint8Array/Buffer values.
 */
export const byteArrayDatatype: IndirectedDatatype<Buffer> & MeldPlugin = {
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
  getDataId: binaryId
};

function binaryId(data: BinaryLike) {
  return sha1().update(data).digest().toString('base64');
}
