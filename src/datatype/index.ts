import { Datatype } from '../api';
import { RDF } from '../ns';
import { sha1 } from '../engine/local';

/**
 * This module contains basic datatypes in common use in m-ld.
 * Extension modules may also declare custom datatypes.
 * @module datatypes
 */

/**
 * JSON datatype for atomic JSON values. Note the behaviour does not match the
 * JSON-LD specification (see below), as the lexical value is hashed with SHA-1.
 * @see https://www.w3.org/TR/json-ld/#the-rdf-json-datatype
 */
export const jsonDatatype: Datatype = {
  '@id': RDF.JSON,
  validate: value => JSON.parse(JSON.stringify(value)),
  /**
   * Hashing the stringified JSON can lead to different hashes for the same
   * logical content – this can sometimes lead to false negatives when
   * comparing. We accept this over expensive canonicalisation.
   */
  getDataId: data => sha1()
    .update(JSON.stringify(data))
    .digest()
    .toString('base64')
};