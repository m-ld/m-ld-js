import { Datatype } from '../api';
import { RDF } from '../ns';
import { sha1 } from '../engine/local';

export const jsonDatatype: Datatype = {
  '@id': RDF.JSON,
  validate: value => JSON.parse(JSON.stringify(value)),
  /**
   * Hashing the stringified JSON can lead to different hashes for the same
   * logical content – this can sometimes lead to false negatives when
   * comparing. We accept this over expensive canonicalisation.
   */
  toLexical: data => sha1()
    .update(JSON.stringify(data))
    .digest()
    .toString('base64')
};