import { Context } from '../../jrql-support';
import { Iri } from 'jsonld/jsonld-spec';
import { Triple, tripleKey } from '../quads';
import { sha1Digest } from '../util';
import { QS } from '../../ns';

/**
 * Context for SU-Set Dataset code to manipulate control content.
 */
export const SUSET_CONTEXT: Context = {
  tid: QS.tid, // Property of triple hash
  thash: QS.thash // Namespace for triple hashes
}

export function toPrefixedId(prefix: string, ...path: string[]): Iri {
  return `${prefix}:${path.map(encodeURIComponent).join('/')}`;
}

export function tripleId(triple: Triple): string {
  return toPrefixedId('thash', sha1Digest(tripleKey(triple)));
}
