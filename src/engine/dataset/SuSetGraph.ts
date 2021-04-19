import { Context } from '../../jrql-support';
import { Iri } from 'jsonld/jsonld-spec';
import { Triple, tripleKey } from '../quads';
import { createHash } from 'crypto';
import { TreeClock } from '../clocks';
import { MsgPack } from '../util';
import { qs } from '../../ns';

/**
 * Context for SU-Set Dataset code to manipulate control content.
 */
export const SUSET_CONTEXT: Context = {
  tid: qs.tid, // Property of triple hash
  thash: qs.thash // Namespace for triple hashes
}

export function toPrefixedId(prefix: string, ...path: string[]): Iri {
  return `${prefix}:${path.map(encodeURIComponent).join('/')}`;
}

export function tripleId(triple: Triple): string {
  const hash = createHash('sha1'); // Fastest
  tripleKey(triple).forEach(key => hash.update(key));
  return toPrefixedId('thash', hash.digest('base64'));
}

export function txnId(time: TreeClock): string {
  return createHash('sha1')
    .update(MsgPack.encode(time.toJson()))
    .digest('base64');
}
