import { Context } from '../../jrql-support';
import { Iri } from 'jsonld/jsonld-spec';
import { Triple, tripleKey } from '../quads';
import { createHash } from 'crypto';
import { TreeClock } from '../clocks';
import { MsgPack } from '../util';
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
  return toPrefixedId('thash', fastDigest(...tripleKey(triple)));
}

export function txnId(time: TreeClock): string {
  return fastDigest(MsgPack.encode(time.toJson()));
}

function fastDigest(...items: (string | Buffer)[]) {
  const hash = createHash('sha1'); // Fastest
  for (let item of items)
    hash.update(item);
  return hash.digest('base64');
}