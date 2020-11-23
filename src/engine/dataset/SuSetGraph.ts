import { Context } from '../../jrql-support';
import { namedNode } from '@rdfjs/data-model';
import { NamedNode } from 'rdf-js';
import { Iri } from 'jsonld/jsonld-spec';
import { Triple, tripleKey } from '../quads';
import { createHash } from 'crypto';

/**
 * Context for SU-Set Dataset code to manipulate control content.
 */
export const SUSET_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/',
  tid: 'qs:#tid', // Property of journal entry AND triple hash
  body: 'qs:#body', // Journal and journal entry body
  thash: 'qs:thash/', // Namespace for triple hashes
  tail: { '@id': 'qs:#tail', '@type': '@id' }, // Property of the journal
  lastDelivered: { '@id': 'qs:#lastDelivered', '@type': '@id' }, // Property of the journal
  entry: 'qs:journal/entry/', // Namespace for journal entries
  hash: 'qs:#hash', // Property of a journal entry
  delta: 'qs:#delta', // Property of a journal entry
  remote: 'qs:#remote', // Property of a journal entry
  time: 'qs:#time', // Property of journal AND a journal entry
  ticks: 'qs:#ticks', // Property of a journal entry
  next: { '@id': 'qs:#next', '@type': '@id' } // Property of a journal entry
}

export function qsName(name: string): NamedNode {
  return namedNode(SUSET_CONTEXT.qs + name);
}

export function toPrefixedId(prefix: string, ...path: string[]): Iri {
  return `${prefix}:${path.map(encodeURIComponent).join('/')}`;
}

export function fromPrefixedId(prefix: string, id: Iri): string[] {
  return id.match(`^${prefix}:(.+)`)?.[1]?.split('/').map(decodeURIComponent) ?? [];
}

export function tripleId(triple: Triple): string {
  const hash = createHash('sha1'); // Fastest
  tripleKey(triple).forEach(key => hash.update(key));
  return toPrefixedId('thash', hash.digest('base64'));
}
