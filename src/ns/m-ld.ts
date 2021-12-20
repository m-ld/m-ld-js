export const $base = 'http://m-ld.org/';

/** For serialisation of transaction IDs in operation messages */
export const tid = `${$base}#tid`; // TID property

export const rdflseq = `${$base}RdfLseq`;

const rdflseqPosIdPre = `${rdflseq}/?=`;

export function matchRdflseqPosId(predicate: string): string | undefined {
  if (predicate.startsWith(rdflseqPosIdPre))
    return predicate.slice(rdflseqPosIdPre.length);
}

export function rdflseqPosId(lseqPosId: string): string {
  return rdflseqPosIdPre + lseqPosId;
}

/**
 * `control` subdomain for Pubsub remotes
 * @see ../remotes/ControlMessage.ts
 */
export enum MeldMessageType {
/** The (usually implicit) RDFS Class of m-ld operation messages */
  operation = 'http://m-ld.org/operation',
  request = 'http://control.m-ld.org/request',
  response = 'http://control.m-ld.org/response'
}

export enum MeldRequestType {
  clock = 'http://control.m-ld.org/request/clock',
  snapshot = 'http://control.m-ld.org/request/snapshot',
  revup = 'http://control.m-ld.org/request/revup'
}

export enum MeldResponseType {
  clock = 'http://control.m-ld.org/response/clock',
  snapshot = 'http://control.m-ld.org/response/snapshot',
  revup = 'http://control.m-ld.org/response/revup',
  rejected = 'http://control.m-ld.org/response/rejected'
}