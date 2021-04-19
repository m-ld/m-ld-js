export const $base = 'http://m-ld.org/';

/** For serialisation of transaction IDs in delta messages */
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
