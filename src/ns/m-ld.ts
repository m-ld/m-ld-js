export const $base = 'http://m-ld.org/';
export const $vocab = `${$base}#`;

/** Property for serialisation of transaction IDs in operation messages */
export const tid = `${$vocab}tid`;

/** Property for serialisation of datatype operations in operation messages */
export const op = `${$vocab}op`;

/** Property for secret known only to domain users */
export const secret = `${$vocab}secret`;

/** Class of encrypted operation envelope */
export const Encrypted = `${$vocab}Encrypted`;

/** Security Principal class */
export const Principal = `${$vocab}Principal`;

/** The local engine as a principal, used for e.g. operation voiding */
export const localEngine = `${$base}principal/local-engine`;

/** Property for asymmetric public key */
export const publicKey = `${$vocab}public-key`;

/**
 * Dual-use identifier:
 * - Property for Principal to have authority over a sh:Shape
 * - Singleton agreement condition
 */
export const hasAuthority = `${$vocab}has-authority`;

/** Statute class */
export const Statute = `${$vocab}Statute`;

/** A reference to a SHACL shape whose target is statutory */
export const statutoryShape = `${$vocab}statutory-shape`;

/** Class to access-control a set of sh:Shapes */
export const WritePermission = `${$vocab}WritePermission`;

/** A reference to a SHACL shape whose targets are controlled */
export const controlledShape = `${$vocab}controlled-shape`;

/** Principal to Permission link */
export const hasPermission = `${$vocab}has-permission`;

/** A reference to a sufficient condition for an agreement */
export const sufficientCondition = `${$vocab}sufficient-condition`;

/** Singleton list of m-ld extension declarations */
export const extensions = `${$base}extensions`;

export namespace EXT {
  /**
   * The namespace of **m-ld**-owned extensions, typically sub-namespaced e.g.
   * 'security/ACL'
   */
  export const $base = 'http://ext.m-ld.org/';
}

export namespace JS {
  export const $base = 'http://js.m-ld.org/';
  export const $vocab = `${$base}#`;

  /** CommonJS module class */
  export const commonJsExport = `${$vocab}CommonJSExport`;

  /** Property for CommonJS module require id */
  export const require = `${$vocab}require`;

  /** Property for Javascript class name (in module scope) */
  export const className = `${$vocab}class`;
}

/**
 * Class of LSEQ-like, default implementation of `@list`.
 * TODO: Change to @vocab on next major release
 * (left on base to avoid breaking non-experimental list data)
 */
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