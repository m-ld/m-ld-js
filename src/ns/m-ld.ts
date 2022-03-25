export const $base = 'http://m-ld.org/';

/** Property for serialisation of transaction IDs in operation messages */
export const tid = `${$base}#tid`;

/** Property for secret known only to domain users */
export const secret = `${$base}#secret`;

/** Class of encrypted operation envelope */
export const Encrypted = `${$base}Encrypted`;

/** Security Principal class */
export const Principal = `${$base}Principal`;

/** The local engine as a principal, used for e.g. operation voiding */
export const localEngine = `${$base}principal/local-engine`;

/** Property for asymmetric public key */
export const publicKey = `${$base}#public-key`;

/**
 * Dual-use identifier:
 * - Property for Principal to have authority over a sh:Shape
 * - Singleton agreement condition
 */
export const hasAuthority = `${$base}#has-authority`;

/** Statute class */
export const Statute = `${$base}Statute`;

/** A reference to a SHACL shape whose target is statutory */
export const statutoryShape = `${$base}#statutory-shape`;

/** Access-controls a set of sh:Shapes */
export const WritePermission = `${$base}WritePermission`;

/** A reference to a SHACL shape whose targets are permissioned */
export const controlledShape = `${$base}#controlled-shape`;

/** Principal to Permission link */
export const hasPermission = `${$base}#has-permission`;

/** A reference to a sufficient condition for an agreement */
export const sufficientCondition = `${$base}#sufficient-condition`;

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

  /** CommonJS module class */
  export const commonJsModule = `${$base}CommonJSModule`;

  /** Property for CommonJS module require id */
  export const require = `${$base}#require`;

  /** Property for Javascript class name (in module scope) */
  export const className = `${$base}#class`;
}

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