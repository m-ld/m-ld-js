declare module 'jsonld/lib/url' {

  /**
   * Returns true if the given value is an absolute IRI or blank node IRI, false
   * if not.
   * Note: This weak check only checks for a correct starting scheme.
   *
   * @param v the value to check.
   *
   * @return true if the value is an absolute IRI, false if not.
   */
  function isAbsolute(v: string): boolean;
}
