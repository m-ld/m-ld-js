declare module 'jsonld/lib/util' {
  /**
   * Clones an object, array, Map, Set, or string/number. If a typed JavaScript
   * object is given, such as a Date, it will be converted to a string.
   *
   * @param value the value to clone.
   *
   * @return the cloned value.
   */
  function clone<T extends {}>(value: T): T;
}
