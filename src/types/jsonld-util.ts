/**
 * This declaration file is stored as .ts so that it is emitted
 */

declare module '@m-ld/jsonld/lib/util' {
  /**
   * Clones an object, array, Map, Set, or string/number. If a typed JavaScript
   * object is given, such as a Date, it will be converted to a string.
   *
   * @param value the value to clone.
   *
   * @return the cloned value.
   */
  function clone<T>(value: T): T;

  /**
   * Returns true if the given subject has the given property.
   *
   * @param subject the subject to check.
   * @param property the property to look for.
   *
   * @return true if the subject has the given property, false if not.
   */
  function hasProperty(subject: object, property: string): boolean;

  /**
   * Determines if the given value is a property of the given subject.
   *
   * @param subject the subject to check.
   * @param property the property to check.
   * @param value the value to check.
   *
   * @return true if the value exists, false if not.
   */
  function hasValue(subject: object, property: string, value: any): boolean;

  /**
   * Gets all of the values for a subject's property as an array.
   *
   * @param subject the subject.
   * @param property the property.
   *
   * @return all of the values for a subject's property as an array.
   */
  // DO NOT USE – ignores falsy values such as 0 and ''
  //function getValues(subject: object, property: string): Array<any>;

  /** Options for {@link addValue} */
  interface ValueOptions {
    /**
     * true if the property is always an array, false if not (default: false).
     */
    propertyIsArray?: boolean;
    /**
     * true if the value to be added should be preserved as an array (lists)
     * (default: false).
     */
    valueIsArray?: boolean;
    /**
     * true to allow duplicates, false not to (uses a simple shallow comparison
     * of subject ID or value) (default: true).
     */
    allowDuplicate?: boolean;
    /**
     * false to prepend value to any existing values. (default: false)
     */
    prependValue?: boolean;
  }

  /**
   * Adds a value to a subject. If the value is an array, all values in the
   * array will be added.
   *
   * @param subject the subject to add the value to.
   * @param property the property that relates the value to the subject.
   * @param value the value to add.
   * @param options the options to use
   */
  function addValue(subject: object, property: string, value: any, options?: ValueOptions): void;

  /**
   * Removes a value from a subject.
   *
   * @param subject the subject.
   * @param property the property that relates the value to the subject.
   * @param value the value to remove.
   * @param [options] the options to use:
   *          [propertyIsArray] true if the property is always an array, false
   *            if not (default: false).
   */
  function removeValue(subject: object, property: string, value: any,
    options?: Pick<ValueOptions, 'propertyIsArray'>): void;

  /**
  * Compares two JSON-LD values for equality. Two JSON-LD values will be
  * considered equal if:
  *
  * 1. They are both primitives of the same type and value.
  * 2. They are both @values with the same @value, @type, @language,
  *   and @index, OR
  * 3. They both have @ids they are the same.
  *
  * @param v1 the first value.
  * @param v2 the second value.
  *
  * @return true if v1 and v2 are considered equal, false if not.
  */
  function compareValues(v1: any, v2: any): boolean;
}
