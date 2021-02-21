declare module 'jsonld/lib/types' {
  /**
   * Returns true if the given value is an Array.
   *
   * @param v the value to check.
   *
   * @return true if the value is an Array, false if not.
   */
  function isArray(arg: any): arg is any[];

  /**
   * Returns true if the given value is a Boolean.
   *
   * @param v the value to check.
   *
   * @return true if the value is a Boolean, false if not.
   */
  function isBoolean(arg: any): arg is boolean | Boolean;

  /**
   * Returns true if the given value is a double.
   *
   * @param v the value to check.
   *
   * @return true if the value is a double, false if not.
   */
  function isDouble(arg: any): boolean;

  /**
   * Returns true if the given value is an empty Object.
   *
   * @param v the value to check.
   *
   * @return true if the value is an empty Object, false if not.
   */
  function isEmptyObject(arg: any): arg is {};

  /**
   * Returns true if the given value is a Number.
   *
   * @param v the value to check.
   *
   * @return true if the value is a Number, false if not.
   */
  function isNumber(arg: any): arg is number | Number;

  /**
   * Returns true if the given value is numeric.
   *
   * @param v the value to check.
   *
   * @return true if the value is numeric, false if not.
   */
  // Do not use – implementation is flawed
  //function isNumeric(arg: string): boolean;

  /**
   * Returns true if the given value is an Object.
   *
   * @param v the value to check.
   *
   * @return true if the value is an Object, false if not.
   */
  function isObject(arg: any): arg is object;

  /**
   * Returns true if the given value is a String.
   *
   * @param v the value to check.
   *
   * @return true if the value is a String, false if not.
   */
  function isString(arg: any): arg is string | String;

  /**
   * Returns true if the given value is undefined.
   *
   * @param v the value to check.
   *
   * @return true if the value is undefined, false if not.
   */
  function isUndefined(arg: any): arg is undefined;
}