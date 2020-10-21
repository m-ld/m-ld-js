import { generate } from 'short-uuid';

/**
 * Utility to normalise a property value according to **m-ld**
 * [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics), from a missing
 * value (`null` or `undefined`), a single value, or an array of values, to an
 * array of values (empty for missing values). This can simplify processing of
 * property values in common cases.
 * @param value the value to normalise to an array
 */
export function array<T>(value?: T | T[]): T[] {
  return value == null ? [] : ([] as T[]).concat(value).filter(v => v != null);
}

/**
 * Utility to generate a short Id according to the given spec.
 * @param spec If a number, a random Id will be generated with the given length.
 * If a string, an obfuscated Id will be deterministically generated for the
 * string (passing the same spec again will generate the same Id).
 * @return a string identifier that is safe to use as an HTML (& XML) element Id
 */
export function shortId(spec: number | string = 8) {
  let genChar: () => number, len: number;
  if (typeof spec == 'number') {
    let d = new Date().getTime();
    genChar = () => (d + Math.random() * 16);
    len = spec;
  } else {
    let i = 0;
    genChar = () => spec.charCodeAt(i++);
    len = spec.length;
  }
  return ('a' + 'x'.repeat(len - 1)).replace(/[ax]/g, c =>
    (genChar() % (c == 'a' ? 6 : 16) + (c == 'a' ? 10 : 0) | 0).toString(16));
}

/**
 * Utility to generate a unique UUID for use in a MeldConfig
 */
export function uuid() {
  // This is indirected for documentation (do not just re-export generate)
  return generate();
}