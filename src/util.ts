import { generate } from 'short-uuid';

/**
 * Utility to normalise a property value according to **m-ld**
 * [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics), from a missing
 * value (`null` or `undefined`), a single value, or an array of values, to an
 * array of values (empty for missing values). This can simplify processing of
 * property values in common cases.
 * @param value the value to normalise to an array
 */
export function array<T>(value?: T | T[] | null): T[] {
  return value == null ? [] : ([] as T[]).concat(value).filter(v => v != null);
}

/**
 * Utility to generate a short Id according to the given spec.
 * @param spec If a number, a random Id will be generated with the given length.
 * If a string, a stable obfuscated Id will be generated for the string with a
 * fast hash.
 * @return a string identifier that is safe to use as an HTML (& XML) element Id
 */
export function shortId(spec: number | string = 8) {
  if (typeof spec == 'number') {
    let d = new Date().getTime();
    return ('a' + 'x'.repeat(spec - 1)).replace(/[ax]/g, c =>
      ((d + Math.random() * 16) % (c == 'a' ? 6 : 16) + (c == 'a' ? 10 : 0) | 0).toString(16));
  } else {
    let hashCode = Math.abs(Array.from(spec).reduce((hash, char) => {
      hash = ((hash << 5) - hash) + char.charCodeAt(0);
      return hash & hash;
    }, 0)).toString(16);
    if (hashCode.charAt(0) <= '9') // Ensure first char is alpha (a-j)
      hashCode = String.fromCharCode(hashCode.charCodeAt(0) + 49) + hashCode.slice(1);
    return hashCode;
  }
}

/**
 * Utility to generate a unique UUID for use in a MeldConfig
 */
export function uuid() {
  // This is indirected for documentation (do not just re-export generate)
  return generate();
}