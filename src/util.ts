import cuid from 'cuid';

/**
 * Utility to normalise a property value according to **m-ld**
 * [data&nbsp;semantics](http://spec.m-ld.org/#data-semantics), from a missing
 * value (`null` or `undefined`), a single value, or an array of values, to an
 * array of values (empty for missing values). This can simplify processing of
 * property values in common cases.
 *
 * @param value the value to normalise to an array
 * @category Utility
 */
export function array<T>(value?: T | T[] | null): NonNullable<T>[] {
  return value == null ? [] : ([] as T[])
    .concat(value).filter((v: T): v is NonNullable<T> => v != null);
}

/**
 * Utility to generate a short Id according to the given spec.
 *
 * @param spec If provided, a stable obfuscated Id will be generated for the
 * string with a fast hash.
 * @return a string identifier that is safe to use as an HTML (& XML) element Id
 * @category Utility
 */
export function shortId(spec?: string) {
  if (spec == null) {
    // Slug is not guaranteed to start with a letter
    return 's' + cuid.slug()
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
 * Utility to generate a unique short UUID for use in a MeldConfig; actually
 * a CUID starting with the character `c` and containing only lowercase
 * US-English letters and digits. (Note that this is not an RFC 4122 UUID.)
 *
 * @category Utility
 * @see https://github.com/paralleldrive/cuid#original-documentation-follows
 */
export function uuid() {
  return cuid();
}