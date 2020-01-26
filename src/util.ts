export function flatten<T>(bumpy: T[][]): T[] {
  return ([] as T[]).concat(...bumpy);
}

export function toArray<T>(value?: T | T[]): T[] {
  return value ? Array.isArray(value) ? value : [value] : [];
}
