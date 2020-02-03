export function flatten<T>(bumpy: T[][]): T[] {
  return ([] as T[]).concat(...bumpy);
}

export function toArray<T>(value?: T | T[]): T[] {
  return value ? Array.isArray(value) ? value : [value] : [];
}

export function jsonFrom(payload: Buffer): any {
  return JSON.parse(payload.toString());
}

export class Future<T> implements PromiseLike<T> {
  private resolver: (t: T) => void;
  private rejecter: (err: any) => void;
  readonly promise: Promise<T>;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolver = resolve;
      this.rejecter = reject;
    });
  }

  get resolve() {
    return this.resolver;
  }

  get reject() {
    return this.rejecter;
  }

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null | undefined,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null | undefined):
    PromiseLike<TResult1 | TResult2> {
    return this.promise.then(onfulfilled, onrejected);
  }
}