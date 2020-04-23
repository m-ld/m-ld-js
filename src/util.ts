import { Quad } from 'rdf-js';
import { fromRDF } from 'jsonld';
import { concat, Observable, OperatorFunction, Subscription } from "rxjs";
import { publish } from "rxjs/operators";

export function flatten<T>(bumpy: T[][]): T[] {
  return ([] as T[]).concat(...bumpy);
}

export function toArray<T>(value?: T | T[]): T[] {
  return value == null ? [] : ([] as T[]).concat(value).filter(v => v != null);
}

export function jsonFrom(payload: Buffer): any {
  return JSON.parse(payload.toString());
}

export function rdfToJson(quads: Quad[]): Promise<any> {
  // Using native types to avoid unexpected value objects
  return fromRDF(quads, { useNativeTypes: true });
}

export class Future<T> implements PromiseLike<T> {
  private resolver: (t: T) => void;
  private rejecter: (err?: any) => void;
  private finalised: boolean = false;
  readonly promise: Promise<T>;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolver = (value?: T | PromiseLike<T> | undefined) => {
        if (!this.finalised) {
          this.finalised = true;
          resolve(value);
        }
      };
      this.rejecter = reason => {
        if (!this.finalised) {
          this.finalised = true;
          reject(reason);
        }
      };
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

export function shortId(len: number = 8) {
  var d = new Date().getTime();
  return ('a' + 'x'.repeat(len - 1)).replace(/[ax]/g, function (c) {
    return ((d + Math.random() * 16) % (c == 'a' ? 6 : 16) + (c == 'a' ? 10 : 0) | 0).toString(16);
  });
}

/**
 * Delays notifications from a source until a signal is received from a notifier.
 * @see https://ncjamieson.com/how-to-write-delayuntil/
 */
export function delayUntil<T>(
  notifier: Observable<any>
): OperatorFunction<T, T> {
  return source =>
    source.pipe(
      publish(published => {
        const delayed = new Observable<T>(subscriber => {
          let buffering = true;
          const buffer: T[] = [];
          const subscription = new Subscription();
          subscription.add(
            notifier.subscribe(
              () => {
                buffer.forEach(value => subscriber.next(value));
                subscriber.complete();
              },
              error => subscriber.error(error),
              () => {
                buffering = false;
                buffer.length = 0;
              }
            )
          );
          subscription.add(
            published.subscribe(
              value => buffering && buffer.push(value),
              error => subscriber.error(error)
            )
          );
          subscription.add(() => {
            buffer.length = 0;
          });
          return subscription;
        });
        return concat(delayed, published);
      })
    );
}