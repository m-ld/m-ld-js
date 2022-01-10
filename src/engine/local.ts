import { Observable } from 'rxjs';

export namespace Idle {
  type Handle = number | NodeJS.Immediate;
  const DEFAULT_IDLE_TIME = 50.0;

  const root: any = typeof window === 'undefined' ? global || {} : window;

  export const requestCallback: (
    cb: (deadline: IdleDeadline) => void,
    opts?: IdleRequestOptions
  ) => Handle =
    root.requestIdleCallback?.bind(root) ?? ((cb: (deadline: IdleDeadline) => void) =>
      // Not supporting timeout parameter to callback request for immediate fallback
      setImmediate((startTime: number) => cb({
        timeRemaining: () => Math.max(0, DEFAULT_IDLE_TIME - (Date.now() - startTime)),
        didTimeout: false
      }), Date.now()));

  export const cancelCallback: (handle: Handle) => void =
    root.cancelIdleCallback?.bind(root) ?? ((handle: Handle) => {
      if (typeof handle != 'number')
        clearImmediate(handle);
    });
}

export const idling = (opts?: IdleRequestOptions) => new Observable<IdleDeadline>(subs => {
  const handle = Idle.requestCallback(deadline => {
    subs.next(deadline);
    subs.complete();
  }, opts);
  return () => Idle.cancelCallback(handle);
});

let webcrypto: Crypto;
if (typeof Crypto == 'function') {
  webcrypto = crypto;
} else {
  // The use of a variable is to prevent Browserify from bundling the module
  let cryptoModule = 'crypto';
  webcrypto = require(cryptoModule).webcrypto;
  if (webcrypto == null) {
    // Fallback to polyfill peer dependency for Node v14 or lower
    cryptoModule = '@peculiar/webcrypto';
    webcrypto = new (require(cryptoModule).Crypto)();
  }
}
if (webcrypto == null)
  console.warn(`No Web Crypto implementation available. Please use a polyfill,
  or add @peculiar/webcrypto as a peer dependency in NodeJS 14 or lower`);

type CryptoType = { subtle: SubtleCrypto, getRandomValues: Crypto['getRandomValues'] };
export const { subtle, getRandomValues } = webcrypto as CryptoType;