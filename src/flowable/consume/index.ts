import { Observable } from 'rxjs';
import { Consumable } from '../index';
import { consume as consumeReadable, MinimalReadable } from './readable';
import { consume as consumeObservable } from './observable';
import { consume as consumeIterable } from './iterable';
import { consume as consumePromise } from './promise';

/**
 * Convert a variety of 'pull'-based constructs to a consumable
 */
export function consume<T>(source:
  Iterable<T> | MinimalReadable<T> | Promise<T> | Observable<T>): Consumable<T> {
  const src = <any>source;
  if (typeof src[Symbol.iterator] == 'function')
    return consumeIterable(src);
  else if (typeof src['then'] == 'function')
    return consumePromise(src);
  else if (typeof src['read'] == 'function')
    return consumeReadable(src);
  else if (typeof src['subscribe'] == 'function')
    return consumeObservable(src);
  else
    throw new Error('Unrecognised consumable type');
}

