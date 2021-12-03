import { BehaviorSubject, concatAll, Observable } from 'rxjs';
import { map, share } from 'rxjs/operators';
import { Bite, isFlowable } from '../index';

export function consume<T>(source: Observable<T>) {
  return isFlowable<T>(source) ? source.consume : bufferObservable(source);
}

function bufferObservable<T>(source: Observable<T>) {
  return source.pipe(share(), map(value => {
    const biteSubject = new BehaviorSubject<Bite<T>>({
      value, next: () => {
        biteSubject.complete();
        return true;
      }
    });
    return biteSubject;
  }), concatAll()); // This will buffer indefinitely
}
