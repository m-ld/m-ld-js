import { firstValueFrom } from 'rxjs';
import { toArray } from 'rxjs/operators';
import { Consumable, flowable } from './index';

export function drain<T>(c: Consumable<T>): Promise<T[]> {
  return firstValueFrom(flowable(c).pipe(toArray()));
}