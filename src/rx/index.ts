import { asapScheduler, Observable, observeOn } from 'rxjs';
import type { GraphSubject, Iri, MeldClone, MeldReadState, MeldUpdate } from '..';
import { updateSubject } from '..';
import { enablePatches, produceWithPatches } from 'immer';
import { takeUntilComplete } from '../engine/util';

enablePatches();

type Eventually<T> = T | undefined | Promise<T | undefined>;

/**
 * 'Watches' the given read/follow procedures on the given m-ld clone, by
 * creating an observable that emit when the initial read or update emit a
 * non-null value.
 * @param meld the clone to attach the observable to
 * @param readValue reads an initial value; or null if not available
 * @param updateValue provides an updated value, or null if not available. If
 * this param is omitted, `readValue` is called for every update.
 * @category Reactive
 */
export function watchQuery<T>(
  meld: MeldClone,
  readValue: (state: MeldReadState) => Eventually<T>,
  updateValue: (update: MeldUpdate, state: MeldReadState) => Eventually<T> =
    (_, state) => readValue(state)
): Observable<T> {
  return new Observable<T>(subs => {
    subs.add(meld.read(
      async state => {
        try {
          const value = await readValue(state);
          !subs.closed && value != null && subs.next(value);
        } catch (e) {
          subs.error(e);
        }
      },
      async (update, state) => {
        try {
          const value = await updateValue(update, state);
          !subs.closed && value != null && subs.next(value);
        } catch (e) {
          subs.error(e);
        }
      }
    ));
  }).pipe(
    takeUntilComplete(meld.status),
    // TODO: workaround: live lock throws due to overlapping states
    observeOn(asapScheduler)
  );
}

/**
 * Shorthand for following the state of a specific subject in the m-ld clone.
 * Will emit an initial state as soon as the subject exists, and every time the
 * subject changes.
 * @param meld the clone to attach the observable to
 * @param id the subject identity IRI
 * @category Reactive
 */
export function watchSubject(
  meld: MeldClone,
  id: Iri
): Observable<GraphSubject> {
  let subject: GraphSubject | undefined, patches = [];
  return watchQuery(
    meld,
    async state => {
      return subject = await state.get(id);
    },
    async (update, state) => {
      if (subject != null) {
        [subject, patches] = produceWithPatches(subject,
          // @ts-ignore: TS cannot cope with mutable GraphSubject
          mutable => updateSubject(mutable, update));
        return patches.length ? subject : undefined;
      } else {
        return subject = await state.get(id);
      }
    }
  );
}
