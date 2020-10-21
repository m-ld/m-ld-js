import { MeldConstraint, MeldUpdate, MeldReadState } from '..';
import { Update, Subject } from '../jrql-support';
import { DeleteInsert } from '..';

/** @internal */
export class CheckList implements MeldConstraint {
  constructor(
    readonly list: MeldConstraint[]) {
  }

  check(state: MeldReadState, update: MeldUpdate): Promise<unknown> {
    return Promise.all(this.list.map(constraint => constraint.check(state, update)));
  }

  apply(state: MeldReadState, update: MeldUpdate): Promise<Update | null> {
    return this.list.reduce<Promise<DeleteInsert<Subject[]> | null>>(async (acc, constraint) => {
      const soFar = await acc, inserts = soFar?.['@insert'] ?? [], deletes = soFar?.['@delete'] ?? [];
      const result = await constraint.apply(state, {
        // Construct an accumulated update
        '@ticks': update['@ticks'],
        '@insert': update['@insert'].concat(inserts),
        '@delete': update['@delete'].concat(deletes)
      });
      return result != null ? {
        '@insert': inserts.concat(result['@insert'] ?? []),
        '@delete': deletes.concat(result['@delete'] ?? [])
      } : soFar;
    }, Promise.resolve(null));
  }
}