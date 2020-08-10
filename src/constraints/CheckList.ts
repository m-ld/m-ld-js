import { MeldConstraint, MeldUpdate, MeldReader } from '../m-ld';
import { Update, DeleteInsert, Subject } from '..';
import { toArray as array } from '../util';

export class CheckList implements MeldConstraint {
  constructor(
    readonly list: MeldConstraint[]) {
  }

  check(update: MeldUpdate, read: MeldReader): Promise<unknown> {
    return Promise.all(this.list.map(constraint => constraint.check(update, read)));
  }

  apply(update: MeldUpdate, read: MeldReader): Promise<Update | null> {
    return this.list.reduce<Promise<DeleteInsert<Subject[]> | null>>(async (acc, constraint) => {
      const soFar = await acc, inserts = soFar?.['@insert'] ?? [], deletes = soFar?.['@delete'] ?? [];
      const result = await constraint.apply({
        // Construct an accumulated update
        '@ticks': update['@ticks'],
        '@insert': update['@insert'].concat(inserts),
        '@delete': update['@delete'].concat(deletes)
      }, read);
      return result != null ? {
        '@insert': inserts.concat(result['@insert'] ?? []),
        '@delete': deletes.concat(result['@delete'] ?? [])
      } : soFar;
    }, Promise.resolve(null));
  }
}