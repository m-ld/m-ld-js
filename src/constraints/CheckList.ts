import { MeldConstraint, MeldUpdate, MeldReadState, MutableMeldUpdate } from '..';
import { Update, Subject } from '../jrql-support';
import { DeleteInsert } from '..';
import { constraintFromConfig } from '.';

/** @internal */
export class CheckList implements MeldConstraint {
  constructor(
    readonly list: MeldConstraint[]) {
  }

  check(state: MeldReadState, update: MeldUpdate) {
    return Promise.all(this.list.map(
      constraint => constraint.check(state, update)));
  }

  async apply(state: MeldReadState, update: MutableMeldUpdate) {
    for (let constraint of this.list)
      await constraint.apply(state, update);
  }
}