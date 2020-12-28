import { MeldConstraint, MeldReadState, MutableMeldUpdate } from '..';

/** @internal */
export class CheckList implements MeldConstraint {
  constructor(
    readonly list: MeldConstraint[]) {
  }

  check(state: MeldReadState, update: MutableMeldUpdate) {
    return Promise.all(this.list.map(
      constraint => constraint.check(state, update)));
  }

  async apply(state: MeldReadState, update: MutableMeldUpdate) {
    for (let constraint of this.list)
      await constraint.apply(state, update);
  }
}