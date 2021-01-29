import { MeldConstraint, MeldReadState, InterimUpdate } from '..';

/** @internal */
export class CheckList implements MeldConstraint {
  constructor(
    readonly list: MeldConstraint[]) {
  }

  async check(state: MeldReadState, update: InterimUpdate) {
    for (let constraint of this.list) {
      await update.ready;
      await constraint.check(state, update);
    }
  }

  async apply(state: MeldReadState, update: InterimUpdate) {
    for (let constraint of this.list) {
      await update.ready;
      await constraint.apply(state, update);
    }
  }
}