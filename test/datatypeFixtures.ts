import { Expression, IndirectedDatatype, MeldPlugin, SharedDatatype } from '../src';
import { XS } from '../src/ns';
import { Iri } from '@m-ld/jsonld';

export const dateDatatype: IndirectedDatatype<Date> = {
  '@id': 'http://ex.org/date',
  validate: (value: string) => new Date(value),
  getDataId: date => date.toDateString(),
  toValue: date => date.toISOString(),
  toJSON: date => ({
    year: date.getFullYear(), month: date.getMonth() + 1, date: date.getDate()
  }),
  fromJSON: json => new Date(`${json.year}-${json.month}-${json.date}`)
};

export class CounterType implements SharedDatatype<number, string>, MeldPlugin {
  counterSeq = 1;
  '@id' = 'http://ex.org/#Counter';
  validate = Number;

  constructor(readonly property: string) {}

  // noinspection JSUnusedGlobalSymbols - IDE snafu
  /** @implements MeldExtensions#indirectedData */
  indirectedData(property: string, datatype: Iri) {
    if (property === this.property || datatype === this['@id'])
      return this;
  }

  toValue(data: number) {
    return { '@type': XS.integer, '@value': data };
  }

  getDataId() { return `counter${this.counterSeq++}`; }

  update(data: number, update: Expression): [number, string] {
    const inc = (<any>update)['@plus'];
    return [data + inc, `+${inc}`];
  }

  apply(data: number, operation: string): [number, Expression] {
    // noinspection SuspiciousTypeOfGuard: for testing
    if (typeof operation != 'string')
      throw new RangeError();
    const inc = Number(/\+(\d+)/.exec(operation)![1]);
    return [data + inc, { '@plus': inc }];
  }

  revert(data: number, operation: string, revert: null): [number, Expression] {
    const inc = Number(/\+(\d+)/.exec(operation)![1]);
    return [data - inc, { '@plus': -inc }];
  }
}
