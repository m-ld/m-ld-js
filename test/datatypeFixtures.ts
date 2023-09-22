import { Datatype, Expression, MeldPlugin, SharedDatatype } from '../src';
import { XS } from '../src/ns';
import { Iri, ValueObject } from '@m-ld/jsonld';

export const dateDatatype: Datatype<Date> = {
  '@id': 'http://ex.org/date',
  validate: (value: ValueObject) => new Date(<string>value['@value']),
  getDataId: date => date.toDateString(),
  toValue: date => date.toISOString(),
  sizeOf: date => JSON.stringify(date).length,
  toJSON: date => ({
    year: date.getFullYear(), month: date.getMonth() + 1, date: date.getDate()
  }),
  fromJSON: json => new Date(`${json.year}-${json.month}-${json.date}`)
};

export class CounterType implements SharedDatatype<number, number>, MeldPlugin {
  counterSeq = 1;
  '@id' = 'http://ex.org/#Counter';
  validate = Number;

  constructor(readonly property: string) {}

  // noinspection JSUnusedGlobalSymbols - IDE snafu
  /** @implements MeldPlugin#indirectedData */
  indirectedData(datatype: Iri, property: string) {
    if (property === this.property || datatype === this['@id'])
      return this;
  }

  getDataId() { return `counter${this.counterSeq++}`; }

  toValue(value: number) {
    return { '@type': XS.integer, '@value': value };
  }

  sizeOf(_value: number): number {
    return 8; // Javascript number
  }

  update(value: number, update: Expression): [number, number] {
    const inc = (<any>update)['@plus'];
    return [value + inc, inc];
  }

  apply(value: number, reversions: [number][], inc: number = 0): [number, Expression] {
    // noinspection SuspiciousTypeOfGuard: for testing
    if (typeof inc != 'number')
      throw new RangeError();
    // Reversions become subtractions
    inc -= reversions.reduce((sum, [inc]) => sum + inc, 0);
    return [value + inc, { '@plus': inc }];
  }

  fuse([inc1]: [number], [inc2]: [number]): [number] {
    return [inc1 + inc2];
  }

  cut(prefix: number, inc: number): number {
    return inc - prefix;
  }
}
