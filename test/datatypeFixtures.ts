import { Datatype, Expression, SharedDatatype, shortId } from '../src';

export const binaryDatatype: Datatype<Buffer> = {
  '@id': 'http://ex.org/#Binary',
  validate: (data: string) => new Buffer(data),
  getDataId: data => shortId(data.toString())
};

export const dateDatatype: Datatype<Date> = {
  '@id': 'http://ex.org/date',
  validate: (value: string) => new Date(value),
  getDataId: date => date.toDateString(),
  toJSON: date => ({
    year: date.getFullYear(), month: date.getMonth() + 1, date: date.getDate()
  }),
  fromJSON: json => new Date(`${json.year}-${json.month}-${json.date}`)
};

export class CounterType implements SharedDatatype<number, string> {
  counterSeq = 1;
  '@id' = 'http://ex.org/#Counter';
  validate = Number;

  getDataId() { return `counter${this.counterSeq++}`; }

  update(data: number, update: Expression): [number, string] {
    const inc = (<any>update)['@plus'];
    return [data + inc, `+${inc}`];
  }

  apply(data: number, operation: string): [number, Expression] {
    const inc = Number(/\+(\d+)/.exec(operation)![1]);
    return [data + inc, { '@plus': inc }];
  }

  revert(data: number, operation: string, revert: null): [number, Expression] {
    const inc = Number(/\+(\d+)/.exec(operation)![1]);
    return [data - inc, { '@plus': -inc }];
  }
}
