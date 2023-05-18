import { Datatype, Expression, SharedDatatype } from '../src';

export class CounterType implements SharedDatatype<number, string> {
  counterSeq = 1;
  '@id' = 'http://ex.org/#Counter';
  validate = Number;

  toLexical() { return `counter${this.counterSeq++}`; }

  update(data: number, update: Expression): [number, string] {
    const inc = (<any>update)['@plus'];
    return [data + inc, `+${inc}`];
  }

  apply(data: number, operation: string): [number, Expression] {
    const inc = Number(/\+(\d+)/.exec(operation)![1]);
    return [data + inc, { '@plus': inc }];
  }
}

export const dateDatatype: Datatype<Date> = {
  '@id': 'http://ex.org/date',
  validate: value => new Date(value),
  toLexical: date => date.toDateString(),
  toJSON: date => ({
    year: date.getFullYear(), month: date.getMonth() + 1, date: date.getDate()
  }),
  fromJSON: json => new Date(`${json.year}-${json.month}-${json.date}`)
};