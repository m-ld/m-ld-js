import { IndexMap, IndexSet } from '../src/engine/indices';

describe('Index set', () => {
  class DateSet extends IndexSet<Date> {
    protected construct(dates?: Iterable<Date>): IndexSet<Date> {
      return new DateSet(dates);
    }
    protected getIndex(date: Date): string {
      return date.getTime().toString();
    }
  }

  test('unit operations', () => {
    const dates = new DateSet();
    expect(dates.add(new Date(2017, 11, 30))).toBe(true);
    expect(dates.has(new Date(2017, 11, 30))).toBe(true);
    expect(dates.has(new Date(2018, 0, 1))).toBe(false);
    expect(dates.delete(new Date(2017, 11, 30))).toBe(true);
  });

  test('from array', () => {
    const dates = new DateSet([new Date(2017, 11, 30), new Date(2018, 0, 1)]);
    expect(dates.has(new Date(2017, 11, 30))).toBe(true);
    expect(dates.has(new Date(2018, 0, 1))).toBe(true);
  });

  test('from set', () => {
    const dates = new DateSet([new Date(2017, 11, 30), new Date(2018, 0, 1)]);
    const dates2 = new DateSet(dates);
    expect(dates2.has(new Date(2017, 11, 30))).toBe(true);
    expect(dates2.has(new Date(2018, 0, 1))).toBe(true);
  });

  test('add all', () => {
    const dates = new DateSet();
    dates.addAll([new Date(2017, 11, 30), new Date(2018, 0, 1)]);
    expect(dates.has(new Date(2017, 11, 30))).toBe(true);
    expect(dates.has(new Date(2018, 0, 1))).toBe(true);
  });

  test('delete all array', () => {
    const dates = new DateSet();
    dates.addAll([new Date(2017, 11, 30), new Date(2018, 0, 1), new Date(2018, 0, 2)]);
    dates.deleteAll([new Date(2017, 11, 30), new Date(2018, 0, 1)]);
    expect(dates.has(new Date(2017, 11, 30))).toBe(false);
    expect(dates.has(new Date(2018, 0, 1))).toBe(false);
    expect(dates.has(new Date(2018, 0, 2))).toBe(true);
  });

  test('delete all filter', () => {
    const dates = new DateSet();
    dates.addAll([new Date(2017, 11, 30), new Date(2018, 0, 1), new Date(2018, 0, 2)]);
    dates.deleteAll(date => date.getTime() < new Date(2018, 0, 1).getTime());
    expect(dates.has(new Date(2017, 11, 30))).toBe(false);
    expect(dates.has(new Date(2018, 0, 1))).toBe(true);
    expect(dates.has(new Date(2018, 0, 2))).toBe(true);
  });

  test('iterate', () => {
    const dates = new DateSet([new Date(2017, 11, 30), new Date(2018, 0, 1)]);
    const times = [...dates].map(date => date.getTime());
    expect(times.length).toBe(2);
    expect(times.includes(new Date(2017, 11, 30).getTime())).toBe(true);
    expect(times.includes(new Date(2018, 0, 1).getTime())).toBe(true);
  });
});

describe('Index map', () => {
  class DateMap<T> extends IndexMap<Date, T> {
    protected getIndex(date: Date): string {
      return date.getTime().toString();
    }
  }

  test('unit operations', () => {
    const dates = new DateMap<string>();
    expect(dates.set(new Date(2017, 11, 30), '1')).toBe(null);
    expect(dates.get(new Date(2017, 11, 30))).toBe('1');
    expect(dates.get(new Date(2018, 0, 1))).toBe(null);
    expect(dates.delete(new Date(2017, 11, 30))).toBe('1');
    expect(dates.get(new Date(2017, 11, 30))).toBe(null);
  });

  test('from array', () => {
    const dates = new DateMap<string>([
      [new Date(2017, 11, 30), '1'],
      [new Date(2018, 0, 1), '2']
    ]);
    expect(dates.get(new Date(2017, 11, 30))).toBe('1');
    expect(dates.get(new Date(2018, 0, 1))).toBe('2');
  });

  test('from map', () => {
    const dates = new DateMap<string>([
      [new Date(2017, 11, 30), '1'],
      [new Date(2018, 0, 1), '2']
    ]);
    expect(dates.get(new Date(2017, 11, 30))).toBe('1');
    expect(dates.get(new Date(2018, 0, 1))).toBe('2');
  });

  test('set all', () => {
    const dates = new DateMap<string>();
    dates.setAll([
      [new Date(2017, 11, 30), '1'],
      [new Date(2018, 0, 1), '2']
    ]);
    expect(dates.get(new Date(2017, 11, 30))).toBe('1');
    expect(dates.get(new Date(2018, 0, 1))).toBe('2');
  });

  test('re-set overrides key', () => {
    const dates = new DateMap<string>();
    const oldKey = new Date(2017, 11, 30);
    dates.set(oldKey, '1');
    const newKey = new Date(2017, 11, 30);
    dates.set(newKey, '2');
    expect(newKey).not.toBe(oldKey); // Confirming toBe behaviour!
    expect([...dates][0][0]).toBe(newKey);
  });

  test('with existing', () => {
    const dates = new DateMap<string>();
    dates.set(new Date(2017, 11, 30), '1');
    expect(dates.with(new Date(2017, 11, 30), () => '2')).toBe('1');
  });

  test('with generated', () => {
    const dates = new DateMap<string>();
    expect(dates.with(new Date(2017, 11, 30), () => '2')).toBe('2');
  });
});