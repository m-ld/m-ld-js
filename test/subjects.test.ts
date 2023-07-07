import fc from 'fast-check';
import { List, SubjectPropertyValues, textDiff } from '../src';

describe('list diffs & updates', () => {
  test('diff from nothing is minimal', () => {
    const spv = SubjectPropertyValues.for({ '@list': [1, 2] }, '@list');
    expect(spv.diff([])).toEqual({ inserts: { '@list': { 0: [1, 2] } } });
  });

  test('diff retains id', () => {
    const spv = SubjectPropertyValues.for({ '@id': 'fred', '@list': [1, 2] }, '@list');
    expect(spv.diff([])).toEqual({
      inserts: { '@id': 'fred', '@list': { 0: [1, 2] } }
    });
  });

  test('diff append is minimal', () => {
    const spv = SubjectPropertyValues.for({ '@list': [0, 1, 2] }, '@list');
    expect(spv.diff([0])).toEqual({ inserts: { '@list': { 1: [1, 2] } } });
  });

  test('diff prepend is minimal', () => {
    const spv = SubjectPropertyValues.for({ '@list': [0, 1, 2] }, '@list');
    expect(spv.diff([1, 2])).toEqual({ inserts: { '@list': { 0: 0 } } });
  });

  test('diff insert is minimal', () => {
    const spv = SubjectPropertyValues.for({ '@list': [0, 1, 2] }, '@list');
    expect(spv.diff([0, 2])).toEqual({ inserts: { '@list': { 1: 1 } } });
  });

  test('diff delete is minimal', () => {
    const spv = SubjectPropertyValues.for({ '@list': [0] }, '@list');
    expect(spv.diff([0, 1, 2])).toEqual({ deletes: { '@list': { 1: 1, 2: 2 } } });
  });

  test('diff has old positions', () => {
    const spv = SubjectPropertyValues.for({
      '@list': [0, 1, 3, 4]
    }, '@list');
    expect(spv.diff(
      [1, 2, 4, 5]
    )).toEqual({
      deletes: { '@list': { 1: 2, 3: 5 } },
      inserts: { '@list': { 0: 0, 2: 3 } }
    });
  });

  test('diff minimises subjects', () => {
    const spv = SubjectPropertyValues.for({
      '@list': [{ '@id': 'fred', name: 'Fred' }]
    }, '@list');
    expect(spv.diff([])).toEqual({
      inserts: { '@list': { 0: { '@id': 'fred' } } }
    });
  });

  test('diff applies', () => {
    fc.assert(
      fc.property(
        fc.array(fc.nat({ max: 5 }), { maxLength: 4 }),
        fc.array(fc.nat({ max: 5 }), { maxLength: 4 }),
        (olds, news) => {
          const { deletes, inserts } = SubjectPropertyValues
            .for({ '@list': news }, '@list')
            .diff(olds);
          // New items must be expressed as slots
          slotify(<List>inserts);
          const regen = SubjectPropertyValues
            .for({ '@list': [...olds] }, '@list')
            .update(deletes, inserts).values;
          expect(regen).toEqual(news);
        }
      ),
      { ignoreEqualValues: true }
    );
  });
});

describe('text diffs', () => {
  test('diff empty to empty', () => {
    expect([...textDiff('', '')]).toEqual([]);
  });

  test('diff empty to chars', () => {
    expect([...textDiff('', 'foo')]).toEqual([[0, 0, 'foo']]);
  });

  test('diff chars to empty', () => {
    expect([...textDiff('foo', '')]).toEqual([[0, 3]]);
  });

  test('diff chars to same chars', () => {
    expect([...textDiff('foo', 'foo')]).toEqual([]);
  });

  test('diff update one char', () => {
    expect([...textDiff('f', 'b')]).toEqual([[0, 1, 'b']]);
  });

  test('diff update char in middle', () => {
    expect([...textDiff('foo', 'fuo')]).toEqual([[1, 1, 'u']]);
  });

  test('diff update separate chars', () => {
    expect([...textDiff('foo', 'bor')]).toEqual([[0, 1, 'b'], [2, 1, 'r']]);
  });
});

function slotify(inserts: List | undefined) {
  if (inserts != null) {
    for (let [index, values] of Object.entries(inserts['@list'])) {
      inserts['@list'][Number(index)] = Array.isArray(values) ?
        values.map(value => ({ '@id': `${value}`, '@item': value })) :
        { '@id': `${values}`, '@item': values };
    }
  }
}
