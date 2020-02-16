import { QuadSolution } from '../src/dataset/QuadSolution';
import { quad, namedNode, literal, variable } from '@rdfjs/data-model';

describe('Quad solution', () => {
  test('empty intersects with no-variable quad', () => {
    const actual = quad(namedNode('fl:fred'), namedNode('fl:#name'), literal('Fred'));
    const solution = QuadSolution.EMPTY.intersect(actual, actual);
    expect(solution && solution.quads.length).toBe(1);
    expect(solution && solution.quads[0].equals(actual)).toBe(true);
  });

  test('empty intersects with variable subject quad', () => {
    const pattern = quad(variable('id'), namedNode('fl:#name'), literal('Fred'));
    const actual = quad(namedNode('fl:fred'), namedNode('fl:#name'), literal('Fred'));
    const solution = QuadSolution.EMPTY.intersect(pattern, actual);
    expect(solution && solution.quads.length).toBe(1);
    expect(solution && solution.quads[0].equals(actual)).toBe(true);
    expect(solution && solution.vars['id'].termType).toBe('NamedNode');
    expect(solution && solution.vars['id'].value).toBe('fl:fred');
  });

  test('empty intersects with variable predicate quad', () => {
    const pattern = quad(namedNode('fl:fred'), variable('name'), literal('Fred'));
    const actual = quad(namedNode('fl:fred'), namedNode('fl:#name'), literal('Fred'));
    const solution = QuadSolution.EMPTY.intersect(pattern, actual);
    expect(solution && solution.quads.length).toBe(1);
    expect(solution && solution.quads[0].equals(actual)).toBe(true);
    expect(solution && solution.vars['name'].termType).toBe('NamedNode');
    expect(solution && solution.vars['name'].value).toBe('fl:#name');
  });

  test('with variable intersects with same variable value', () => {
    const pattern = quad(variable('id'), namedNode('fl:#name'), literal('Fred'));
    const actual1 = quad(namedNode('fl:fred'), namedNode('fl:#name'), literal('Fred'));
    const actual2 = quad(namedNode('fl:fred'), namedNode('fl:#height'), literal('10'));
    const solution = QuadSolution.EMPTY.intersect(pattern, actual1)?.intersect(pattern, actual2);
    expect(solution && solution.quads.length).toBe(2);
    expect(solution && solution.quads[0].equals(actual1)).toBe(true);
    expect(solution && solution.quads[1].equals(actual2)).toBe(true);
    expect(solution && solution.vars['id'].termType).toBe('NamedNode');
    expect(solution && solution.vars['id'].value).toBe('fl:fred');
  });

  test('with variable does not intersect with different variable value', () => {
    const pattern = quad(variable('id'), namedNode('fl:#name'), literal('Fred'));
    const actual1 = quad(namedNode('fl:fred'), namedNode('fl:#name'), literal('Fred'));
    const actual2 = quad(namedNode('fl:wilma'), namedNode('fl:#height'), literal('10'));
    const solution = QuadSolution.EMPTY.intersect(pattern, actual1)?.intersect(pattern, actual2);
    expect(solution).toBeNull();
  });

  test('with variable intersects with same variable value in different position', () => {
    const pattern1 = quad(variable('id1'), namedNode('fl:#name'), literal('Fred'));
    const pattern2 = quad(variable('id2'), namedNode('fl:#hubby'), variable('id1'));
    const actual1 = quad(namedNode('fl:fred'), namedNode('fl:#name'), literal('Fred'));
    const actual2 = quad(namedNode('fl:wilma'), namedNode('fl:#hubby'), namedNode('fl:fred'));
    const solution = QuadSolution.EMPTY.intersect(pattern1, actual1)?.intersect(pattern2, actual2);
    expect(solution && solution.quads.length).toBe(2);
    expect(solution && solution.quads[0].equals(actual1)).toBe(true);
    expect(solution && solution.quads[1].equals(actual2)).toBe(true);
    expect(solution && solution.vars['id1'].termType).toBe('NamedNode');
    expect(solution && solution.vars['id1'].value).toBe('fl:fred');
  });

  test('with variable intersects with different variable in same quad', () => {
    const pattern = quad(variable('id'), namedNode('fl:#name'), variable('name'));
    const actual = quad(namedNode('fl:fred'), namedNode('fl:#name'), literal('Fred'));
    const solution = QuadSolution.EMPTY.intersect(pattern, actual);
    expect(solution && solution.quads.length).toBe(1);
    expect(solution && solution.quads[0].equals(actual)).toBe(true);
    expect(solution && solution.vars['id'].termType).toBe('NamedNode');
    expect(solution && solution.vars['id'].value).toBe('fl:fred');
    expect(solution && solution.vars['name'].termType).toBe('Literal');
    expect(solution && solution.vars['name'].value).toBe('Fred');
  });

  test('with variable intersects with different variable in another quad', () => {
    const pattern1 = quad(variable('id1'), namedNode('fl:#name'), literal('Fred'));
    const pattern2 = quad(variable('id2'), namedNode('fl:#hubby'), variable('hubby'));
    const actual1 = quad(namedNode('fl:fred'), namedNode('fl:#name'), literal('Fred'));
    const actual2 = quad(namedNode('fl:wilma'), namedNode('fl:#hubby'), namedNode('fl:fred'));
    const solution = QuadSolution.EMPTY.intersect(pattern1, actual1)?.intersect(pattern2, actual2);
    expect(solution && solution.quads.length).toBe(2);
    expect(solution && solution.quads[0].equals(actual1)).toBe(true);
    expect(solution && solution.quads[1].equals(actual2)).toBe(true);
    expect(solution && solution.vars['id1'].termType).toBe('NamedNode');
    expect(solution && solution.vars['id2'].value).toBe('fl:wilma');
  });
});