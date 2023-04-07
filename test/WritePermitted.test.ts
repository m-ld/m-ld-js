import { SH } from '../src/ns';
import { MockGraphState, mockInterim, testConfig } from './testClones';
import { WritePermitted } from '../src/security';
import { SubjectGraph } from '../src/engine/SubjectGraph';
import { OrmDomain } from '../src/orm/index';
import { MeldError } from '../src/index';

describe('Write permissions', () => {
  let state: MockGraphState;
  let domain: OrmDomain;

  beforeEach(async () => {
    state = await MockGraphState.create();
    domain = new OrmDomain({ config: testConfig(), app: {} });
  });

  afterEach(() => state.close());

  const nameShape = {
    '@id': 'http://test.m-ld.org/nameShape',
    [SH.path]: { '@vocab': 'http://test.m-ld.org/#name' }
  };

  test('allows anything if no permissions', async () => {
    const writePermitted = new WritePermitted();
    await domain.updating(state.graph.asReadState, orm =>
      writePermitted.initialise({ '@id': 'security/WritePermitted' }, orm));
    expect.hasAssertions();
    for (let constraint of writePermitted.constraints)
      await expect(constraint.check(state.graph.asReadState, mockInterim({
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }])
      }))).resolves.not.toThrow();
  });

  test('allows anything not subject to permissions', async () => {
    await state.write(WritePermitted.declareControlled(
      'http://test.m-ld.org/namePermission', nameShape));
    const writePermitted = new WritePermitted();
    await domain.updating(state.graph.asReadState, orm =>
      writePermitted.initialise({ '@id': 'security/WritePermitted' }, orm));
    expect.hasAssertions();
    for (let constraint of writePermitted.constraints)
      await expect(constraint.check(state.graph.asReadState, mockInterim({
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#height': 6
        }])
      }))).resolves.not.toThrow();
  });

  test('disallows an update without permission', async () => {
    await state.write(WritePermitted.declareControlled(
      'http://test.m-ld.org/namePermission', nameShape));
    const writePermitted = new WritePermitted();
    await domain.updating(state.graph.asReadState, orm =>
      writePermitted.initialise({ '@id': 'security/WritePermitted' }, orm));
    expect.hasAssertions();
    for (let constraint of writePermitted.constraints)
      await expect(constraint.check(state.graph.asReadState, mockInterim({
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }])
      }))).rejects.toThrowError(MeldError);
  });

  test('allows an update with permission', async () => {
    await state.write(WritePermitted.declareControlled(
      'http://test.m-ld.org/namePermission', nameShape));
    await state.write(WritePermitted.declarePermission(
      'http://test.m-ld.org/hanna',
      { '@id': 'http://test.m-ld.org/namePermission' }));
    const writePermitted = new WritePermitted();
    await domain.updating(state.graph.asReadState, orm =>
      writePermitted.initialise({ '@id': 'security/WritePermitted' }, orm));
    expect.hasAssertions();
    for (let constraint of writePermitted.constraints)
      await expect(constraint.check(state.graph.asReadState, mockInterim({
        '@principal': { '@id': 'http://test.m-ld.org/hanna' },
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }])
      }))).resolves.not.toThrow();
  });
});