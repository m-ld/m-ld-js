import { MockGraphState, mockInterim } from './testClones';
import { SubjectGraph } from '../src/engine/SubjectGraph';
import { M_LD, SH } from '../src/ns';
import { AgreementProver, Statute, Statutory } from '../src/constraints/Statutory';
import { GraphSubject, OrmDomain } from '../src/index';
import { Shape } from '../src/shacl';
import { MeldError } from '../src/engine/MeldError';
import { DefaultList } from '../src/constraints/DefaultList';

describe('Statutory', () => {
  let state: MockGraphState;

  beforeEach(async () => {
    state = await MockGraphState.create();
  });

  afterEach(() => state.close());

  const nameShape = {
    '@id': 'http://test.m-ld.org/nameShape',
    [SH.path]: { '@vocab': 'http://test.m-ld.org/#name' }
  };

  describe('declarations', () => {
    test('declare extension', async () => {
      await state.write(Statutory.declare(1), new DefaultList('test'));
      await expect(state.graph.asReadState.get('http://m-ld.org/extensions'))
        .resolves.toMatchObject({
          '@id': 'http://m-ld.org/extensions',
          '@list': [{ '@id': 'http://ext.m-ld.org/constraints/Statutory' }]
        });
      await expect(state.graph.asReadState.get('http://ext.m-ld.org/constraints/Statutory'))
        .resolves.toMatchObject({
          '@id': 'http://ext.m-ld.org/constraints/Statutory',
          '@type': 'http://js.m-ld.org/CommonJSModule',
          'http://js.m-ld.org/#require': '@m-ld/m-ld/dist/constraints/Statutory',
          'http://js.m-ld.org/#class': 'Statutory'
        });
    });

    test('declare statute', async () => {
      await state.write(Statutory.declareStatute({
        statutoryShapes: [{ '@id': 'http://test.m-ld.org/nameShape' }],
        sufficientConditions: { '@id': 'http://m-ld.org/#hasAuthority' }
      }));
      const statutes = await state.graph.asReadState.read({
        '@describe': '?statute',
        '@where': { '@id': '?statute', '@type': 'http://m-ld.org/Statute' }
      });
      expect(statutes).toEqual([expect.objectContaining({
        'http://m-ld.org/#statutory-shape': { '@id': 'http://test.m-ld.org/nameShape' },
        'http://m-ld.org/#sufficient-condition': { '@id': 'http://m-ld.org/#hasAuthority' }
      })]);
    });

    test('declare authority', async () => {
      await state.write(Statutory.declareAuthority(
        'http://ex.org/Alice',
        { '@id': 'http://test.m-ld.org/nameShape' }));
      await expect(state.graph.asReadState.get('http://ex.org/Alice'))
        .resolves.toMatchObject({
          '@id': 'http://ex.org/Alice',
          'http://m-ld.org/#has-authority': { '@id': 'http://test.m-ld.org/nameShape' }
        });
    });
  });

  describe('extension', () => {
    test('passes an update if no statutes', async () => {
      const statutory = new Statutory();
      await statutory.initialise(state.graph.asReadState);
      expect.hasAssertions();
      for (let constraint of statutory.constraints)
        await expect(constraint.check(state.graph.asReadState, mockInterim({
          '@ticks': 0,
          '@delete': new SubjectGraph([]),
          '@insert': new SubjectGraph([{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
          }])
        }))).resolves.not.toThrow();
    });

    test('passes an update not affecting statutes', async () => {
      await state.write({
        '@insert': {
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: { '@id': M_LD.hasAuthority }
        }
      });
      const statutory = new Statutory();
      await statutory.initialise(state.graph.asReadState);
      expect.hasAssertions();
      for (let constraint of statutory.constraints)
        await expect(constraint.check(state.graph.asReadState, mockInterim({
          '@ticks': 0,
          '@delete': new SubjectGraph([]),
          '@insert': new SubjectGraph([{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#height': 6
          }])
        }))).resolves.not.toThrow();
    });

    test('passes an update of statutes with authority', async () => {
      await state.write({
        '@insert': [{
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: { '@id': M_LD.hasAuthority }
        }, {
          '@id': 'http://test.m-ld.org/hanna',
          '@type': M_LD.Principal,
          [M_LD.hasAuthority]: nameShape
        }]
      });
      const statutory = new Statutory();
      await statutory.initialise(state.graph.asReadState);
      expect.hasAssertions();
      for (let constraint of statutory.constraints)
        await expect(constraint.check(state.graph.asReadState, mockInterim({
          '@principal': { '@id': 'http://test.m-ld.org/hanna' },
          '@ticks': 0,
          '@delete': new SubjectGraph([]),
          '@insert': new SubjectGraph([{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
          }])
        }))).resolves.not.toThrow();
    });

    test('fails an update of statutes without authority', async () => {
      await state.write({
        '@insert': [{
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: { '@id': M_LD.hasAuthority }
        }, {
          '@id': 'http://test.m-ld.org/hanna',
          '@type': M_LD.Principal,
          [M_LD.hasAuthority]: { // We have authority over height but not name
            '@id': 'http://test.m-ld.org/heightShape',
            [SH.path]: { '@vocab': 'http://test.m-ld.org/#height' }
          }
        }]
      });
      const statutory = new Statutory();
      await statutory.initialise(state.graph.asReadState);
      expect.hasAssertions();
      for (let constraint of statutory.constraints)
        await expect(constraint.check(state.graph.asReadState, mockInterim({
          '@principal': { '@id': 'http://test.m-ld.org/hanna' },
          '@ticks': 0,
          '@delete': new SubjectGraph([]),
          '@insert': new SubjectGraph([{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
          }])
        }))).rejects.toBeDefined();
    });

    test('fails an update of statutes without principal', async () => {
      await state.write({
        '@insert': [{
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: { '@id': M_LD.hasAuthority }
        }, {
          '@id': 'http://test.m-ld.org/hanna',
          '@type': M_LD.Principal,
          [M_LD.hasAuthority]: nameShape
        }]
      });
      const statutory = new Statutory();
      await statutory.initialise(state.graph.asReadState);
      expect.hasAssertions();
      for (let constraint of statutory.constraints)
        await expect(constraint.check(state.graph.asReadState, mockInterim({
          '@ticks': 0,
          '@delete': new SubjectGraph([]),
          '@insert': new SubjectGraph([{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
          }])
        }))).rejects.toBeDefined();
    });

    test('can be initialised on update', async () => {
      const statutory = new Statutory();
      await statutory.initialise(state.graph.asReadState);
      const update = await state.write({
        '@insert': [{
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: { '@id': M_LD.hasAuthority }
        }, {
          '@id': 'http://test.m-ld.org/hanna',
          '@type': M_LD.Principal,
          [M_LD.hasAuthority]: { // We have authority over height but not name
            '@id': 'http://test.m-ld.org/heightShape',
            [SH.path]: { '@vocab': 'http://test.m-ld.org/#height' }
          }
        }]
      });
      await statutory.onUpdate(update, state.graph.asReadState);
      expect.hasAssertions();
      for (let constraint of statutory.constraints)
        await expect(constraint.check(state.graph.asReadState, mockInterim({
          '@principal': { '@id': 'http://test.m-ld.org/hanna' },
          '@ticks': 0,
          '@delete': new SubjectGraph([]),
          '@insert': new SubjectGraph([{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
          }])
        }))).rejects.toBeDefined();
    });

    test('authority can be updated', async () => {
      await state.write({
        '@insert': [{
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: { '@id': M_LD.hasAuthority }
        }, {
          '@id': 'http://test.m-ld.org/hanna',
          '@type': M_LD.Principal,
          [M_LD.hasAuthority]: { // We have authority over height but not name
            '@id': 'http://test.m-ld.org/heightShape',
            [SH.path]: { '@vocab': 'http://test.m-ld.org/#height' }
          }
        }]
      });
      const statutory = new Statutory();
      await statutory.initialise(state.graph.asReadState);
      const update = await state.write({
        '@id': 'http://test.m-ld.org/hanna',
        [M_LD.hasAuthority]: nameShape
      });
      // Now we have authority over name too
      await statutory.onUpdate(update, state.graph.asReadState);
      expect.hasAssertions();
      for (let constraint of statutory.constraints)
        await expect(constraint.check(state.graph.asReadState, mockInterim({
          '@principal': { '@id': 'http://test.m-ld.org/hanna' },
          '@ticks': 0,
          '@delete': new SubjectGraph([]),
          '@insert': new SubjectGraph([{
            '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
          }])
        }))).resolves.not.toThrow();
    });
  });

  describe('statute', () => {
    let appState: OrmDomain;

    class TestProver extends AgreementProver {
      value: any;
      constructor(src: GraphSubject) {
        super(src);
        this.value = src['http://test.m-ld.org/#value'];
      }
      async prove() { return this.value; }
      async test(state: any, affected: any, proof: any) {
        return proof === this.value;
      }
    }

    const testProver = (src: GraphSubject) => new TestProver(src);

    beforeEach(() => {
      appState = new OrmDomain();
    });

    test('passes an update of non-statutes', async () => {
      const statute = await appState.withActiveState(state.graph.asReadState, orm =>
        new Statute({
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: {
            '@id': 'http://test.m-ld.org/alwaysTrue',
            'http://test.m-ld.org/#value': true
          }
        }, orm, testProver));
      const update = {
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#height': 5
        }])
      };
      await expect(statute.test(state.graph.asReadState, update)).resolves.not.toThrow();
      await expect(statute.check(state.graph.asReadState,
        mockInterim({ ...update, '@ticks': 0 }))).resolves.not.toThrow();
    });

    test('agrees an insert of a property statute', async () => {
      const statute = await appState.withActiveState(state.graph.asReadState, orm =>
        new Statute({
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: {
            '@id': 'http://test.m-ld.org/alwaysTrue',
            'http://test.m-ld.org/#value': true
          }
        }, orm, testProver));
      const update = {
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }])
      };
      const interim = mockInterim({ ...update, '@ticks': 0 });
      await statute.check(state.graph.asReadState, interim);
      expect(interim.assert).toBeCalledWith({ '@agree': true });
      await expect(statute.test(state.graph.asReadState,
        { ...update, '@agree': true })).resolves.not.toThrow();
    });

    test('agrees a delete of a property statute', async () => {
      const statute = await appState.withActiveState(state.graph.asReadState, orm =>
        new Statute({
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          [M_LD.sufficientCondition]: {
            '@id': 'http://test.m-ld.org/alwaysTrue',
            'http://test.m-ld.org/#value': true
          }
        }, orm, testProver));
      const update = {
        '@delete': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }]),
        '@insert': new SubjectGraph([])
      };
      const interim = mockInterim({ ...update, '@ticks': 0 });
      await statute.check(state.graph.asReadState, interim);
      expect(interim.assert).toBeCalledWith({ '@agree': true });
      await expect(statute.test(state.graph.asReadState,
        { ...update, '@agree': true })).resolves.not.toThrow();
    });

    test('finds sufficient condition', async () => {
      const statute = await appState.withActiveState(state.graph.asReadState, orm =>
        new Statute({
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          // First condition is unproven
          [M_LD.sufficientCondition]: [{
            '@id': 'http://test.m-ld.org/alwaysFalse',
            'http://test.m-ld.org/#value': false
          }, {
            '@id': 'http://test.m-ld.org/alwaysTrue',
            'http://test.m-ld.org/#value': true
          }]
        }, orm, testProver));
      const update = {
        '@ticks': 0,
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }])
      };
      const interim = mockInterim({ ...update, '@ticks': 0 });
      await statute.check(state.graph.asReadState, interim);
      expect(interim.assert).toBeCalledWith({ '@agree': true });
      await expect(statute.test(state.graph.asReadState,
        { ...update, '@agree': true })).resolves.not.toThrow();
    });

    test('fails if insufficient conditions', async () => {
      const statute = await appState.withActiveState(state.graph.asReadState, orm =>
        new Statute({
          '@id': 'http://test.m-ld.org/nameStatute',
          '@type': M_LD.Statute,
          [M_LD.statutoryShape]: nameShape,
          // All conditions are unproven
          [M_LD.sufficientCondition]: [{
            '@id': 'http://test.m-ld.org/alwaysFalse',
            'http://test.m-ld.org/#value': false
          }, {
            '@id': 'http://test.m-ld.org/alwaysNull',
            'http://test.m-ld.org/#value': 0
          }]
        }, orm, testProver));
      const update = {
        '@delete': new SubjectGraph([]),
        '@insert': new SubjectGraph([{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }])
      };
      const interim = mockInterim({ ...update, '@ticks': 0 });
      await expect(statute.check(state.graph.asReadState, interim)).rejects.toBeDefined();
      await expect(statute.test(state.graph.asReadState,
        { ...update, '@agree': true })).rejects.toBeDefined();
    });
  });

  describe('has-authority agreement prover', () => {
    let appState: OrmDomain;

    beforeEach(() => {
      appState = new OrmDomain();
    });

    test('throws if no principal', async () => {
      expect.hasAssertions();
      return appState.withActiveState(state.graph.asReadState, async orm => {
        const prover = AgreementProver.from({ '@id': M_LD.hasAuthority }, orm);
        const shape = Shape.from(nameShape);
        await expect(prover.prove(
          state.graph.asReadState,
          [shape],
          undefined
        )).rejects.toThrow(MeldError);
        await expect(prover.test(
          state.graph.asReadState,
          [shape],
          undefined,
          undefined
        )).rejects.toThrow(MeldError);
      });
    });

    test('returns falsey if principal not found', async () => {
      expect.hasAssertions();
      return appState.withActiveState(state.graph.asReadState, async orm => {
        const prover = AgreementProver.from({ '@id': M_LD.hasAuthority }, orm);
        const shape = Shape.from(nameShape);
        const principalRef = { '@id': 'http://test.m-ld.org/hanna' };
        await expect(prover.prove(state.graph.asReadState, [shape], principalRef))
          .resolves.toBe(false);
        await expect(prover.test(state.graph.asReadState, [shape], true, principalRef))
          .resolves.toBe(false);
      });
    });

    test('returns truthy if principal has authority', async () => {
      expect.hasAssertions();
      return appState.withActiveState(state.graph.asReadState, async orm => {
        const prover = AgreementProver.from({ '@id': M_LD.hasAuthority }, orm);
        await state.write({
          '@id': 'http://test.m-ld.org/hanna',
          [M_LD.hasAuthority]: nameShape
        });
        const shape = Shape.from(nameShape);
        const principalRef = { '@id': 'http://test.m-ld.org/hanna' };
        await expect(prover.prove(state.graph.asReadState, [shape], principalRef))
          .resolves.toBe(true);
        await expect(prover.test(state.graph.asReadState, [shape], true, principalRef))
          .resolves.toBe(true);
      });
    });
  });
});
