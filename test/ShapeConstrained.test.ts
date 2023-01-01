import { MockGraphState, mockInterim, testConfig } from './testClones';
import { OrmDomain } from '../src/orm';
import { PropertyShape, ShapeConstrained } from '../src/shacl';
import { SingletonExtensionSubject } from '../src/orm/ExtensionSubject';

describe('Shape constrained extension', () => {
  let state: MockGraphState;
  let domain: OrmDomain;

  beforeEach(async () => {
    state = await MockGraphState.create();
    domain = new OrmDomain({ config: testConfig(), app: {} });
  });

  afterEach(() => state.close());

  /** Boilerplate normally done by the CloneExtensions */
  const loadShapeConstrained = async () =>
    await domain.updating(state.graph.asReadState, async orm => (await orm.get({
      '@id': 'http://ext.m-ld.org/shacl/ShapeConstrained'
    }, src => new SingletonExtensionSubject<ShapeConstrained>(src, orm))).singleton);

  test('initialises with property shape', async () => {
    await state.write(ShapeConstrained.declare(0, PropertyShape.declare({
      path: 'name'
    })));
    //
    const ext = await loadShapeConstrained();
    expect(ext.constraints).toMatchObject([{
      path: 'http://test.m-ld.org/#name'
    }]);
  });

  test('checks updates', async () => {
    await state.write(ShapeConstrained.declare(0, PropertyShape.declare({
      path: 'name', count: 1
    })));
    const ext = await loadShapeConstrained();
    expect(ext.constraints.length).toBe(1);
    await expect(ext.constraints[0].check(state.graph.asReadState, mockInterim({
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
    }))).resolves.not.toThrow();
    await expect(ext.constraints[0].check(state.graph.asReadState, mockInterim({
      '@insert': [{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
      }]
    }))).rejects.toBeDefined();
  });
});
