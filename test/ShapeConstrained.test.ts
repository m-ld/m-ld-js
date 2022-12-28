import { MockGraphState, mockInterim, testConfig } from './testClones';
import { OrmDomain } from '../src/orm';
import { PropertyShape, ShapeConstrained } from '../src/shacl';
import { SingletonExtensionSubject } from '../src/orm/ExtensionSubject';
import { PropertyShapeSpec } from '../src/shacl/Shape';

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

describe('Property shape constraints', () => {
  let state: MockGraphState;

  beforeEach(async () => {
    state = await MockGraphState.create();
  });

  afterEach(() => state.close());

  /** Boilerplate property shape creation */
  const propertyShape = (spec: PropertyShapeSpec) =>
    new PropertyShape({ '@id': 'myShape', ...PropertyShape.declare(spec) });

  test('constructs with count', () => {
    const shape = propertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
    expect(shape.path).toBe('http://test.m-ld.org/#name');
    expect(shape.minCount).toBe(1);
    expect(shape.maxCount).toBe(1);
  });

  test('checks OK update', async () => {
    const shape = propertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
    await expect(shape.check(state.graph.asReadState, mockInterim({
      '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
    }))).resolves.not.toThrow();
  });

  test('checks too many inserted in update', async () => {
    const shape = propertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
    await expect(shape.check(state.graph.asReadState, mockInterim({
      '@insert': [{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
      }]
    }))).rejects.toBeDefined();
  });

  test('checks OK after update', async () => {
    await state.write({ '@id': 'fred', name: 'Fred' });
    const shape = propertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
    await expect(shape.check(state.graph.asReadState, mockInterim({
      '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }],
      '@insert': [{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
      }]
    }))).resolves.not.toThrow();
  });

  test('checks too many after update', async () => {
    await state.write({ '@id': 'fred', name: 'Fred' });
    const shape = propertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
    await expect(shape.check(state.graph.asReadState, mockInterim({
      '@insert': [{
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Flintstone'
      }]
    }))).rejects.toBeDefined();
  });

  test('checks too few after update', async () => {
    await state.write({ '@id': 'fred', name: 'Fred' });
    const shape = propertyShape({ path: 'http://test.m-ld.org/#name', count: 1 });
    await expect(shape.check(state.graph.asReadState, mockInterim({
      '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
    }))).rejects.toBeDefined();
  });

  test('checks minCount of zero after update', async () => {
    await state.write({ '@id': 'fred', name: 'Fred' });
    const shape = propertyShape({ path: 'http://test.m-ld.org/#name', maxCount: 1 });
    await expect(shape.check(state.graph.asReadState, mockInterim({
      '@delete': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
    }))).resolves.not.toThrow();
  });
});