import { MockGraphState, mockInterim, MockRemotes, testConfig, testContext } from './testClones';
import { OrmDomain } from '../src/orm';
import { PropertyShape, ShapeConstrained } from '../src/shacl';
import { SingletonExtensionSubject } from '../src/orm/ExtensionSubject';
import { PropertyShapeSpec } from '../src/shacl/PropertyShape';
import { clone, MeldClone } from '../src/index';
import { MemoryLevel } from 'memory-level';

describe('Shape constrained extension', () => {
  describe('declared in app', () => {
    // NOTE most functional testing is done in the 'declared in data' suite

    let api: MeldClone;

    afterEach(() => api?.close());

    test('checks bad update', async () => {
      api = await clone(
        new MemoryLevel, MockRemotes, testConfig(),
        new ShapeConstrained(new PropertyShape({
          path: 'http://test.m-ld.org/#name', count: 1
        })));
      await expect(api.write({
        '@id': 'fred', 'name': ['Fred', 'Flintstone']
      })).rejects.toBeDefined();
    });
  });

  describe('declared in data', () => {
    let state: MockGraphState;
    let domain: OrmDomain;

    beforeEach(async () => {
      state = await MockGraphState.create();
      domain = new OrmDomain({
        config: testConfig(), app: {}, context: await testContext
      });
    });

    afterEach(() => state.close());

    async function installShapeConstrained(spec: PropertyShapeSpec) {
      await state.write(ShapeConstrained.declare(0, PropertyShape.declare(spec)));
      return domain.updating(state.graph.asReadState, async orm => (await orm.get({
        '@id': 'http://ext.m-ld.org/shacl/ShapeConstrained'
      }, src => new SingletonExtensionSubject<ShapeConstrained>(src, orm))).singleton);
    }

    test('initialises with property shape', async () => {
      const ext = await installShapeConstrained({ path: 'name' });
      expect(ext.shapes).toMatchObject([{
        path: 'http://test.m-ld.org/#name'
      }]);
    });

    test('checks OK update', async () => {
      const ext = await installShapeConstrained({ path: 'name', count: 1 });
      const interim = mockInterim({
        '@insert': [{ '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred' }]
      });
      await expect(Promise.all(ext.constraints.map(c =>
        c.check(state.graph.asReadState, interim)))).resolves.not.toThrow();
    });

    test('checks bad update', async () => {
      const ext = await installShapeConstrained({ path: 'name', count: 1 });
      const interim = mockInterim({
        '@insert': [{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': ['Fred', 'Flintstone']
        }]
      });
      await expect(Promise.all(ext.constraints.map(c =>
        c.check(state.graph.asReadState, interim)))).rejects.toBeDefined();
    });

    test('allows subject deletion', async () => {
      const ext = await installShapeConstrained({ path: 'name', count: 1 });
      await state.write({
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
      });
      const interim = mockInterim({
        '@delete': [{
          '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
        }]
      });
      await expect(Promise.all(ext.constraints.map(c =>
        c.check(state.graph.asReadState, interim)))).resolves.not.toThrow();
    });

    test('does not contradict subject deletion', async () => {
      const ext = await installShapeConstrained({ path: 'name', count: 1 });
      const fredName = {
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
      };
      await state.write(fredName);
      const interim = mockInterim({ '@delete': [fredName] });
      await Promise.all(ext.constraints.map(c =>
        c.apply!(state.graph.asReadState, interim)));
      // This is an internal detail: that the shape will correct the minValue
      // violation, but that will be overridden by removal of the assertion
      expect(interim.assert).toBeCalledWith({ '@insert': fredName });
      expect(interim.remove).toBeCalledWith({ '@insert': fredName });
    });

    test('does not contradict correction if subject not deleted', async () => {
      const ext = await installShapeConstrained({ path: 'name', count: 1 });
      const fredName = {
        '@id': 'http://test.m-ld.org/fred', 'http://test.m-ld.org/#name': 'Fred'
      };
      await state.write({ ...fredName, 'http://test.m-ld.org/#height': 1 });
      const interim = mockInterim({ '@delete': [fredName] });
      await Promise.all(ext.constraints.map(c =>
        c.apply!(state.graph.asReadState, interim)));
      expect(interim.assert).toBeCalledWith({ '@insert': fredName });
      expect(interim.remove).not.toBeCalled();
    });
  });
});
