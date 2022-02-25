import { CloneExtensions } from '../src/engine/index';
import { MockGraphState, testConfig, testContext } from './testClones';
import { DefaultList } from '../src/constraints/DefaultList';
import { SingleValued } from '../src/constraints/SingleValued';
import {
  MeldApp, MeldConfig, MeldConstraint, MeldExtensions, MeldTransportSecurity
} from '../src/index';
import { mock, mockFn } from 'jest-mock-extended';
import { M_LD } from '../src/ns';

const thisModuleId = require.resolve('./CloneExtensions.test');

export class MockExtensions implements MeldExtensions {
  static mockTs = mock<MeldTransportSecurity>();
  static mockConstraint = mock<MeldConstraint>();
  static lastConfig: MeldConfig;
  static lastApp: MeldApp;

  constructor(config: MeldConfig, app: MeldApp) {
    MockExtensions.lastConfig = config;
    MockExtensions.lastApp = app;
  }

  readonly constraints = [MockExtensions.mockConstraint];
  readonly transportSecurity = MockExtensions.mockTs;
  readonly initialise = mockFn();
  readonly onUpdate = mockFn();
}

describe('Top-level extensions loading', () => {
  test('empty config has just default list', async () => {
    const ext = await CloneExtensions.initial(testConfig(), {}, testContext);
    expect(ext.transportSecurity).toBeUndefined();
    const constraints = [...ext.constraints];
    expect(constraints.length).toBe(1);
    expect(constraints[0]).toBeInstanceOf(DefaultList);
  });

  test('from config', async () => {
    const ext = await CloneExtensions.initial(testConfig({
      constraints: [{
        '@type': 'single-valued',
        property: 'prop1'
      }]
    }), {}, testContext);
    expect(ext.transportSecurity).toBeUndefined();
    const constraints = [...ext.constraints];
    expect(constraints.length).toBe(2);
    expect(constraints[0]).toBeInstanceOf(SingleValued);
    expect(constraints[0]).toMatchObject({ property: 'http://test.m-ld.org/#prop1' });
    expect(constraints[1]).toBeInstanceOf(DefaultList);
  });

  test('from app', async () => {
    const ext = await CloneExtensions.initial(testConfig(), {
      constraints: [MockExtensions.mockConstraint],
      transportSecurity: MockExtensions.mockTs
    }, testContext);
    expect(ext.transportSecurity).toBe(MockExtensions.mockTs);
    const constraints = [...ext.constraints];
    expect(constraints.length).toBe(2);
    expect(constraints[0]).toBe(MockExtensions.mockConstraint);
    expect(constraints[1]).toBeInstanceOf(DefaultList);
  });

  describe('from data', () => {
    let state: MockGraphState;

    beforeEach(async () => {
      state = await MockGraphState.create({ context: testContext });
    });

    afterEach(() => state.close());

    test('initialises with no modules', async () => {
      const ext = await CloneExtensions.initial(testConfig(), {}, testContext);
      await ext.initialise(state.jrqlGraph.asReadState);
      expect(ext.transportSecurity).toBeUndefined();
    });

    test('loads a module on initialise', async () => {
      const config = testConfig();
      const ext = await CloneExtensions.initial(config, {}, testContext);
      expect(ext.transportSecurity).toBeUndefined();
      await state.write({
        '@id': M_LD.extensions,
        '@list': [{
          '@type': M_LD.JS.commonJsModule,
          [M_LD.JS.require]: thisModuleId,
          [M_LD.JS.className]: 'MockExtensions'
        }]
      }, new DefaultList('test'));
      await ext.initialise(state.jrqlGraph.asReadState);
      expect(MockExtensions.lastConfig).toBe(config);
      expect(ext.transportSecurity).toBe(MockExtensions.mockTs);
      const constraints = [...ext.constraints];
      expect(constraints.length).toBe(2);
      expect(constraints[0]).toBe(MockExtensions.mockConstraint);
      expect(constraints[1]).toBeInstanceOf(DefaultList);
    });

    test('loads a module on update', async () => {
      const config = testConfig();
      const ext = await CloneExtensions.initial(config, {}, testContext);
      await ext.initialise(state.jrqlGraph.asReadState);
      const update = await state.write({
        '@id': M_LD.extensions,
        '@list': [{
          '@type': M_LD.JS.commonJsModule,
          [M_LD.JS.require]: thisModuleId,
          [M_LD.JS.className]: 'MockExtensions'
        }]
      }, new DefaultList('test'));
      await ext.onUpdate(update, state.jrqlGraph.asReadState);
      expect(MockExtensions.lastConfig).toBe(config);
      expect(ext.transportSecurity).toBe(MockExtensions.mockTs);
      const constraints = [...ext.constraints];
      expect(constraints.length).toBe(2);
      expect(constraints[0]).toBe(MockExtensions.mockConstraint);
      expect(constraints[1]).toBeInstanceOf(DefaultList);
    });
  });
});