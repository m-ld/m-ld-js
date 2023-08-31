import { MockGraphState, testConfig, testDomainContext } from './testClones';
import { DefaultList } from '../src/lseq/DefaultList';
import { SingleValued } from '../src/constraints/SingleValued';
import {
  combinePlugins, GraphSubject, MeldConstraint, MeldPlugin, MeldTransportSecurity,
  noTransportSecurity
} from '../src';
import { mock, mockFn } from 'jest-mock-extended';
import { M_LD } from '../src/ns';
import { CloneExtensions } from '../src/engine/CloneExtensions';
import { OrmUpdating } from '../src/orm';
import { ExtensionSubjectInstance } from '../src/orm/ExtensionSubject';

const thisModuleId = require.resolve('./CloneExtensions.test');

export class MockExtensions implements ExtensionSubjectInstance, MeldPlugin {
  static mockTs = mock<MeldTransportSecurity>();
  static mockConstraint = mock<MeldConstraint>();
  static last: OrmUpdating;

  initFromData(src: GraphSubject, orm: OrmUpdating) {
    MockExtensions.last = orm;
    return this;
  }

  constraints = [MockExtensions.mockConstraint];

  transportSecurity = MockExtensions.mockTs;
}

describe('Top-level extensions loading', () => {
  test('empty config has just default list', async () => {
    const cloneExtensions = await CloneExtensions.initial(testConfig(), {}, testDomainContext);
    const ext = await cloneExtensions.ready();
    expect(ext.transportSecurity).toBe(noTransportSecurity);
    const constraints = [...ext.constraints!];
    expect(constraints.length).toBe(1);
    expect(constraints[0]).toBeInstanceOf(DefaultList);
  });

  test('from config', async () => {
    // noinspection JSDeprecatedSymbols - testing legacy config
    const cloneExtensions = await CloneExtensions.initial(testConfig({
      constraints: [{
        '@type': 'single-valued',
        property: 'prop1'
      }]
    }), {}, testDomainContext);
    const ext = await cloneExtensions.ready();
    expect(ext.transportSecurity).toBe(noTransportSecurity);
    const constraints = [...ext.constraints!];
    expect(constraints.length).toBe(2);
    expect(constraints[0]).toBeInstanceOf(SingleValued);
    expect(constraints[0]).toMatchObject({ property: 'http://test.m-ld.org/#prop1' });
    expect(constraints[1]).toBeInstanceOf(DefaultList);
  });

  test('from app', async () => {
    const cloneExtensions = await CloneExtensions.initial(testConfig(), {
      constraints: [MockExtensions.mockConstraint],
      transportSecurity: MockExtensions.mockTs
    }, testDomainContext);
    const ext = await cloneExtensions.ready();
    expect(ext.transportSecurity).toBe(MockExtensions.mockTs);
    const constraints = [...ext.constraints!];
    expect(constraints.length).toBe(2);
    expect(constraints[0]).toBe(MockExtensions.mockConstraint);
    expect(constraints[1]).toBeInstanceOf(DefaultList);
  });

  test('combined in app', async () => {
    const setExtensionContext = mockFn();
    const cloneExtensions = await CloneExtensions.initial(testConfig(),
      combinePlugins([{
        transportSecurity: MockExtensions.mockTs
      }, {
        setExtensionContext
      }]),
      testDomainContext);
    const ext = await cloneExtensions.ready();
    expect(ext.transportSecurity).toBe(MockExtensions.mockTs);
    expect(setExtensionContext).toHaveBeenCalled();
  });

  describe('from data', () => {
    let state: MockGraphState;

    beforeEach(async () => {
      state = await MockGraphState.create();
    });

    afterEach(() => state.close());

    test('initialises with no modules', async () => {
      const cloneExtensions = await CloneExtensions.initial(testConfig(), {}, testDomainContext);
      await cloneExtensions.onInitial(state.graph.asReadState);
      const ext = await cloneExtensions.ready();
      expect(ext.transportSecurity).toBe(noTransportSecurity);
    });

    test('loads a module on initialise', async () => {
      const config = testConfig();
      const cloneExtensions = await CloneExtensions.initial(config, {}, testDomainContext);
      let ext = await cloneExtensions.ready();
      expect(ext.transportSecurity).toBe(noTransportSecurity);
      await state.write({
        '@id': M_LD.extensions,
        '@list': [{
          [M_LD.JS.require]: thisModuleId,
          [M_LD.JS.className]: 'MockExtensions'
        }]
      }, new DefaultList('test'));
      await cloneExtensions.onInitial(state.graph.asReadState);
      ext = await cloneExtensions.ready();
      expect(ext.transportSecurity).toBe(MockExtensions.mockTs);
      const constraints = [...ext.constraints!];
      expect(constraints.length).toBe(2);
      expect(constraints[0]).toBe(MockExtensions.mockConstraint);
      expect(constraints[1]).toBeInstanceOf(DefaultList);
      expect(MockExtensions.last.domain.config).toBe(config);
    });

    test('loads a module on update', async () => {
      const config = testConfig();
      const cloneExtensions = await CloneExtensions.initial(config, {}, testDomainContext);
      await cloneExtensions.onInitial(state.graph.asReadState);
      const update = await state.write({
        '@id': M_LD.extensions,
        '@list': [{
          [M_LD.JS.require]: thisModuleId,
          [M_LD.JS.className]: 'MockExtensions'
        }]
      }, new DefaultList('test'));
      await cloneExtensions.onUpdate(update, state.graph.asReadState);
      let ext = await cloneExtensions.ready();
      expect(ext.transportSecurity).toBe(MockExtensions.mockTs);
      const constraints = [...ext.constraints!];
      expect(constraints.length).toBe(2);
      expect(constraints[0]).toBe(MockExtensions.mockConstraint);
      expect(constraints[1]).toBeInstanceOf(DefaultList);
      expect(MockExtensions.last.domain.config).toBe(config);
    });
  });
});