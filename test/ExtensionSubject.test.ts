import { MockGraphState, testConfig, testContext } from './testClones';
import { M_LD } from '../src/ns';
import { ExtensionSubject, OrmDomain } from '../src/orm';
import { ExtensionSubjectInstance, SingletonExtensionSubject } from '../src/orm/ExtensionSubject';

interface MyKindOfExtension extends ExtensionSubjectInstance {
  doIt(): boolean;
}

export class MyExtension implements MyKindOfExtension {
  doIt = () => true;
}

describe('Extension subject', () => {
  let state: MockGraphState;
  let domain: OrmDomain;

  beforeEach(async () => {
    state = await MockGraphState.create();
    domain = new OrmDomain({
      config: testConfig(), app: {}, context: await testContext
    });
  });

  afterEach(() => state.close());

  test('Loads singleton', async () => {
    const src = {
      '@id': 'myDoIt',
      '@type': M_LD.JS.commonJsExport,
      [M_LD.JS.require]: require.resolve('./ExtensionSubject.test'),
      [M_LD.JS.className]: 'MyExtension'
    };
    expect.hasAssertions();
    await domain.updating(state.graph.asReadState, async orm => {
      const es = await orm.get(src, src =>
        new SingletonExtensionSubject<MyKindOfExtension>(src, orm));
      const inst = await es.singleton;
      expect(inst).toBeDefined();
      expect(inst.doIt()).toBe(true);
    });
  });

  test('Loads instance', async () => {
    const src = { '@id': 'myDoIt', '@type': 'myExtensionType' };
    await state.write(ExtensionSubject.declare(
      'myExtensionType',
      require.resolve('./ExtensionSubject.test'),
      'MyExtension')
    );
    expect.hasAssertions();
    await domain.updating(state.graph.asReadState, async orm => {
      const inst = await ExtensionSubject.instance<MyKindOfExtension>(src, orm);
      expect(inst).toBeDefined();
      expect(inst.doIt()).toBe(true);
    });
  });

  test('Cannot load if no type', async () => {
    const src = { '@id': 'myDoIt', 'done': true };
    expect.hasAssertions();
    await domain.updating(state.graph.asReadState, orm =>
      expect(ExtensionSubject.instance<MyKindOfExtension>(src, orm))
        .rejects.toThrow(TypeError));
  });

  test('Cannot load if type is not a module', async () => {
    const src = { '@id': 'myDoIt', '@type': 'myExtensionType' };
    await state.write({
      '@insert': {
        '@id': 'myExtensionType',
        [M_LD.JS.require]: require.resolve('./ExtensionSubject.test'),
        [M_LD.JS.className]: 'MyExtension'
      }
    });
    expect.hasAssertions();
    await domain.updating(state.graph.asReadState, orm =>
      expect(ExtensionSubject.instance<MyKindOfExtension>(src, orm))
        .rejects.toThrow(TypeError));
  });

  test.todo('Reloads extension on update');
  test.todo('Passes graph subject to extension initialise on update');
});