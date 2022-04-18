import { MockGraphState, testContext } from './testClones';
import { GraphSubject, Optional, Subject, updateSubject } from '../src';
import { DefaultList } from '../src/constraints/DefaultList';
import { OrmSubject } from '../src/orm';

describe('Object-RDF Mapping', () => {
  let state: MockGraphState;

  beforeEach(async () => {
    state = await MockGraphState.create({ context: testContext });
  });

  afterEach(() => state.close());

  class Flintstone extends OrmSubject {
    name: string;
    height?: number;

    constructor(src: GraphSubject) {
      super(src);
      this.initSrcProperty(src, 'name', String,
        () => this.name, v => this.name = v);
      this.initSrcProperty(src, 'height', [Optional, Number],
        () => this.height, (v?: number) => this.height = v);
    }
  }

  test('updates a subject string property', async () => {
    const fredSrc = { '@id': 'fred', name: 'Fred' };
    await state.write(fredSrc);
    const fred = new Flintstone(fredSrc);
    expect(fred.deleted).toBe(false);
    const update = await state.write({
      '@delete': fredSrc,
      '@insert': { '@id': 'fred', name: 'Fred Flintstone' }
    }, { updateType: 'user' });
    updateSubject(fred.src, update);
    expect(fred.deleted).toBe(false);
    expect(fred.name).toBe('Fred Flintstone');
  });

  test('source property reflects changes', () => {
    const fredSrc = { '@id': 'fred', name: 'Fred' };
    const fred = new Flintstone(fredSrc);
    fred.name = 'Fred Flintstone';
    expect(fred.src.name).toBe('Fred Flintstone');
  });

  test('update rejects if invariant broken', async () => {
    const fredSrc = { '@id': 'fred', name: 'Fred', height: 6 };
    await state.write(fredSrc);
    const fred = new Flintstone(fredSrc);
    const update = await state.write(
      { '@delete': { '@id': 'fred', name: 'Fred' } }, { updateType: 'user' });
    updateSubject(fred.src, update);
    await expect(fred.updated).rejects.toThrowError(TypeError);
    // Update failed, so name property unchanged
    expect(fred.src.name).toBe('Fred');
    expect(fred.src.height).toBe(6);
    expect(fred.deleted).toBe(false);
  });

  test('object is deleted if all properties removed', async () => {
    const fredSrc = { '@id': 'fred', name: 'Fred', height: 6 };
    await state.write(fredSrc);
    const fred = new Flintstone(fredSrc);
    const update = await state.write({ '@delete': fredSrc }, { updateType: 'user' });
    updateSubject(fred.src, update);
    expect(fred.deleted).toBe(true);
  });

  class EpisodeTitles extends OrmSubject {
    list: string[] = [];

    constructor(src: GraphSubject) {
      super(src);
      this.initList(src, String, this.list,
        i => this.list[i], (i, v) => this.list[i] = v);
    }
  }

  test('updates a list string', async () => {
    const episodesSrc = { '@id': 'episodes', '@list': ['The Flintstone Flyer'] };
    const constraint = new DefaultList('test');
    await state.write(episodesSrc, constraint);
    const episodes = new EpisodeTitles(episodesSrc);
    expect(episodes.deleted).toBe(false);
    let update = await state.write({
      '@insert': { '@id': 'episodes', '@list': { 1: 'Hot Lips Hannigan' } }
    }, { updateType: 'user', constraint });
    updateSubject(episodes.src, update);
    expect(episodes.list).toEqual(['The Flintstone Flyer', 'Hot Lips Hannigan']);
    expect(episodes.deleted).toBe(false);
    update = await state.write({
      '@delete': { '@id': 'episodes', '@list': { '?': '?' } }
    }, { updateType: 'user', constraint });
    updateSubject(episodes.src, update);
    expect(episodes.list).toEqual([]);
    expect(episodes.deleted).toBe(true);
  });

  class Episode extends OrmSubject {
    title: string;

    constructor(src: GraphSubject, title?: string) {
      super(src);
      this.initSrcProperty(src, 'title', String,
        () => this.title, v => this.title = v, title);
    }
  }

  class Episodes extends OrmSubject {
    list: Episode[] = [];

    constructor(src: GraphSubject) {
      super(src);
      this.initList(src, Subject, this.list,
        i => this.list[i].src, async (i, v: GraphSubject) =>
          // Here we always load because we don't have an OrmDomain in the test
          this.list[i] = new Episode((await state.graph.get(v['@id'], state.ctx))!));
    }
  }

  test('updates a list subject', async () => {
    const tff = { '@id': 'tff', title: 'The Flintstone Flyer' };
    const episodesSrc = { '@id': 'episodes', '@list': [tff] };
    const constraint = new DefaultList('test');
    await state.write(episodesSrc, constraint);
    const episodes = new Episodes(episodesSrc);
    await episodes.updated;
    const hlh = { '@id': 'hlh', title: 'Hot Lips Hannigan' };
    const update = await state.write({
      '@insert': { '@id': 'episodes', '@list': { 1: hlh } }
    }, { updateType: 'user', constraint });
    updateSubject(episodes.src, update);
    await episodes.updated;
    expect(episodes.list.map(e => e.src)).toEqual([tff, hlh]);
  });
});