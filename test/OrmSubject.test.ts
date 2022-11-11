import { MockGraphState, testContext } from './testClones';
import { GraphSubject, updateSubject } from '../src';
import { DefaultList } from '../src/constraints/DefaultList';
import { OrmSubject, OrmUpdating } from '../src/orm';
import { mock } from 'jest-mock-extended';
import { Episode, Flintstone, Series } from './ormFixtures';

describe('Object-RDF Mapping Subjects', () => {
  let state: MockGraphState;
  let orm: OrmUpdating;

  beforeEach(async () => {
    state = await MockGraphState.create({ context: testContext });
    orm = mock<OrmUpdating>({
      async get(src, construct) {
        if (Array.isArray(src))
          throw 'mock cannot load arrays';
        // Here we always load because we don't have an OrmDomain in the test
        return construct((await state.graph.get(
          typeof src == 'string' ? src : src['@id'], state.ctx))!, this);
      }
    });
  });

  afterEach(() => state.close());

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
    const fred = new Flintstone({ '@id': 'fred', name: 'Fred' });
    fred.name = 'Fred Flintstone';
    expect(fred.src.name).toBe('Fred Flintstone');
  });

  test('commit property change', () => {
    const fred = new Flintstone({ '@id': 'fred', name: 'Fred' });
    fred.name = 'Fred Flintstone';
    expect(fred.commit()).toEqual({
      '@delete': { '@id': 'fred', name: ['Fred'] },
      '@insert': { '@id': 'fred', name: ['Fred Flintstone'] }
    });
    expect(fred.name).toBe('Fred');
  });

  test('commit property added', () => {
    const fred = new Flintstone({ '@id': 'fred', name: 'Fred' });
    fred.height = 6;
    expect(fred.commit()).toEqual({
      '@insert': { '@id': 'fred', height: [6] }
    });
    expect(fred.height).toBeUndefined();
  });

  test('commit property deleted', () => {
    const fred = new Flintstone({ '@id': 'fred', name: 'Fred', height: 6 });
    delete fred.height;
    expect(fred.commit()).toEqual({
      '@delete': { '@id': 'fred', height: [6] }
    });
    expect(fred.height).toBe(6);
  });

  test('commit property undefined', () => {
    const fred = new Flintstone({ '@id': 'fred', name: 'Fred', height: 6 });
    fred.height = undefined;
    expect(fred.commit()).toEqual({
      '@delete': { '@id': 'fred', height: [6] }
    });
    expect(fred.height).toBe(6);
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
      this.initSrcList(src, String, this.list, {
        get: i => this.list[i], set: (i, v) => this.list[i] = v
      });
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

  test('updates a list subject', async () => {
    const tff = { '@id': 'tff', title: 'The Flintstone Flyer' };
    const episodesSrc = { '@id': 'episodes', '@list': [tff] };
    const constraint = new DefaultList('test');
    await state.write(episodesSrc, constraint);
    const series = new Series(episodesSrc, orm);
    await series.updated;
    const hlh = { '@id': 'hlh', title: 'Hot Lips Hannigan' };
    const update = await state.write({
      '@insert': { '@id': 'episodes', '@list': { 1: hlh } }
    }, { updateType: 'user', constraint });
    updateSubject(series.src, update);
    await series.updated;
    expect(series.episodes.map(e => e.src)).toEqual([tff, hlh]);
  });

  test('commits a list append', async () => {
    const tff = { '@id': 'tff', title: 'The Flintstone Flyer' };
    const hlh = { '@id': 'hlh', title: 'Hot Lips Hannigan' };
    await state.write({ '@insert': [tff, hlh] });
    const series = new Series({ '@id': 'episodes', '@list': [tff] }, orm);
    await series.updated;
    series.episodes.push(new Episode(hlh, orm));
    expect(series.commit()).toEqual({
      '@insert': { '@id': 'episodes', '@list': { 1: [{ '@id': 'hlh' }] } }
    });
    expect(series.episodes.length).toBe(1);
  });

  test('commits a list remove', async () => {
    const tff = { '@id': 'tff', title: 'The Flintstone Flyer' };
    const hlh = { '@id': 'hlh', title: 'Hot Lips Hannigan' };
    await state.write({ '@insert': [tff, hlh] });
    const series = new Series({ '@id': 'episodes', '@list': [tff, hlh] }, orm);
    await series.updated;
    series.episodes.pop();
    expect(series.commit()).toEqual({
      '@delete': { '@id': 'episodes', '@list': { 1: { '@id': 'hlh' } } }
    });
    expect(series.episodes.length).toBe(2);
  });

  test('commits a list replace', async () => {
    const tff = { '@id': 'tff', title: 'The Flintstone Flyer' };
    const hlh = { '@id': 'hlh', title: 'Hot Lips Hannigan' };
    await state.write({ '@insert': [tff, hlh] });
    const series = new Series({ '@id': 'episodes', '@list': [tff] }, orm);
    await series.updated;
    series.episodes = [new Episode(hlh, orm)];
    expect(series.commit()).toEqual({
      '@delete': { '@id': 'episodes', '@list': { 0: { '@id': 'tff' } } },
      '@insert': { '@id': 'episodes', '@list': { 1: [{ '@id': 'hlh' }] } }
    });
    expect(series.episodes.length).toBe(1);
  });
});