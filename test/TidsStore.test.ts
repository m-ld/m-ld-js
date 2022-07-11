import { PatchTids, TidsStore } from '../src/engine/dataset/TidsStore';
import { memStore } from './testClones';
import { DataFactory as RdfDataFactory } from 'rdf-data-factory/lib/DataFactory';
import { KvpStore } from '../src/engine/dataset';

describe('TIDs Store', () => {
  const rdf = new RdfDataFactory();
  const fredName = rdf.quad(
    rdf.namedNode('http://ex.org/fred'),
    rdf.namedNode('http://ex.org/#name'),
    rdf.literal('Fred'));
  let kvpStore: KvpStore;
  let tidsStore: TidsStore;

  beforeEach(async () => {
    kvpStore = await memStore();
    tidsStore = new TidsStore(kvpStore);
  });

  test('Finds no TIDs for triple when empty', async () => {
    await expect(tidsStore.findTripleTids(fredName)).resolves.toEqual([]);
  });

  test('Finds no TIDs for triples when empty', async () => {
    expect([...await tidsStore.findTriplesTids([fredName])]).toEqual([]);
  });

  test('Finds no TIDs including empty for triples when empty', async () => {
    expect([...await tidsStore.findTriplesTids([fredName], 'includeEmpty')])
      .toEqual([[fredName, []]]);
  });

  test('adds multiple tids for a triple', async () => {
    const loadTidsSpy = jest.spyOn(tidsStore, 'findTripleTids');
    const kvps = await tidsStore.commit(new PatchTids(tidsStore, {
      inserts: [[fredName, 'tid1'], [fredName, 'tid2']]
    }));
    await kvpStore.transact({ prepare: () => ({ kvps }) });
    // Check that we're not doing more work than we ought to
    expect(loadTidsSpy).toHaveBeenCalledTimes(1);
    await expect(tidsStore.findTripleTids(fredName))
      .resolves.toEqual(expect.arrayContaining(['tid1', 'tid2']));
  });

  describe('with inserted TID', () => {
    beforeEach(async () => {
      const kvps = await tidsStore.commit(new PatchTids(tidsStore, {
        inserts: [[fredName, 'tid1']]
      }));
      await kvpStore.transact({ prepare: () => ({ kvps }) });
    });

    test('Finds inserted TID for triple', async () => {
      await expect(tidsStore.findTripleTids(fredName)).resolves.toEqual(['tid1']);
    });

    test('Finds inserted TIDs for triples', async () => {
      expect([...await tidsStore.findTriplesTids([fredName])])
        .toEqual([[fredName, ['tid1']]]);
    });

    test('Replaces inserted TID for triple', async () => {
      const patchTids = new PatchTids(tidsStore, {
        deletes: [[fredName, 'tid1']], inserts: [[fredName, 'tid2']]
      });
      const kvps = await tidsStore.commit(patchTids);
      await kvpStore.transact({ prepare: () => ({ kvps }) });

      await expect(tidsStore.findTripleTids(fredName)).resolves.toEqual(['tid2']);
    });

    test('Adds inserted TID for another triple', async () => {
      const fredHeight = rdf.quad(
        rdf.namedNode('http://ex.org/fred'),
        rdf.namedNode('http://ex.org/#height'),
        rdf.literal('6'));
      const patchTids = new PatchTids(tidsStore, {
        inserts: [[fredHeight, 'tid2']]
      });
      const kvps = await tidsStore.commit(patchTids);
      await kvpStore.transact({ prepare: () => ({ kvps }) });

      await expect(tidsStore.findTripleTids(fredName)).resolves.toEqual(['tid1']);
      await expect(tidsStore.findTripleTids(fredHeight)).resolves.toEqual(['tid2']);
    });
  });
});