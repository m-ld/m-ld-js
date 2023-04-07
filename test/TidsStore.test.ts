import { PatchTids, TidsStore } from '../src/engine/dataset/TidsStore';
import { memStore } from './testClones';
import { DataFactory as RdfDataFactory } from 'rdf-data-factory/lib/DataFactory';
import { TripleKeyStore } from '../src/engine/dataset';
import { Operation } from '../src/engine/ops';
import { Triple } from '../src/engine/quads';
import { UUID } from '../src/engine/MeldEncoding';

describe('TIDs Store', () => {
  const rdf = new RdfDataFactory();
  const fredName = rdf.quad(
    rdf.namedNode('http://test.m-ld.org/fred'),
    rdf.namedNode('http://test.m-ld.org/#name'),
    rdf.literal('Fred'));
  let store: TripleKeyStore;
  let tidsStore: TidsStore;

  async function doCommit(patch: Partial<Operation<[Triple, UUID]>>) {
    const kvps = await tidsStore.commit(new PatchTids(tidsStore, patch));
    await store.transact({ prepare: () => ({ kvps }) });
  }

  beforeEach(async () => {
    store = await memStore();
    tidsStore = new TidsStore(store);
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
    await doCommit({ inserts: [[fredName, 'tid1'], [fredName, 'tid2']] });
    // Check that we're not doing more work than we ought to
    expect(loadTidsSpy).toHaveBeenCalledTimes(1);
    await expect(tidsStore.findTripleTids(fredName))
      .resolves.toEqual(expect.arrayContaining(['tid1', 'tid2']));
  });

  describe('key compaction', () => {
    test('Stored TID has compacted key', async () => {
      await doCommit({ inserts: [[fredName, 'tid1']] });
      await expect(store.has('_qs:ttd:"fred","#name","Literal","Fred","xs:string"'))
        .resolves.toBe(true);
    });

    test('Common terms compacted', async () => {
      // This is also testing that the store prefixes include xs & rdf
      const fredType = rdf.quad(
        rdf.namedNode('http://test.m-ld.org/fred'),
        rdf.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
        rdf.namedNode('http://www.w3.org/2001/XMLSchema#string'));
      await doCommit({ inserts: [[fredType, 'tid1']] });
      await expect(store.has('_qs:ttd:"fred","rdf:type","NamedNode","xs:string"'))
        .resolves.toBe(true);
    });
  });

  describe('with inserted TID', () => {
    beforeEach(() => doCommit({ inserts: [[fredName, 'tid1']] }));

    test('Finds inserted TID for triple', async () => {
      await expect(tidsStore.findTripleTids(fredName)).resolves.toEqual(['tid1']);
    });

    test('Finds inserted TIDs for triples', async () => {
      expect([...await tidsStore.findTriplesTids([fredName])])
        .toEqual([[fredName, ['tid1']]]);
    });

    test('Replaces inserted TID for triple', async () => {
      await doCommit({
        deletes: [[fredName, 'tid1']], inserts: [[fredName, 'tid2']]
      });
      await expect(tidsStore.findTripleTids(fredName)).resolves.toEqual(['tid2']);
    });

    test('Adds inserted TID for another triple', async () => {
      const fredHeight = rdf.quad(
        rdf.namedNode('http://test.m-ld.org/fred'),
        rdf.namedNode('http://test.m-ld.org/#height'),
        rdf.literal('6'));
      await doCommit({ inserts: [[fredHeight, 'tid2']] });
      await expect(tidsStore.findTripleTids(fredName)).resolves.toEqual(['tid1']);
      await expect(tidsStore.findTripleTids(fredHeight)).resolves.toEqual(['tid2']);
    });

    test('Does not find triple for unmatched subject and predicate', done => {
      const found = tidsStore.findTriples(
        rdf.namedNode('http://test.m-ld.org/fred'),
        rdf.namedNode('http://test.m-ld.org/#height'));
      found.subscribe({
        next: () => { done('Unexpected triple found'); },
        complete: () => done()
      });
    });

    test('Finds triple for subject and predicate', done => {
      // Add an extra triple before and after to ensure correct range
      const fredHeight = rdf.quad(
        rdf.namedNode('http://test.m-ld.org/fred'),
        rdf.namedNode('http://test.m-ld.org/#height'), // lexically before
        rdf.literal('6'));
      const wilmaName = rdf.quad(
        rdf.namedNode('http://test.m-ld.org/wilma'), // lexically after
        rdf.namedNode('http://test.m-ld.org/#name'),
        rdf.literal('Wilma'));
      expect.hasAssertions();
      Promise.all([
        doCommit({ inserts: [[fredHeight, 'tid2']] }),
        doCommit({ inserts: [[wilmaName, 'tid3']] })
      ]).then(() => {
        const found = tidsStore.findTriples(
          rdf.namedNode('http://test.m-ld.org/fred'),
          rdf.namedNode('http://test.m-ld.org/#name'));
        found.subscribe({
          next: ({ value, next }) => {
            expect(value.equals(fredName.object)).toBe(true);
            next();
          },
          complete: () => done()
        });
      });
    });
  });
});