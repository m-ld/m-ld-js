import { SuSetDataset } from '../src/dataset/SuSetDataset';
import { memStore } from './testClones';
import { TreeClock } from '../src/clocks';
import { Hash } from '../src/hash';

describe('SU-Set Dataset', () => {
  let ds: SuSetDataset;

  beforeEach(() => ds = new SuSetDataset(memStore()));

  describe('when initialised', () => {
    beforeEach(async () => ds.initialise());

    test('has a hash', async () => {
      expect(await ds.lastHash()).toBeTruthy();
    });

    test('does not have a time', async () => {
      expect(await ds.loadClock()).toBeNull();
    });

    test('has no unsent operations', async () => {
      await expect(ds.unsentLocalOperations().toPromise()).resolves.toBeUndefined();
    });

    test('does not answer operations since garbage', async () => {
      await expect(ds.operationsSince(Hash.random())).resolves.toBeUndefined();
    });

    test('has no operations since genesis', async () => {
      const ops = await ds.operationsSince(await ds.lastHash());
      await expect(ops && ops.toPromise()).resolves.toBeUndefined();
    });

    describe('with an initial time', () => {
      let time = TreeClock.GENESIS.ticked();

      beforeEach(async () => ds.saveClock(time, true));

      test('answers the time', async () => {
        const savedTime = await ds.loadClock();
        expect(savedTime && savedTime.equals(time)).toBe(true);
      });

      test('answers an empty snapshot', async () => {
        const snapshot = await ds.takeSnapshot();
        expect(snapshot.time.equals(time)).toBe(true);
        expect(snapshot.lastHash).toBeTruthy();
        await expect(snapshot.data.toPromise()).resolves.toBeUndefined();
      });

      test('transacts a triple', async () => {
        const msg = await ds.transact(async () => [
          time = time.ticked(),
          await ds.insert({
            '@id': 'http://test.m-ld.org/fred',
            'http://test.m-ld.org/#name': 'Fred'
          })
        ]);

        expect(msg.time.equals(time)).toBe(true);
        expect(msg.data.tid).toBeTruthy();

        const insertedJsonLd = JSON.parse(msg.data.insert);
        expect(insertedJsonLd).toHaveProperty(['@graph', '@id'], 'http://test.m-ld.org/fred');
        expect(insertedJsonLd).toHaveProperty(['@graph', 'http://test.m-ld.org/#name'], 'Fred');

        expect(JSON.parse(msg.data.delete)).toHaveProperty('@graph', {});
      });

      test('applies an insert delta', async () => {
        await ds.apply({
          'tid': 'B6FVbHGtFxXhdLKEVmkcd',
          'insert': '{"@graph":{"@id":"http://test.m-ld.org/fred","http://test.m-ld.org/#name":"Fred"}}',
          'delete': '{"@graph":{}}'
        }, time = time.ticked());
        await expect(ds.find({ '@id': 'http://test.m-ld.org/fred' }))
          .resolves.toEqual(new Set(['http://test.m-ld.org/fred']));
      });

      describe('with an initial triple', () => {
        let firstHash: Hash;

        beforeEach(async () => firstHash = await ds.lastHash());

        beforeEach(async () => ds.transact(async () => [
          time = time.ticked(),
          await ds.insert({
            '@id': 'http://test.m-ld.org/fred',
            'http://test.m-ld.org/#name': 'Fred'
          })
        ]));

        test('has a new hash', async () => {
          const newHash = await ds.lastHash();
          expect(newHash.equals(firstHash)).toBe(false);
        });

        test('answers the new time', async () => {
          const newTime = await ds.loadClock();
          expect(newTime && newTime.equals(time)).toBe(true);
        });

        test('has an unsent operation', async () => {
          await expect(ds.unsentLocalOperations().toPromise()).resolves.toBeDefined();
        });

        test('answers a snapshot', async () => {
          const snapshot = await ds.takeSnapshot();
          expect(snapshot.time.equals(time)).toBe(true);
          expect(snapshot.lastHash.equals(firstHash)).toBe(false);
          await expect(snapshot.data.toPromise()).resolves.toBeDefined();
        });


        test('can delete the triple', async () => {
          const msg = await ds.transact(async () => [
            time = time.ticked(),
            await ds.delete({ '@id': 'http://test.m-ld.org/fred' })
          ]);

          expect(msg.time.equals(time)).toBe(true);
          expect(msg.data.tid).toBeTruthy();

          expect(JSON.parse(msg.data.insert)).toHaveProperty('@graph', {});

          const deletedJsonLd = JSON.parse(msg.data.delete);
          expect(deletedJsonLd).toHaveProperty(['@graph', '@type'], 'rdf:Statement');
          expect(deletedJsonLd).toHaveProperty(['@graph', 'tid']);
          expect(deletedJsonLd).toHaveProperty(['@graph', 's'], 'http://test.m-ld.org/fred');
          expect(deletedJsonLd).toHaveProperty(['@graph', 'p'], 'http://test.m-ld.org/#name');
          expect(deletedJsonLd).toHaveProperty(['@graph', 'o'], 'Fred');
        });
      });
    });
  });
});