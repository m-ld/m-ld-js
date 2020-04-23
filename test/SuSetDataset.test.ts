import { SuSetDataset } from '../src/dataset/SuSetDataset';
import { memStore } from './testClones';
import { TreeClock } from '../src/clocks';
import { Hash } from '../src/hash';
import { first, toArray } from 'rxjs/operators';

const fred = {
  '@id': 'http://test.m-ld.org/fred',
  'http://test.m-ld.org/#name': 'Fred'
}, barney = {
  '@id': 'http://test.m-ld.org/barney',
  'http://test.m-ld.org/#name': 'Barney'
};

describe('SU-Set Dataset', () => {
  let ds: SuSetDataset;

  function captureUpdate() {
    return ds.updates.pipe(first()).toPromise();
  }

  beforeEach(() => {
    ds = new SuSetDataset(memStore());
  });

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

      test('transacts an insert', async () => {
        const willUpdate = captureUpdate();

        const msg = await ds.transact(async () => [
          time = time.ticked(),
          await ds.insert(fred)
        ]);

        expect(msg.time.equals(time)).toBe(true);
        expect(msg.data.tid).toBeTruthy();

        const insertedJsonLd = JSON.parse(msg.data.insert);
        expect(insertedJsonLd).toHaveProperty(['@graph', '@id'], 'http://test.m-ld.org/fred');
        expect(insertedJsonLd).toHaveProperty(['@graph', 'http://test.m-ld.org/#name'], 'Fred');

        expect(JSON.parse(msg.data.delete)).toHaveProperty('@graph', {});

        await expect(willUpdate).resolves.toHaveProperty('@insert', { '@graph': [fred] });
      });

      test('applies an insert delta', async () => {
        const willUpdate = captureUpdate();

        await ds.apply({
          tid: 'B6FVbHGtFxXhdLKEVmkcd',
          insert: '{"@graph":{"@id":"http://test.m-ld.org/fred","http://test.m-ld.org/#name":"Fred"}}',
          delete: '{"@graph":{}}'
        }, time = time.ticked());
        await expect(ds.find({ '@id': 'http://test.m-ld.org/fred' }))
          .resolves.toEqual(new Set(['http://test.m-ld.org/fred']));

        await expect(willUpdate).resolves.toHaveProperty('@insert', { '@graph': [fred] });
      });

      describe('with an initial triple', () => {
        let firstHash: Hash;
        let firstTid: string;

        beforeEach(async () => firstHash = await ds.lastHash());

        beforeEach(async () => {
          firstTid = (await ds.transact(async () => [
            time = time.ticked(),
            await ds.insert(fred)
          ])).data.tid;
        });

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

        test('transacts a delete', async () => {
          const willUpdate = captureUpdate();

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

          await expect(willUpdate).resolves.toHaveProperty('@delete', { '@graph': [fred] });
        });

        test('applies a delete delta', async () => {
          const willUpdate = captureUpdate();

          await ds.apply({
            tid: 'uSX1mPGhuWAEH56RLwYmvG',
            insert: '{"@graph":{}}',
            // Deleting the triple based on the inserted Transaction ID
            delete: `{"@graph":{"@id":"b4vMkTurWFf6qjBuhkRvjX","@type":"rdf:Statement",
              "tid":"${firstTid}","o":"Fred","p":"http://test.m-ld.org/#name",
              "s":"http://test.m-ld.org/fred"}}`
          }, time = time.ticked());
          await expect(ds.find({ '@id': 'http://test.m-ld.org/fred' }))
            .resolves.toEqual(new Set());

          await expect(willUpdate).resolves.toHaveProperty('@delete', { '@graph': [fred] });
        });

        test('transacts another insert', async () => {
          const msg = await ds.transact(async () => [
            time = time.ticked(),
            await ds.insert(barney)
          ]);
          expect(msg.time.equals(time)).toBe(true);

          await expect(ds.describe1('http://test.m-ld.org/barney')).resolves.toEqual(barney);
        })

        test('answers operations since first', async () => {
          const lastHash = await ds.lastHash();
          const msg = await ds.transact(async () => [
            time = time.ticked(),
            await ds.insert(barney)
          ]);
          const ops = await ds.operationsSince(lastHash);
          expect(ops).not.toBeNull();
          if (ops == null) return; // Compiler avoidance
          const opArray = await ops.pipe(toArray()).toPromise();
          expect(opArray.length).toBe(1);
          expect(msg.time.equals(opArray[0].time)).toBe(true);
        })
      });
    });
  });
});