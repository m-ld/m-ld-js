import { SuSetDataset } from '../src/dataset/SuSetDataset';
import { memStore } from './testClones';
import { TreeClock } from '../src/clocks';
import { Hash } from '../src/hash';
import { first, toArray, isEmpty } from 'rxjs/operators';
import { uuid } from 'short-uuid';
import { Subject } from 'json-rql';
import { JsonDelta } from '../src/m-ld';
import { Dataset } from '../src/dataset';
import { from } from 'rxjs';

const fred = {
  '@id': 'http://test.m-ld.org/fred',
  'http://test.m-ld.org/#name': 'Fred'
}, wilma = {
  '@id': 'http://test.m-ld.org/wilma',
  'http://test.m-ld.org/#name': 'Wilma'
}, barney = {
  '@id': 'http://test.m-ld.org/barney',
  'http://test.m-ld.org/#name': 'Barney'
};

describe('SU-Set Dataset', () => {
  let ssd: SuSetDataset;
  let dataset: Dataset;

  function captureUpdate() {
    return ssd.updates.pipe(first()).toPromise();
  }

  beforeEach(() => {
    dataset = memStore();
    ssd = new SuSetDataset('test', dataset);
  });

  describe('when initialised', () => {
    beforeEach(async () => ssd.initialise());

    test('cannot share a dataset', async () => {
      const otherSsd = new SuSetDataset('boom', dataset);
      await expect(otherSsd.initialise()).rejects.toThrow();
    });

    test('has a hash', async () => {
      expect(await ssd.lastHash()).toBeTruthy();
    });

    test('does not have a time', async () => {
      expect(await ssd.loadClock()).toBeNull();
    });

    test('has no unsent operations', async () => {
      await expect(ssd.undeliveredLocalOperations().toPromise()).resolves.toBeUndefined();
    });

    describe('with an initial time', () => {
      let { left: localTime, right: remoteTime } = TreeClock.GENESIS.forked();

      beforeEach(async () => ssd.saveClock(localTime = localTime.ticked(), true));

      test('does not answer operations since before start', async () => {
        await expect(ssd.operationsSince(remoteTime)).resolves.toBeUndefined();
      });

      test('has no operations since first time', async () => {
        const ops = await ssd.operationsSince(localTime);
        await expect(ops && ops.pipe(isEmpty()).toPromise()).resolves.toBe(true);
      });

      test('answers the time', async () => {
        const savedTime = await ssd.loadClock();
        expect(savedTime && savedTime.equals(localTime)).toBe(true);
      });

      test('answers an empty snapshot', async () => {
        const snapshot = await ssd.takeSnapshot();
        expect(snapshot.lastTime.equals(localTime)).toBe(true);
        expect(snapshot.lastHash).toBeTruthy();
        await expect(snapshot.quads.toPromise()).resolves.toBeUndefined();
        await expect(snapshot.tids.toPromise()).resolves.toBeUndefined();
      });

      test('transacts an insert', async () => {
        const willUpdate = captureUpdate();

        const msg = await ssd.transact(async () => [
          localTime = localTime.ticked(),
          await ssd.insert(fred)
        ]);

        expect(msg.time.equals(localTime)).toBe(true);
        expect(msg.data.tid).toBeTruthy();

        const insertedJsonLd = JSON.parse(msg.data.insert);
        expect(insertedJsonLd).toEqual(fred);

        expect(JSON.parse(msg.data.delete)).toEqual({});

        await expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);
      });

      test('applies an insert delta', async () => {
        const willUpdate = captureUpdate();

        await ssd.apply({
          tid: 'B6FVbHGtFxXhdLKEVmkcd',
          insert: '{"@id":"http://test.m-ld.org/fred","http://test.m-ld.org/#name":"Fred"}',
          delete: '{}'
        }, remoteTime = remoteTime.ticked(), localTime = localTime.update(remoteTime).ticked());

        await expect(ssd.find1({ '@id': 'http://test.m-ld.org/fred' }))
          .resolves.toEqual('http://test.m-ld.org/fred');

        await expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);
      });

      describe('with an initial triple', () => {
        let firstHash: Hash;
        let firstTid: string;

        beforeEach(async () => {
          firstHash = await ssd.lastHash();
          firstTid = (await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.insert(fred)
          ])).data.tid;
        });

        test('has a new hash', async () => {
          const newHash = await ssd.lastHash();
          expect(newHash.equals(firstHash)).toBe(false);
        });

        test('answers the new time', async () => {
          const newTime = await ssd.loadClock();
          expect(newTime && newTime.equals(localTime)).toBe(true);
        });

        test('has an unsent operation', async () => {
          await expect(ssd.undeliveredLocalOperations().toPromise()).resolves.toBeDefined();
        });

        test('answers a snapshot', async () => {
          const snapshot = await ssd.takeSnapshot();
          expect(snapshot.lastTime.equals(localTime)).toBe(true);
          expect(snapshot.lastHash.equals(firstHash)).toBe(false);
          await expect(snapshot.quads.toPromise()).resolves.toBeDefined();
          await expect(snapshot.tids.toPromise()).resolves.toBeDefined();
        });

        test('applies a snapshot', async () => {
          const snapshot = await ssd.takeSnapshot();
          const lastHash = Hash.random(); // Blatant lie, for the test
          await ssd.applySnapshot({
            lastTime: localTime,
            lastHash,
            quads: from(await snapshot.quads.pipe(toArray()).toPromise()),
            tids: from(await snapshot.tids.pipe(toArray()).toPromise())
          }, localTime = localTime.ticked());
          expect((await ssd.lastHash()).equals(lastHash)).toBe(true);
          await expect(ssd.find1({ '@id': 'http://test.m-ld.org/fred' }))
            .resolves.toEqual('http://test.m-ld.org/fred');
        });

        test('transacts a delete', async () => {
          const willUpdate = captureUpdate();

          const msg = await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.delete({ '@id': 'http://test.m-ld.org/fred' })
          ]);

          expect(msg.time.equals(localTime)).toBe(true);
          expect(msg.data.tid).toBeTruthy();

          expect(JSON.parse(msg.data.insert)).toEqual({});

          const deletedJsonLd = JSON.parse(msg.data.delete);
          expect(deletedJsonLd).toMatchObject({
            '@type': 'rdf:Statement',
            'tid': firstTid,
            's': 'http://test.m-ld.org/fred',
            'p': 'http://test.m-ld.org/#name',
            'o': 'Fred'
          });

          await expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);
        });

        test('applies a delete delta', async () => {
          const willUpdate = captureUpdate();

          await ssd.apply({
            tid: 'uSX1mPGhuWAEH56RLwYmvG',
            insert: '{}',
            // Deleting the triple based on the inserted Transaction ID
            delete: `{"@id":"b4vMkTurWFf6qjBuhkRvjX","@type":"rdf:Statement",
              "tid":"${firstTid}","o":"Fred","p":"http://test.m-ld.org/#name",
              "s":"http://test.m-ld.org/fred"}`
          }, remoteTime = remoteTime.ticked(), localTime = localTime.update(remoteTime).ticked());

          await expect(ssd.find1({ '@id': 'http://test.m-ld.org/fred' })).resolves.toEqual('');
          await expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);
        });

        test('transacts another insert', async () => {
          const msg = await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.insert(barney)
          ]);
          expect(msg.time.equals(localTime)).toBe(true);

          await expect(ssd.describe1('http://test.m-ld.org/barney')).resolves.toEqual(barney);
        });

        test('ignores a duplicate transaction', async () => {
          await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.delete({ '@id': 'http://test.m-ld.org/fred' })
          ]);
          await ssd.apply({
            tid: firstTid,
            insert: '{"@id":"http://test.m-ld.org/fred","http://test.m-ld.org/#name":"Fred"}',
            delete: '{}'
          }, remoteTime = remoteTime.ticked(), localTime = localTime.update(remoteTime).ticked());

          await expect(ssd.find1({ '@id': 'http://test.m-ld.org/fred' })).resolves.toEqual('');
        });

        test('ignores a duplicate txn after snapshot', async () => {
          await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.delete({ '@id': 'http://test.m-ld.org/fred' })
          ]);
          const snapshot = await ssd.takeSnapshot();
          await ssd.applySnapshot({
            lastTime: localTime,
            lastHash: await ssd.lastHash(),
            quads: from(await snapshot.quads.pipe(toArray()).toPromise()),
            tids: from(await snapshot.tids.pipe(toArray()).toPromise())
          }, localTime = localTime.ticked());
          await ssd.apply({
            tid: firstTid,
            insert: '{"@id":"http://test.m-ld.org/fred","http://test.m-ld.org/#name":"Fred"}',
            delete: '{}'
          }, remoteTime = remoteTime.ticked(), localTime = localTime.update(remoteTime).ticked());

          await expect(ssd.find1({ '@id': 'http://test.m-ld.org/fred' })).resolves.toEqual('');
        });

        test('answers local op since first', async () => {
          // Remote knows about first entry
          remoteTime = remoteTime.update(localTime);
          // Create a new journal entry that the remote doesn't know
          await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.insert(barney)
          ]);
          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(localTime.equals(opArray[0].time)).toBe(true);
        });

        test('answers remote op since first', async () => {
          // Remote knows about first entry
          remoteTime = remoteTime.update(localTime);
          // Create a remote entry from a third clone that the remote doesn't know
          const forkLocal = localTime.forked();
          localTime = forkLocal.left;
          const thirdTime = forkLocal.right.ticked();
          await ssd.apply({
            tid: 'uSX1mPGhuWAEH56RLwYmvG',
            insert: '{}',
            delete: `{"@id":"b4vMkTurWFf6qjBuhkRvjX","@type":"rdf:Statement",
              "tid":"${firstTid}","o":"Fred","p":"http://test.m-ld.org/#name",
              "s":"http://test.m-ld.org/fred"}`
          }, thirdTime, localTime = localTime.update(thirdTime).ticked());

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(thirdTime.equals(opArray[0].time)).toBe(true);
          expect(opArray[0].data.tid).toBe('uSX1mPGhuWAEH56RLwYmvG');
        });

        test('answers missed local op', async () => {
          remoteTime = remoteTime.update(localTime);
          // New entry that the remote hasn't seen
          const localOp = await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.insert(barney)
          ]);
          // Don't update remote time from local
          await ssd.apply(remoteInsert(wilma),
            remoteTime = remoteTime.ticked(),
            localTime = localTime.update(remoteTime).ticked());

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          // We expect only the missed local op
          expect(opArray.length).toBe(1);
          expect(opArray[0].data.tid).toBe(localOp.data.tid);
        });

        test('answers missed third party op', async () => {
          remoteTime = remoteTime.update(localTime);
          let { left, right } = remoteTime.forked();
          remoteTime = left;
          let { left: thirdTime, right: fourthTime } = right.forked();
          // Remote doesn't see third party op
          const thirdOp = remoteInsert(wilma);
          await ssd.apply(thirdOp,
            thirdTime = thirdTime.ticked(),
            localTime = localTime.update(thirdTime).ticked());
          // Remote does see fourth party op
          await ssd.apply(remoteInsert(barney),
            fourthTime = fourthTime.ticked(),
            localTime = localTime.update(fourthTime).ticked());
          remoteTime = remoteTime.update(fourthTime).ticked();

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          // We expect only the missed remote op
          expect(opArray.length).toBe(1);
          expect(opArray[0].data.tid).toBe(thirdOp.tid);
        });
      });
    });
  });
});

function remoteInsert(subject: Subject): JsonDelta {
  return { tid: uuid(), insert: JSON.stringify(subject), delete: '{}' };
}
