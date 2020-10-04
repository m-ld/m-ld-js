import { SuSetDataset } from '../src/engine/dataset/SuSetDataset';
import { memStore } from './testClones';
import { TreeClock } from '../src/engine/clocks';
import { Hash } from '../src/engine/hash';
import { first, toArray, isEmpty } from 'rxjs/operators';
import { uuid } from 'short-uuid';
import { Subject } from 'json-rql';
import { EncodedDelta } from '../src/engine';
import { Dataset } from '../src/engine/dataset';
import { from } from 'rxjs';
import { Describe, MeldConstraint } from '../src';
import { NO_CONSTRAINT } from '../src/constraints';

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

  function captureUpdate() {
    return ssd.updates.pipe(first()).toPromise();
  }

  describe('with basic config', () => {
    let dataset: Dataset;

    beforeEach(async () => {
      dataset = await memStore();
      ssd = new SuSetDataset(dataset, NO_CONSTRAINT, {
        '@id': 'test', '@domain': 'test.m-ld.org', genesis: true
      });
      await ssd.initialise();
    });

    test('cannot share a dataset', async () => {
      const otherSsd = new SuSetDataset(dataset, NO_CONSTRAINT, {
        '@id': 'boom', '@domain': 'test.m-ld.org', genesis: true
      });
      await expect(otherSsd.initialise()).rejects.toThrow();
    });

    test('has a hash', async () => {
      expect(await ssd.lastHash()).toBeTruthy();
    });

    test('does not have a time', async () => {
      expect(await ssd.loadClock()).toBeNull();
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
        const [ver, tid, del, ins] = msg.data;
        expect(ver).toBe(0);
        expect(tid).toBeTruthy();

        expect(await EncodedDelta.decode(ins))
          .toEqual({ '@id': 'fred', 'name': 'Fred' });
        expect(await EncodedDelta.decode(del)).toEqual({});

        await expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);
      });

      test('applies an insert delta', async () => {
        const willUpdate = captureUpdate();

        await ssd.apply(
          [0, 'B6FVbHGtFxXhdLKEVmkcd', '{}', '{"@id":"fred","name":"Fred"}'],
          remoteTime = remoteTime.ticked(),
          localTime = localTime.update(remoteTime).ticked(),
          localTime = localTime.ticked());

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
          ])).data[1];
        });

        test('has a new hash', async () => {
          const newHash = await ssd.lastHash();
          expect(newHash.equals(firstHash)).toBe(false);
        });

        test('answers the new time', async () => {
          const newTime = await ssd.loadClock();
          expect(newTime && newTime.equals(localTime)).toBe(true);
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
          const [_, tid, del, ins] = msg.data;
          expect(tid).toBeTruthy();

          expect(await EncodedDelta.decode(ins)).toEqual({});
          expect(await EncodedDelta.decode(del)).toMatchObject({
            '@type': 'rdf:Statement',
            'tid': firstTid,
            's': 'fred',
            'p': '#name',
            'o': 'Fred'
          });

          await expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);
        });

        test('applies a delete delta', async () => {
          const willUpdate = captureUpdate();

          await ssd.apply(
              // Deleting the triple based on the inserted Transaction ID
            [0, 'uSX1mPGhuWAEH56RLwYmvG', `{"@type":"rdf:Statement",
              "tid":"${firstTid}","o":"Fred","p":"#name",
              "s":"fred"}`, '{}'],
            remoteTime = remoteTime.ticked(),
            localTime = localTime.update(remoteTime).ticked(),
            localTime = localTime.ticked());

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

        test('rejects a duplicate transaction', async () => {
          await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.delete({ '@id': 'http://test.m-ld.org/fred' })
          ]);
          await ssd.apply(
            [0, firstTid, '{}', '{"@id":"fred","name":"Fred"}'],
            remoteTime = remoteTime.ticked(),
            localTime = localTime.update(remoteTime).ticked(),
            localTime = localTime.ticked());

          await expect(ssd.find1({ '@id': 'http://test.m-ld.org/fred' })).resolves.toEqual('');
        });

        test('rejects a duplicate txn after snapshot', async () => {
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
          await ssd.apply(
            [0, firstTid, '{}', '{"@id":"fred","name":"Fred"}'],
            remoteTime = remoteTime.ticked(),
            localTime = localTime.update(remoteTime).ticked(),
            localTime = localTime.ticked());

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
          await ssd.apply(
            [0, 'uSX1mPGhuWAEH56RLwYmvG', `{"@type":"rdf:Statement",
              "tid":"${firstTid}","o":"Fred","p":"#name",
              "s":"fred"}`, '{}'],
            thirdTime,
            localTime = localTime.update(thirdTime).ticked(),
            localTime = localTime.ticked());

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(thirdTime.equals(opArray[0].time)).toBe(true);
          expect(opArray[0].data[1]).toBe('uSX1mPGhuWAEH56RLwYmvG');
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
            localTime = localTime.update(remoteTime).ticked(),
            localTime = localTime.ticked());

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          // We expect only the missed local op
          expect(opArray.length).toBe(1);
          expect(opArray[0].data[1]).toBe(localOp.data[1]);
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
            localTime = localTime.update(thirdTime).ticked(),
            localTime = localTime.ticked());
          // Remote does see fourth party op
          await ssd.apply(remoteInsert(barney),
            fourthTime = fourthTime.ticked(),
            localTime = localTime.update(fourthTime).ticked(),
            localTime = localTime.ticked());
          remoteTime = remoteTime.update(fourthTime).ticked();

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          // We expect only the missed remote op
          expect(opArray.length).toBe(1);
          expect(opArray[0].data[1]).toBe(thirdOp[1]);
        });

        // @see https://github.com/m-ld/m-ld-js/issues/29
        test('accepts own unpersisted update', async () => {
          await ssd.apply(
            [0, 'uSX1mPGhuWAEH56RLwYmvG', '{}', '{"@id":"wilma","name":"Wilma"}'],
            localTime = localTime.ticked(),
            localTime,
            localTime = localTime.ticked());

          await expect(ssd.find1({ '@id': 'http://test.m-ld.org/wilma' }))
            .resolves.toEqual('http://test.m-ld.org/wilma');
        });

        // @see https://github.com/m-ld/m-ld-js/issues/29
        test('answers unpersisted remote op', async () => {
          // Remote knows about first entry
          remoteTime = remoteTime.update(localTime);
          // Create a remote entry that the remote fails to persist fully
          const unpersistedTime = remoteTime.ticked();
          await ssd.apply(
            [0, 'uSX1mPGhuWAEH56RLwYmvG', `{"@type":"rdf:Statement",
              "tid":"${firstTid}","o":"Fred","p":"#name",
              "s":"fred"}`, '{}'],
            unpersistedTime,
            localTime = localTime.update(unpersistedTime).ticked(),
            localTime = localTime.ticked());

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(unpersistedTime.equals(opArray[0].time)).toBe(true);
          expect(opArray[0].data[1]).toBe('uSX1mPGhuWAEH56RLwYmvG');
        });
      });
    });
  });

  describe('with a constraint', () => {
    let { left: localTime, right: remoteTime } = TreeClock.GENESIS.forked();
    let constraint: MeldConstraint;

    beforeEach(async () => {
      constraint = {
        check: () => Promise.resolve(),
        apply: () => Promise.resolve(null)
      };
      ssd = new SuSetDataset(await memStore(), constraint, {
        '@id': 'test', '@domain': 'test.m-ld.org', genesis: true
      });
      await ssd.initialise();
      await ssd.saveClock(localTime = localTime.ticked(), true);
    });

    test('checks the constraint', async () => {
      constraint.check = () => Promise.reject('Failed!');
      await expect(ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.insert(fred)
      ])).rejects.toBe('Failed!');
    });

    test('provides read to the constraint', async () => {
      await ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.insert(wilma)
      ]);
      constraint.check = async (_, read) =>
        read<Describe>({ '@describe': 'http://test.m-ld.org/wilma' }).toPromise().then(wilma => {
          if (wilma == null)
            throw 'not found!';
        });
      await expect(ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.insert(fred)
      ])).resolves.toBeDefined();
    });

    test('applies an inserting constraint', async () => {
      constraint.apply = async () => ({ '@insert': wilma });
      const willUpdate = captureUpdate();
      const msg = await ssd.apply(
        [0, 'B6FVbHGtFxXhdLKEVmkcd', '{}', '{"@id":"fred","name":"Fred"}'],
        remoteTime = remoteTime.update(localTime).ticked(),
        localTime = localTime.update(remoteTime).ticked(),
        localTime = localTime.ticked());

      expect(msg).not.toBeNull();
      if (msg != null) {
        expect(msg.time.equals(localTime)).toBe(true);
        expect(msg.data[1]).toBeTruthy();
      }
      await expect(ssd.find1({ '@id': 'http://test.m-ld.org/wilma' }))
        .resolves.toEqual('http://test.m-ld.org/wilma');

      expect((<TreeClock>await ssd.loadClock()).equals(localTime)).toBe(true);

      await expect(willUpdate).resolves.toEqual(
        { '@delete': [], '@insert': [fred, wilma], '@ticks': localTime.ticks });
      
      // Check that we have a valid journal
      const ops = await ssd.operationsSince(remoteTime);
      if (ops == null)
        fail();
      const entries = await ops.pipe(toArray()).toPromise();
      expect(entries.length).toBe(1);
      expect(entries[0].time.equals(localTime)).toBe(true);
      const [,, del, ins] = entries[0].data;
      expect(del).toBe('{}');
      expect(ins).toBe('{\"@id\":\"wilma\",\"name\":\"Wilma\"}');
    });

    test('does not apply a constraint if suppressed', async () => {
      constraint.apply = async () => ({ '@insert': wilma });
      const willUpdate = captureUpdate();
      await ssd.apply(
        [0, 'B6FVbHGtFxXhdLKEVmkcd', '{}', '{"@id":"fred","name":"Fred"}'],
        remoteTime = remoteTime.ticked(),
        localTime = localTime.update(remoteTime).ticked(),
        // No tick provided for constraint
        localTime);

      await expect(ssd.find1({ '@id': 'http://test.m-ld.org/wilma' }))
        .resolves.toBeFalsy();

      await expect(willUpdate).resolves.toEqual(
        { '@delete': [], '@insert': [fred], '@ticks': localTime.ticks });
    });

    test('applies a deleting constraint', async () => {
      constraint.apply = async () => ({ '@delete': wilma });

      await ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.insert(wilma)
      ]);

      const willUpdate = captureUpdate();
      await ssd.apply(
        [0, 'B6FVbHGtFxXhdLKEVmkcd', '{}', '{"@id":"fred","name":"Fred"}'],
        remoteTime = remoteTime.ticked(),
        localTime = localTime.update(remoteTime).ticked(),
        localTime = localTime.ticked());

      await expect(ssd.find1({ '@id': 'http://test.m-ld.org/wilma' }))
        .resolves.toBeFalsy();

      await expect(willUpdate).resolves.toEqual(
        { '@insert': [fred], '@delete': [wilma], '@ticks': localTime.ticks });
    });

    test('applies a self-deleting constraint', async () => {
      // Constraint is going to delete the data we're inserting
      constraint.apply = async () => ({ '@delete': wilma });

      const willUpdate = captureUpdate();
      await ssd.apply(
        [0, 'B6FVbHGtFxXhdLKEVmkcd', '{}', '{"@id":"wilma","name":"Wilma"}'],
        remoteTime = remoteTime.ticked(),
        localTime = localTime.update(remoteTime).ticked(),
        localTime = localTime.ticked());

      await expect(ssd.find1({ '@id': 'http://test.m-ld.org/wilma' }))
        .resolves.toBeFalsy();

      await expect(willUpdate).resolves.toEqual(
        { '@insert': [], '@delete': [wilma], '@ticks': localTime.ticks });
    });
  });

  test('enforces delta size limit', async () => {
    ssd = new SuSetDataset(await memStore(), NO_CONSTRAINT, {
      '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, maxDeltaSize: 1
    });
    await ssd.initialise();
    await ssd.saveClock(TreeClock.GENESIS, true);
    await expect(ssd.transact(async () => [
      TreeClock.GENESIS.ticked(),
      await ssd.insert(fred)
    ])).rejects.toThrow();
  });
});

function remoteInsert(subject: Subject): EncodedDelta {
  return [0, uuid(), '{}', JSON.stringify(subject)];
}
