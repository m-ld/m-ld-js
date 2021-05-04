import { SuSetDataset } from '../src/engine/dataset/SuSetDataset';
import { memStore } from './testClones';
import { TreeClock } from '../src/engine/clocks';
import { first, toArray, isEmpty, take } from 'rxjs/operators';
import { DeltaMessage, EncodedDelta, txnId } from '../src/engine';
import { Dataset } from '../src/engine/dataset';
import { from } from 'rxjs';
import { Describe, MeldConstraint, Subject } from '../src';
import { jsonify } from './testUtil';
import { SubjectGraph } from '../src/engine/SubjectGraph';

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
    // Convert the subject graphs to JSON for matching convenience
    return ssd.updates.pipe(first()).toPromise().then(jsonify);
  }

  describe('with basic config', () => {
    let dataset: Dataset;

    beforeEach(async () => {
      dataset = await memStore();
      ssd = new SuSetDataset(dataset, {}, [], { '@id': 'test', '@domain': 'test.m-ld.org' });
      await ssd.initialise();
    });

    test('cannot share a dataset', async () => {
      const otherSsd = new SuSetDataset(dataset, {}, [], { '@id': 'boom', '@domain': 'test.m-ld.org' });
      await expect(otherSsd.initialise()).rejects.toThrow();
    });

    test('does not have a time', async () => {
      expect(await ssd.loadClock()).toBeNull();
    });

    describe('with an initial time', () => {
      let localTime: TreeClock, remoteTime: TreeClock;

      beforeEach(async () => {
        let { left, right } = TreeClock.GENESIS.forked();
        localTime = left;
        remoteTime = right;
        await ssd.saveClock(() => localTime = localTime.ticked(), true);
      });

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
        expect(snapshot.lastTime.equals(localTime.scrubId())).toBe(true);
        await expect(snapshot.quads.toPromise()).resolves.toBeUndefined();
      });

      test('transacts a no-op', async () => {
        const willUpdate = captureUpdate();
        const msg = await ssd.transact(async () => [
          localTime = localTime.ticked(),
          await ssd.write({ '@insert': [] })
        ]);
        expect(msg).toBeNull();
        await expect(Promise.race([willUpdate, Promise.resolve()]))
          .resolves.toBeUndefined();
      });

      test('transacts an insert', async () => {
        const willUpdate = captureUpdate();

        const msg = await ssd.transact(async () => [
          localTime = localTime.ticked(),
          await ssd.write({ '@insert': fred })
        ]) ?? fail();
        // The update should happen in-transaction, so no 'await' here
        expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);

        expect(msg.time.equals(localTime)).toBe(true);
        const [ver, fused, del, ins] = msg.data;
        expect(ver).toBe(2);

        expect(await EncodedDelta.decode(ins))
          .toEqual({ '@id': 'fred', 'name': 'Fred' });
        expect(fused).toBe(false);
        expect(await EncodedDelta.decode(del)).toEqual({});
      });

      test('applies an insert delta', async () => {
        const willUpdate = captureUpdate();

        await ssd.apply(new DeltaMessage(
          remoteTime.ticks, remoteTime = remoteTime.ticked(),
          [2, false, '{}', '{"@id":"fred","name":"Fred"}']),
          localTime = localTime.update(remoteTime).ticked(),
          localTime = localTime.ticked());
        expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);

        await expect(ssd.read<Describe>({
          '@describe': 'http://test.m-ld.org/fred'
        }).pipe(toArray()).toPromise()).resolves.toEqual([fred]);
      });

      test('applies a no-op delta', async () => {
        const willUpdate = captureUpdate();

        const msg = await ssd.apply(new DeltaMessage(
          remoteTime.ticks,
          remoteTime = remoteTime.ticked(), [2, false, '{}', '{}']),
          localTime = localTime.update(remoteTime).ticked(),
          localTime = localTime.ticked());

        expect(msg).toBeNull();
        await expect(Promise.race([willUpdate, Promise.resolve()]))
          .resolves.toBeUndefined();
      });

      describe('with an initial triple', () => {
        let firstTid: string;

        beforeEach(async () => {
          firstTid = txnId((await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.write({ '@insert': fred })
          ]) ?? fail()).time);
        });

        test('answers the new time', async () => {
          const newTime = await ssd.loadClock();
          expect(newTime && newTime.equals(localTime)).toBe(true);
        });

        test('answers a snapshot', async () => {
          const snapshot = await ssd.takeSnapshot();
          expect(snapshot.lastTime.equals(localTime.scrubId())).toBe(true);
          const data = await snapshot.quads.toPromise();
          expect(SubjectGraph.fromRDF(data)).toMatchObject([{
            'http://m-ld.org/#tid': firstTid,
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#subject': {
              '@id': 'http://test.m-ld.org/fred'
            },
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate': {
              '@id': 'http://test.m-ld.org/#name'
            },
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#object': 'Fred'
          }]);
        });

        test('snapshot includes entailed triples', async () => {
          // Prepare some entailed information which does not have any
          // transaction identity. Using the dataset this way is a back door and
          // may be brittle if the way that entailments work changes.
          await dataset.transact({
            prepare: async () => ({
              patch: await ssd.write({
                '@insert': {
                  '@id': 'http://test.m-ld.org/fred',
                  'http://test.m-ld.org/#sex': 'male'
                }
              })
            })
          });
          const snapshot = await ssd.takeSnapshot();
          const data = await snapshot.quads.toPromise();
          expect(SubjectGraph.fromRDF(data)).toMatchObject(expect.arrayContaining([
            expect.objectContaining({
              // No tids here
              'http://www.w3.org/1999/02/22-rdf-syntax-ns#subject': {
                '@id': 'http://test.m-ld.org/fred'
              },
              'http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate': {
                '@id': 'http://test.m-ld.org/#sex'
              },
              'http://www.w3.org/1999/02/22-rdf-syntax-ns#object': 'male'
            })
          ]));
        });

        test('applies a snapshot', async () => {
          const snapshot = await ssd.takeSnapshot();
          const staticData = await snapshot.quads.pipe(toArray()).toPromise();
          await ssd.applySnapshot({
            lastTime: localTime,
            quads: from(staticData)
          }, localTime = localTime.ticked());
          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }).pipe(toArray()).toPromise()).resolves.toEqual([fred]);
        });

        test('transacts a delete', async () => {
          const willUpdate = captureUpdate();

          const msg = await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.write({ '@delete': { '@id': 'http://test.m-ld.org/fred' } })
          ]) ?? fail();
          expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);

          expect(msg.time.equals(localTime)).toBe(true);
          const [_, fused, del, ins] = msg.data;

          expect(fused).toBe(false);
          expect(await EncodedDelta.decode(ins)).toEqual({});
          expect(await EncodedDelta.decode(del)).toMatchObject({
            'tid': firstTid,
            's': 'fred',
            'p': '#name',
            'o': 'Fred'
          });
        });

        test('applies a delete delta', async () => {
          const willUpdate = captureUpdate();

          await ssd.apply(new DeltaMessage(
            remoteTime.ticks,
            remoteTime = remoteTime.ticked(),
            // Deleting the triple based on the inserted Transaction ID
            [2, false, `{"tid":"${firstTid}","o":"Fred","p":"#name", "s":"fred"}`, '{}']),
            localTime = localTime.update(remoteTime).ticked(),
            localTime = localTime.ticked());
          expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);

          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }).pipe(toArray()).toPromise()).resolves.toEqual([]);
        });

        test('transacts another insert', async () => {
          const msg = await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.write({ '@insert': barney })
          ]) ?? fail();
          expect(msg.time.equals(localTime)).toBe(true);

          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/barney'
          }).pipe(toArray()).toPromise()).resolves.toEqual([barney]);
        });

        test('answers local op since first', async () => {
          // Remote knows about first entry
          remoteTime = remoteTime.update(localTime);
          // Create a new journal entry that the remote doesn't know
          await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.write({ '@insert': barney })
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
          let thirdTime = forkLocal.right;
          await ssd.apply(new DeltaMessage(
            thirdTime.ticks,
            thirdTime = thirdTime.ticked(),
            [2, false, `{"tid":"${firstTid}","o":"Fred","p":"#name","s":"fred"}`, '{}']),
            localTime = localTime.update(thirdTime).ticked(),
            localTime = localTime.ticked());

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(thirdTime.equals(opArray[0].time)).toBe(true);
        });

        test('answers missed local op', async () => {
          remoteTime = remoteTime.update(localTime);
          // New entry that the remote hasn't seen
          const localOp = await ssd.transact(async () => [
            localTime = localTime.ticked(),
            await ssd.write({ '@insert': barney })
          ]) ?? fail();
          // Don't update remote time from local
          await ssd.apply(new DeltaMessage(
            remoteTime.ticks,
            remoteTime = remoteTime.ticked(),
            remoteInsert(wilma)),
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
          await ssd.apply(new DeltaMessage(
            thirdTime.ticks,
            thirdTime = thirdTime.ticked(),
            thirdOp),
            localTime = localTime.update(thirdTime).ticked(),
            localTime = localTime.ticked());
          // Remote does see fourth party op
          await ssd.apply(new DeltaMessage(
            fourthTime.ticks,
            fourthTime = fourthTime.ticked(),
            remoteInsert(barney)),
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
          await ssd.apply(new DeltaMessage(
            localTime.ticks,
            localTime = localTime.ticked(),
            [2, false, '{}', '{"@id":"wilma","name":"Wilma"}']),
            localTime,
            localTime = localTime.ticked());

          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/wilma'
          }).pipe(toArray()).toPromise()).resolves.toEqual([wilma]);
        });

        // @see https://github.com/m-ld/m-ld-js/issues/29
        test('answers unpersisted remote op', async () => {
          // Remote knows about first entry
          remoteTime = remoteTime.update(localTime);
          // Create a remote entry that the remote fails to persist fully
          const unpersistedTime = remoteTime.ticked();
          await ssd.apply(new DeltaMessage(
            remoteTime.ticks,
            unpersistedTime,
            [2, false, `{"tid":"${firstTid}","o":"Fred","p":"#name","s":"fred"}`, '{}']),
            localTime = localTime.update(unpersistedTime).ticked(),
            localTime = localTime.ticked());

          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(unpersistedTime.equals(opArray[0].time)).toBe(true);
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
        apply: () => Promise.resolve()
      };
      ssd = new SuSetDataset(await memStore(), {}, [constraint],
        { '@id': 'test', '@domain': 'test.m-ld.org' });
      await ssd.initialise();
      await ssd.saveClock(() => localTime = localTime.ticked(), true);
    });

    test('checks the constraint', async () => {
      constraint.check = () => Promise.reject('Failed!');
      await expect(ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.write({ '@insert': fred })
      ])).rejects.toBe('Failed!');
    });

    test('provides state to the constraint', async () => {
      await ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.write({ '@insert': wilma })
      ]);
      constraint.check = async state =>
        state.read<Describe>({ '@describe': 'http://test.m-ld.org/wilma' }).toPromise().then(wilma => {
          if (wilma == null)
            throw 'not found!';
        });
      await expect(ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.write({ '@insert': fred })
      ])).resolves.toBeDefined();
    });

    test('provides a mutable update to the constraint', async () => {
      constraint.check = async (_, interim) => {
        interim.assert({ '@insert': wilma });
        let update = await interim.update;
        expect(update['@insert']).toMatchObject(expect.arrayContaining([wilma]));
        interim.assert({ '@insert': barney });
        update = await interim.update;
        expect(update['@insert']).toMatchObject(expect.arrayContaining([wilma, barney]));
      };
      await expect(ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.write({ '@insert': fred })
      ])).resolves.toBeDefined();
      await expect(ssd.read(<Describe>{ '@describe': wilma['@id'] })
        .pipe(take(1)).toPromise()).resolves.toEqual(wilma);
    });

    test('applies an inserting constraint', async () => {
      constraint.apply = async (_, update) => update.assert({ '@insert': wilma });
      const willUpdate = captureUpdate();
      const msg = await ssd.apply(new DeltaMessage(
        remoteTime.ticks,
        remoteTime = remoteTime.update(localTime).ticked(),
        [2, false, '{}', '{"@id":"fred","name":"Fred"}']),
        localTime = localTime.update(remoteTime).ticked(),
        localTime = localTime.ticked());
      expect(willUpdate).resolves.toEqual(
        { '@delete': [], '@insert': [fred, wilma], '@ticks': localTime.ticks });

      expect(msg).not.toBeNull();
      if (msg != null) {
        expect(msg.time.equals(localTime)).toBe(true);
        const [, fused, del, ins] = msg.data;
        expect(fused).toBe(false);
        expect(del).toBeTruthy();
      }
      await expect(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }).pipe(toArray()).toPromise()).resolves.toEqual([wilma]);

      expect((<TreeClock>await ssd.loadClock()).equals(localTime)).toBe(true);

      // Check that we have a valid journal
      const ops = await ssd.operationsSince(remoteTime);
      if (ops == null)
        fail();
      const entries = await ops.pipe(toArray()).toPromise();
      expect(entries.length).toBe(1);
      expect(entries[0].time.equals(localTime)).toBe(true);
      const [, fused, del, ins] = entries[0].data;
      expect(fused).toBe(false);
      expect(del).toBe('{}');
      expect(ins).toBe('{\"@id\":\"wilma\",\"name\":\"Wilma\"}');
    });

    test('applies a deleting constraint', async () => {
      constraint.apply = async (_, update) => update.assert({ '@delete': wilma });

      await ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.write({ '@insert': wilma })
      ]);

      const willUpdate = captureUpdate();
      await ssd.apply(new DeltaMessage(
        remoteTime.ticks,
        remoteTime = remoteTime.ticked(),
        [2, false, '{}', '{"@id":"fred","name":"Fred"}']),
        localTime = localTime.update(remoteTime).ticked(),
        localTime = localTime.ticked());
      expect(willUpdate).resolves.toEqual(
        { '@insert': [fred], '@delete': [wilma], '@ticks': localTime.ticks });

      await expect(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }).pipe(toArray()).toPromise()).resolves.toEqual([]);
    });

    test('applies a self-deleting constraint', async () => {
      // Constraint is going to delete the data we're inserting
      constraint.apply = async (_, update) => update.assert({ '@delete': wilma });

      const willUpdate = captureUpdate();
      await ssd.apply(new DeltaMessage(
        remoteTime.ticks,
        remoteTime = remoteTime.ticked(),
        [2, false, '{}', '{"@id":"wilma","name":"Wilma"}']),
        localTime = localTime.update(remoteTime).ticked(),
        localTime = localTime.ticked());

      await expect(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }).pipe(toArray()).toPromise()).resolves.toEqual([]);

      // The inserted data was deleted, but Wilma may have existed before
      await expect(willUpdate).resolves.toEqual(
        { '@insert': [], '@delete': [wilma], '@ticks': localTime.ticks });
    });

    test('applies a self-inserting constraint', async () => {
      // Constraint is going to insert the data we're deleting
      constraint.apply = async (_, update) => update.assert({ '@insert': wilma });

      const tid = (await ssd.transact(async () => [
        localTime = localTime.ticked(),
        await ssd.write({ '@insert': wilma })
      ]) ?? fail()).data[1];

      const willUpdate = captureUpdate();
      await ssd.apply(new DeltaMessage(
        remoteTime.ticks,
        remoteTime = remoteTime.ticked(),
        [2, false, `{"tid":"${tid}","o":"Wilma","p":"#name", "s":"wilma"}`, '{}']),
        localTime = localTime.update(remoteTime).ticked(),
        localTime = localTime.ticked());

      await expect(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }).pipe(toArray()).toPromise()).resolves.toEqual([wilma]);

      // The deleted data was re-inserted, but Wilma may not have existed before
      await expect(willUpdate).resolves.toEqual(
        { '@insert': [wilma], '@delete': [], '@ticks': localTime.ticks });
    });
  });

  test('enforces delta size limit', async () => {
    ssd = new SuSetDataset(await memStore(), {}, [],
      { '@id': 'test', '@domain': 'test.m-ld.org', maxDeltaSize: 1 });
    await ssd.initialise();
    await ssd.saveClock(() => TreeClock.GENESIS, true);
    await expect(ssd.transact(async () => [
      TreeClock.GENESIS.ticked(),
      await ssd.write({ '@insert': fred })
    ])).rejects.toThrow();
  });
});

function remoteInsert(subject: Subject): EncodedDelta {
  return [2, false, '{}', JSON.stringify(subject)];
}
