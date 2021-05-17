import { SuSetDataset } from '../src/engine/dataset/SuSetDataset';
import { memStore, MockProcess } from './testClones';
import { TreeClock } from '../src/engine/clocks';
import { first, toArray, isEmpty, take } from 'rxjs/operators';
import { Dataset } from '../src/engine/dataset';
import { from } from 'rxjs';
import { Describe, MeldConstraint } from '../src';
import { jsonify } from './testUtil';
import { SubjectGraph } from '../src/engine/SubjectGraph';
import { MeldEncoder } from '../src/engine/MeldEncoding';
import { OperationMessage } from '../src/engine';

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
      let local: MockProcess, remote: MockProcess;

      beforeEach(async () => {
        let { left, right } = TreeClock.GENESIS.forked();
        local = new MockProcess(left);
        remote = new MockProcess(right);
        await ssd.saveClock(() => local.tick().time, true);
      });

      test('does not answer operations since before start', async () => {
        await expect(ssd.operationsSince(remote.time)).resolves.toBeUndefined();
      });

      test('has no operations since first time', async () => {
        const ops = await ssd.operationsSince(local.time);
        await expect(ops && ops.pipe(isEmpty()).toPromise()).resolves.toBe(true);
      });

      test('answers the time', async () => {
        const savedTime = await ssd.loadClock();
        expect(savedTime && savedTime.equals(local.time)).toBe(true);
      });

      test('answers an empty snapshot', async () => {
        const snapshot = await ssd.takeSnapshot();
        expect(snapshot.lastTime.equals(local.time.scrubId())).toBe(true);
        await expect(snapshot.quads.toPromise()).resolves.toBeUndefined();
      });

      test('transacts a no-op', async () => {
        const willUpdate = captureUpdate();
        const msg = await ssd.transact(async () => [
          local.tick().time,
          await ssd.write({ '@insert': [] })
        ]);
        expect(msg).toBeNull();
        await expect(Promise.race([willUpdate, Promise.resolve()]))
          .resolves.toBeUndefined();
      });

      test('transacts an insert', async () => {
        const willUpdate = captureUpdate();

        const msg = await ssd.transact(async () => [
          local.tick().time,
          await ssd.write({ '@insert': fred })
        ]) ?? fail();
        // The update should happen in-transaction, so no 'await' here
        expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);

        expect(msg.time.equals(local.time)).toBe(true);
        const [ver, from, time, del, ins] = msg.data;
        expect(ver).toBe(2);

        expect(from).toBe(local.time.ticks);
        expect(local.time.equals(TreeClock.fromJson(time) as TreeClock)).toBe(true);
        expect(MeldEncoder.jsonFromBuffer(ins))
          .toEqual({ '@id': 'fred', 'name': 'Fred' });
        expect(MeldEncoder.jsonFromBuffer(del)).toEqual({});
      });

      test('applies an insert operation', async () => {
        const willUpdate = captureUpdate();

        await ssd.apply(
          remote.sentOperation('{}', '{"@id":"fred","name":"Fred"}'),
          local.join(remote).tick().time,
          local.tick().time);
        await expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);

        await expect(ssd.read<Describe>({
          '@describe': 'http://test.m-ld.org/fred'
        }).pipe(toArray()).toPromise()).resolves.toEqual([fred]);
      });

      test('applies a no-op operation', async () => {
        const willUpdate = captureUpdate();

        const msg = await ssd.apply(remote.sentOperation('{}', '{}'),
          local.join(remote).tick().time,
          local.tick().time);

        expect(msg).toBeNull();
        await expect(Promise.race([willUpdate, Promise.resolve()]))
          .resolves.toBeUndefined();
      });

      describe('with an initial triple', () => {
        let firstTid: string;

        beforeEach(async () => {
          firstTid = (await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': fred })
          ]) ?? fail()).time.hash();
        });

        test('answers the new time', async () => {
          const newTime = await ssd.loadClock();
          expect(newTime && newTime.equals(local.time)).toBe(true);
        });

        test('answers a snapshot', async () => {
          const snapshot = await ssd.takeSnapshot();
          expect(snapshot.lastTime.equals(local.time.scrubId())).toBe(true);
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
            lastTime: local.time,
            quads: from(staticData)
          }, local.tick().time);
          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }).pipe(toArray()).toPromise()).resolves.toEqual([fred]);
        });

        test('transacts a delete', async () => {
          const willUpdate = captureUpdate();

          const msg = await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@delete': { '@id': 'http://test.m-ld.org/fred' } })
          ]) ?? fail();
          expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);

          expect(msg.time.equals(local.time)).toBe(true);
          const [, , , del, ins] = msg.data;

          expect(MeldEncoder.jsonFromBuffer(ins)).toEqual({});
          expect(MeldEncoder.jsonFromBuffer(del)).toMatchObject({
            'tid': firstTid,
            's': 'fred',
            'p': '#name',
            'o': 'Fred'
          });
        });

        test('applies a delete operation', async () => {
          const willUpdate = captureUpdate();

          await ssd.apply(
            remote.sentOperation(`{"tid":"${firstTid}","o":"Fred","p":"#name", "s":"fred"}`, '{}'),
            local.join(remote).tick().time,
            local.tick().time);
          expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);

          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }).pipe(toArray()).toPromise()).resolves.toEqual([]);
        });

        test('transacts another insert', async () => {
          const msg = await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': barney })
          ]) ?? fail();
          expect(msg.time.equals(local.time)).toBe(true);

          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/barney'
          }).pipe(toArray()).toPromise()).resolves.toEqual([barney]);
        });

        test('answers local op since first', async () => {
          // Remote knows about first entry
          remote.join(local);
          // Create a new journal entry that the remote doesn't know
          await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': barney })
          ]);
          const ops = await ssd.operationsSince(remote.time);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(local.time.equals(opArray[0].time)).toBe(true);
        });

        test('answers remote op since first', async () => {
          // Remote knows about first entry
          remote.join(local);
          // Create a remote entry from a third clone that the remote doesn't know
          let thirdClock = local.fork();
          await ssd.apply(
            thirdClock.sentOperation(`{"tid":"${firstTid}","o":"Fred","p":"#name","s":"fred"}`, '{}'),
            local.join(thirdClock).tick().time,
            local.tick().time);

          const ops = await ssd.operationsSince(remote.time);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(thirdClock.time.equals(opArray[0].time)).toBe(true);
        });

        test('answers missed local op', async () => {
          remote.join(local);
          // New entry that the remote hasn't seen
          const localOp = await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': barney })
          ]) ?? fail();
          // Don't update remote time from local
          await ssd.apply(
            remote.sentOperation('{}', JSON.stringify(wilma)),
            local.join(remote).tick().time,
            local.tick().time);

          const ops = await ssd.operationsSince(remote.time);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          // We expect the missed local op (barney) but not the remote op
          // (wilma), because it should be filtered out
          expect(opArray.length).toBe(1);
          expect(opArray[0].time.equals(localOp.time)).toBe(true);
        });

        test('answers missed third party op', async () => {
          remote.join(local);
          const third = remote.fork();
          const fourth = third.fork();
          // Remote doesn't see third party op
          const thirdOp = third.sentOperation('{}', JSON.stringify(wilma));
          await ssd.apply(thirdOp,
            local.join(third).tick().time,
            local.tick().time);
          // Remote does see fourth party op
          await ssd.apply(fourth.sentOperation('{}', JSON.stringify(barney)),
            local.join(fourth).tick().time,
            local.tick().time);
          remote.join(fourth).tick();

          const ops = await ssd.operationsSince(remote.time);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          // We expect only the missed remote op
          expect(opArray.length).toBe(1);
          expect(opArray[0].data).toEqual(thirdOp.data);
        });

        // @see https://github.com/m-ld/m-ld-js/issues/29
        test('accepts own unpersisted update', async () => {
          await ssd.apply(local.sentOperation(
            '{}', '{"@id":"wilma","name":"Wilma"}'),
            local.time,
            local.tick().time);

          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/wilma'
          }).pipe(toArray()).toPromise()).resolves.toEqual([wilma]);
        });

        // @see https://github.com/m-ld/m-ld-js/issues/29
        test('answers unpersisted remote op', async () => {
          // Remote knows about first entry
          remote.join(local);
          const remoteTime = remote.time;
          // Create a remote entry that the remote fails to persist fully
          await ssd.apply(
            remote.sentOperation(`{"tid":"${firstTid}","o":"Fred","p":"#name","s":"fred"}`, '{}'),
            local.join(remote).tick().time,
            local.tick().time);
          // The remote asks for its previous time
          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await ops.pipe(toArray()).toPromise() : [];
          expect(opArray.length).toBe(1);
          expect(remote.time.equals(opArray[0].time)).toBe(true);
        });

        test('cuts stale from incoming fusion', async () => {
          const third = remote.fork();
          // The remote will have two transactions, which fuse in its journal.
          // The local sees the first, adding wilma...
          const one = remote.sentOperation('{}', '{"@id":"wilma","name":"Wilma"}');
          const oneTid = one.time.hash();
          await ssd.apply(one, local.time, local.tick().time);
          // ... and then get a third-party txn, deleting wilma
          await ssd.apply(third.sentOperation(
            `{"tid":"${oneTid}","o":"Wilma","p":"#name","s":"wilma"}`, '{}'),
            local.time, local.tick().time);
          // Finally the local gets the remote fusion (as a rev-up), which still
          // includes the insert of wilma
          remote.tick();
          await ssd.apply(new OperationMessage(one.time.ticks, // Previous tick of remote
            [2, one.time.ticks, remote.time.toJson(), '{}', `[
              {"tid":"${oneTid}","o":"Wilma","p":"#name","s":"wilma"},
              {"tid":"${remote.time.hash()}","o":"Barney","p":"#name","s":"barney"}]`]),
            local.time, local.tick().time);
          // Result should not include wilma
          await expect(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/wilma'
          }).pipe(toArray()).toPromise()).resolves.toEqual([]);
        });
      });
    });
  });

  describe('with a constraint', () => {
    let local: MockProcess, remote: MockProcess;
    let constraint: MeldConstraint;

    beforeEach(async () => {
      let { left, right } = TreeClock.GENESIS.forked();
      local = new MockProcess(left);
      remote = new MockProcess(right);
      constraint = {
        check: () => Promise.resolve(),
        apply: () => Promise.resolve()
      };
      ssd = new SuSetDataset(await memStore(), {}, [constraint],
        { '@id': 'test', '@domain': 'test.m-ld.org' });
      await ssd.initialise();
      await ssd.saveClock(() => local.tick().time, true);
    });

    test('checks the constraint', async () => {
      constraint.check = () => Promise.reject('Failed!');
      await expect(ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred })
      ])).rejects.toBe('Failed!');
    });

    test('provides state to the constraint', async () => {
      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': wilma })
      ]);
      constraint.check = async state =>
        state.read<Describe>({ '@describe': 'http://test.m-ld.org/wilma' }).toPromise().then(wilma => {
          if (wilma == null)
            throw 'not found!';
        });
      await expect(ssd.transact(async () => [
        local.tick().time,
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
        local.tick().time,
        await ssd.write({ '@insert': fred })
      ])).resolves.toBeDefined();
      await expect(ssd.read(<Describe>{ '@describe': wilma['@id'] })
        .pipe(take(1)).toPromise()).resolves.toEqual(wilma);
    });

    test('applies an inserting constraint', async () => {
      constraint.apply = async (_, update) => update.assert({ '@insert': wilma });
      const willUpdate = captureUpdate();
      remote.join(local);
      const msg = await ssd.apply(
        remote.sentOperation('{}', '{"@id":"fred","name":"Fred"}'),
        local.join(remote).tick().time,
        local.tick().time);
      expect(willUpdate).resolves.toEqual(
        { '@delete': [], '@insert': [fred, wilma], '@ticks': local.time.ticks });

      expect(msg).not.toBeNull();
      if (msg != null) {
        expect(msg.time.equals(local.time)).toBe(true);
        const [, from, time, del, ins] = msg.data;
        expect(from).toBe(TreeClock.fromJson(time)?.ticks);
        expect(del).toBe('{}');
        expect(ins).toBe('{"@id":"wilma","name":"Wilma"}');
      }
      await expect(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }).pipe(toArray()).toPromise()).resolves.toEqual([wilma]);

      const newLocalTime = (<TreeClock>await ssd.loadClock());
      expect(newLocalTime.equals(local.time)).toBe(true);

      // Check that we have a valid journal
      const ops = await ssd.operationsSince(remote.time);
      if (ops == null)
        fail();
      const entries = await ops.pipe(toArray()).toPromise();
      expect(entries.length).toBe(1);
      expect(entries[0].time.equals(local.time)).toBe(true);
      const [, from, time, del, ins] = entries[0].data;
      expect(from).toBe(TreeClock.fromJson(time)?.ticks);
      expect(del).toBe('{}');
      expect(ins).toBe('{\"@id\":\"wilma\",\"name\":\"Wilma\"}');
    });

    test('applies a deleting constraint', async () => {
      constraint.apply = async (_, update) => update.assert({ '@delete': wilma });

      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': wilma })
      ]);

      const willUpdate = captureUpdate();
      await ssd.apply(
        remote.sentOperation('{}', '{"@id":"fred","name":"Fred"}'),
        local.join(remote).tick().time,
        local.tick().time);
      expect(willUpdate).resolves.toEqual(
        { '@insert': [fred], '@delete': [wilma], '@ticks': local.time.ticks });

      await expect(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }).pipe(toArray()).toPromise()).resolves.toEqual([]);
    });

    test('applies a self-deleting constraint', async () => {
      // Constraint is going to delete the data we're inserting
      constraint.apply = async (_, update) => update.assert({ '@delete': wilma });

      const willUpdate = captureUpdate();
      await ssd.apply(
        remote.sentOperation('{}', '{"@id":"wilma","name":"Wilma"}'),
        local.join(remote).tick().time,
        local.tick().time);

      await expect(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }).pipe(toArray()).toPromise()).resolves.toEqual([]);

      // The inserted data was deleted, but Wilma may have existed before
      await expect(willUpdate).resolves.toEqual(
        { '@insert': [], '@delete': [wilma], '@ticks': local.time.ticks });
    });

    test('applies a self-inserting constraint', async () => {
      // Constraint is going to insert the data we're deleting
      constraint.apply = async (_, update) => update.assert({ '@insert': wilma });

      const tid = (await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': wilma })
      ]) ?? fail()).data[1];

      const willUpdate = captureUpdate();
      await ssd.apply(
        remote.sentOperation(`{"tid":"${tid}","o":"Wilma","p":"#name", "s":"wilma"}`, '{}'),
        local.join(remote).tick().time,
        local.tick().time);

      await expect(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }).pipe(toArray()).toPromise()).resolves.toEqual([wilma]);

      // The deleted data was re-inserted, but Wilma may not have existed before
      await expect(willUpdate).resolves.toEqual(
        { '@insert': [wilma], '@delete': [], '@ticks': local.time.ticks });
    });
  });

  test('enforces operation size limit', async () => {
    ssd = new SuSetDataset(await memStore(), {}, [],
      { '@id': 'test', '@domain': 'test.m-ld.org', maxOperationSize: 1 });
    await ssd.initialise();
    await ssd.saveClock(() => TreeClock.GENESIS, true);
    await expect(ssd.transact(async () => [
      TreeClock.GENESIS.ticked(),
      await ssd.write({ '@insert': fred })
    ])).rejects.toThrow();
  });
});