import { SuSetDataset } from '../src/engine/dataset/SuSetDataset';
import { MockProcess, MockState, testOp } from './testClones';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { toArray } from 'rxjs/operators';
import { EmptyError, firstValueFrom, lastValueFrom, Subject } from 'rxjs';
import {
  AgreementCondition, Describe, JournalCheckPoint, MeldConstraint, MeldUpdate
} from '../src';
import { jsonify } from './testUtil';
import { MeldEncoder } from '../src/engine/MeldEncoding';
import { BufferEncoding, EncodedOperation, OperationMessage } from '../src/engine';
import { drain } from 'rx-flowable';
import { MeldError } from '../src/engine/MeldError';
import { mockFn } from 'jest-mock-extended';

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
  let state: MockState;
  let ssd: SuSetDataset;

  beforeEach(async () => {
    state = await MockState.create();
  });

  afterEach(() => state.close());

  function captureUpdate(): Promise<MeldUpdate> {
    // Convert the subject graphs to JSON for matching convenience
    return firstValueFrom(ssd.updates).then(jsonify);
  }

  function expectNoUpdate(willUpdate: Promise<MeldUpdate>) {
    return expect(Promise.race([
      // Give it a tick, just in case the update is async
      willUpdate, new Promise(resolve => setImmediate(resolve))]
    )).resolves.toBeUndefined();
  }

  describe('with basic config', () => {
    beforeEach(async () => {
      ssd = new SuSetDataset(state.dataset, {}, {}, {}, {
        '@id': 'test',
        '@domain': 'test.m-ld.org'
      });
      await ssd.initialise();
      await ssd.allowTransact();
    });

    test('does not have a time', async () => {
      expect(await ssd.loadClock()).toBeUndefined();
    });

    describe('with an initial time', () => {
      let local: MockProcess, remote: MockProcess;

      beforeEach(async () => {
        let { left, right } = TreeClock.GENESIS.forked();
        local = new MockProcess(left);
        remote = new MockProcess(right);
        await ssd.resetClock(local.time);
      });

      test('has no operations since first time', async () => {
        const ops = await ssd.operationsSince(local.time);
        await expect(firstValueFrom(ops!)).rejects.toBeInstanceOf(EmptyError);
      });

      test('answers the time', async () => {
        const savedTime = await ssd.loadClock();
        expect(savedTime!.equals(local.time)).toBe(true);
      });

      test('answers an empty snapshot', async () => {
        const snapshot = await ssd.takeSnapshot();
        // Test artifact: resetClock sets GWC to genesis – always set to a snapshot in prod
        expect(snapshot.gwc.equals(GlobalClock.GENESIS)).toBe(true);
        await expect(lastValueFrom(snapshot.data)).rejects.toBeInstanceOf(EmptyError);
      });

      test('transacts a no-op', async () => {
        const willUpdate = captureUpdate();
        const msg = await ssd.transact(async () => [
          local.tick().time,
          await ssd.write({ '@insert': [] })
        ]);
        expect(msg).toBeNull();
        await expectNoUpdate(willUpdate);
      });

      test('transacts an insert', async () => {
        const willUpdate = captureUpdate();

        const msg = await ssd.transact(async () => [
          local.tick().time,
          await ssd.write({ '@insert': fred })
        ]);
        // The update should happen in-transaction, so no 'await' here
        await expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);

        expect(msg!.time.equals(local.time)).toBe(true);
        const [ver, from, time, upd, enc] = msg!.data;
        expect(ver).toBe(4);

        expect(from).toBe(local.time.ticks);
        expect(local.time.equals(TreeClock.fromJson(time) as TreeClock)).toBe(true);
        expect(MeldEncoder.jsonFromBuffer(upd, enc))
          .toEqual([{}, { '@id': 'fred', 'name': 'Fred' }]);
      });

      test('applies an insert operation', async () => {
        const willUpdate = captureUpdate();

        await ssd.apply(
          remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }),
          local.join(remote.time));
        await expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);

        await expect(drain(ssd.read<Describe>({
          '@describe': 'http://test.m-ld.org/fred'
        }))).resolves.toEqual([fred]);
      });

      test('applies a no-op operation', async () => {
        const willUpdate = captureUpdate();

        const msg = await ssd.apply(remote.sentOperation({}, {}),
          local.join(remote.time));

        expect(msg).toBeNull();
        await expectNoUpdate(willUpdate);
      });

      describe('with an initial triple', () => {
        let firstTid: string;

        beforeEach(async () => {
          firstTid = (await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': fred })
          ]))!.time.hash;
        });

        test('answers the new time', async () => {
          const newTime = await ssd.loadClock();
          expect(newTime && newTime.equals(local.time)).toBe(true);
        });

        test('answers a snapshot', async () => {
          const snapshot = await ssd.takeSnapshot();
          expect(snapshot.gwc.equals(local.gwc)).toBe(true);
          const data = await firstValueFrom(snapshot.data.pipe(toArray()));
          expect(data.length).toBe(2);
          const reifiedFredJson = new RegExp(
            `(,?("s":"fred"|"p":"#name"|"o":"Fred"|"tid":"${firstTid}")){4}`);
          expect(data.every(v => {
            if ('operation' in v) {
              const [ver, from, time, upd, enc] = v.operation;
              expect(ver).toBe(4);
              expect(from).toBe(local.time.ticks);
              expect(TreeClock.fromJson(time).equals(local.time)).toBe(true);
              expect(MeldEncoder.jsonFromBuffer(upd, enc))
                .toMatchObject([{}, { '@id': 'fred', 'name': 'Fred' }]);
              return true;
            } else if ('inserts' in v) {
              expect(JSON.stringify(MeldEncoder.jsonFromBuffer(v.inserts, v.encoding)))
                .toMatch(reifiedFredJson);
              return true;
            }
          })).toBe(true);
        });

        test('answers a snapshot with fused last operations', async () => {
          const firstTick = local.time.ticks;
          await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': wilma })
          ]);
          const snapshot = await ssd.takeSnapshot();
          expect(snapshot.gwc.equals(local.gwc)).toBe(true);
          const data = await firstValueFrom(snapshot.data.pipe(toArray()));
          expect(data.length).toBe(2);
          // noinspection LongLine
          const reifiedFlintstoneJson = new RegExp( // Not a great check but must have all properties
            `(.+("s":"fred"|"s":"wilma"|"p":"#name"|"o":"Fred"|"o":"Wilma"|"tid":"${firstTid}|"tid":"${local.time.hash}")){8}`);
          expect(data.every(v => {
            if ('operation' in v) {
              const [ver, from, time, upd, enc] = v.operation;
              expect(ver).toBe(4);
              expect(from).toBe(firstTick);
              expect(TreeClock.fromJson(time).equals(local.time)).toBe(true);
              const [del, ins] = MeldEncoder.jsonFromBuffer(upd, enc);
              expect(del).toEqual({});
              expect(JSON.stringify(ins)).toMatch(reifiedFlintstoneJson);
              return true;
            } else if ('inserts' in v) {
              expect(JSON.stringify(MeldEncoder.jsonFromBuffer(v.inserts, v.encoding)))
                .toMatch(reifiedFlintstoneJson);
              expect(v.encoding).toEqual([BufferEncoding.MSGPACK]);
              return true;
            }
          })).toBe(true);
        });

        test('snapshot includes entailed triples', async () => {
          // Prepare some entailed information which does not have any
          // transaction identity. Using the dataset this way is a back door and
          // may be brittle if the way that entailments work changes.
          await state.write(() => ssd.write({
            '@insert': {
              '@id': 'http://test.m-ld.org/fred',
              'http://test.m-ld.org/#sex': 'male'
            }
          }));
          const snapshot = await ssd.takeSnapshot();
          const data = await firstValueFrom(snapshot.data.pipe(toArray()));
          expect(data.length).toBe(2);
          const reifiedSexJson = new RegExp( // Not a great check but must have all properties
            `(.+("s":"fred"|"p":("#name"|"#sex")|"o":("Fred"|"male")|"tid":"${firstTid}")){7}`);
          expect(data.every(v => {
            if ('operation' in v) {
              const [ver, from, time, upd, enc] = v.operation;
              expect(ver).toBe(4);
              expect(from).toBe(local.time.ticks);
              expect(TreeClock.fromJson(time).equals(local.time)).toBe(true);
              expect(MeldEncoder.jsonFromBuffer(upd, enc))
                .toMatchObject([{}, { '@id': 'fred', 'name': 'Fred' }]);
              return true;
            } else if ('inserts' in v) {
              expect(JSON.stringify(MeldEncoder.jsonFromBuffer(v.inserts, v.encoding)))
                .toMatch(reifiedSexJson);
              expect(v.encoding).toEqual([BufferEncoding.MSGPACK]);
              return true;
            }
          })).toBe(true);
        });

        test('applies a snapshot', async () => {
          const snapshot = await ssd.takeSnapshot();
          const staticData = await firstValueFrom(snapshot.data.pipe(toArray()));
          await ssd.applySnapshot(local.snapshot(staticData), local.tick().time);
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }))).resolves.toEqual([fred]);
        });

        test('does not answer operations since before snapshot start', async () => {
          const snapshot = await ssd.takeSnapshot();
          const staticData = await firstValueFrom(snapshot.data.pipe(toArray()));
          await ssd.applySnapshot(local.snapshot(staticData), local.tick().time);
          await expect(ssd.operationsSince(remote.time)).resolves.toBeUndefined();
        });

        test('transacts a delete', async () => {
          const willUpdate = captureUpdate();

          const msg = await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@delete': { '@id': 'http://test.m-ld.org/fred' } })
          ]);
          await expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);

          expect(msg!.time.equals(local.time)).toBe(true);
          const [, , , upd, enc] = msg!.data;

          expect(MeldEncoder.jsonFromBuffer(upd, enc)).toMatchObject([{
            'tid': firstTid,
            's': 'fred',
            'p': '#name',
            'o': 'Fred'
          }, {}]);
        });

        test('applies a delete operation', async () => {
          const willUpdate = captureUpdate();

          await ssd.apply(
            remote.sentOperation({ 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' }, {}),
            local.join(remote.time));
          await expect(willUpdate).resolves.toHaveProperty('@delete', [fred]);

          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }))).resolves.toEqual([]);
        });

        test('applies a concurrent delete', async () => {
          // Insert Fred again, with Wilma and a new TID
          const remoteOp = remote.sentOperation({},
            [{ '@id': 'fred', 'name': 'Fred' }, { '@id': 'wilma', 'name': 'Wilma' }]);
          await ssd.apply(remoteOp, local.join(remote.time));
          // Delete the remotely-inserted Flintstones
          const willUpdate = captureUpdate();
          await ssd.apply(
            remote.sentOperation([
              { 'tid': remoteOp.time.hash, 'o': 'Fred', 'p': '#name', 's': 'fred' },
              { 'tid': remoteOp.time.hash, 'o': 'Wilma', 'p': '#name', 's': 'wilma' }
            ], {}),
            local.join(remote.time));
          // Expect Fred to still exist and Wilma to be deleted
          await expect(willUpdate).resolves.toHaveProperty('@delete', [wilma]);
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }))).resolves.toEqual([fred]);
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/wilma'
          }))).resolves.toEqual([]);
          // Delete the locally-inserted Fred
          await ssd.apply(
            remote.sentOperation({ 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' }, {}),
            local.join(remote.time));
          // Expect Fred to be deleted
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }))).resolves.toEqual([]);
        });

        test('transacts another insert', async () => {
          const msg = await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': barney })
          ]);
          expect(msg!.time.equals(local.time)).toBe(true);

          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/barney'
          }))).resolves.toEqual([barney]);
        });

        test('transacts a duplicating insert', async () => {
          const willUpdate = captureUpdate();
          const moreFred = { ...fred, height: 6 };
          await ssd.transact(async () => [
            local.tick().time,
            await ssd.write(moreFred)
          ]);
          // The update includes the repeat value
          await expect(willUpdate).resolves.toHaveProperty('@insert', [moreFred]);
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }))).resolves.toEqual([moreFred]);
          // Pretend the remotes have seen only the first Fred name, and delete it
          await ssd.apply(
            remote.sentOperation({ 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' }, {}),
            local.join(remote.time));
          // Expect the second Fred name to persist
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }))).resolves.toEqual([moreFred]);
        });

        test('answers local op since first', async () => {
          // Remote knows about first entry
          remote.join(local.time);
          // Create a new journal entry that the remote doesn't know
          await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': barney })
          ]);
          const ops = await ssd.operationsSince(remote.time);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await firstValueFrom(ops.pipe(toArray())) : [];
          expect(opArray.length).toBe(1);
          expect(local.time.equals(opArray[0].time)).toBe(true);
        });

        test('answers remote op since first', async () => {
          // Remote knows about first entry
          remote.join(local.time);
          // Create a remote entry from a third clone that the remote doesn't know
          let thirdClock = local.fork();
          await ssd.apply(
            thirdClock.sentOperation({
              'tid': firstTid,
              'o': 'Fred',
              'p': '#name',
              's': 'fred'
            }, {}),
            local.join(thirdClock.time));

          const ops = await ssd.operationsSince(remote.time);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await firstValueFrom(ops.pipe(toArray())) : [];
          expect(opArray.length).toBe(1);
          expect(thirdClock.time.equals(opArray[0].time)).toBe(true);
        });

        test('answers missed local op', async () => {
          remote.join(local.time);
          // New entry that the remote hasn't seen
          const localOp = await ssd.transact(async () => [
            local.tick().time,
            await ssd.write({ '@insert': barney })
          ]);
          // Don't update remote time from local
          await ssd.apply(
            remote.sentOperation({}, wilma),
            local.join(remote.time));

          const ops = await ssd.operationsSince(remote.time);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await firstValueFrom(ops.pipe(toArray())) : [];
          // We expect the missed local op (barney) but not the remote op
          // (wilma), because it should be filtered out
          expect(opArray.length).toBe(1);
          expect(opArray[0].time.equals(localOp!.time)).toBe(true);
        });

        test('answers missed third party op', async () => {
          remote.join(local.time);
          const third = remote.fork();
          const fourth = third.fork();
          // Remote doesn't see third party op
          const thirdOp = third.sentOperation({}, wilma);
          await ssd.apply(thirdOp,
            local.join(third.time));
          // Remote does see fourth party op
          await ssd.apply(fourth.sentOperation({}, barney),
            local.join(fourth.time));
          remote.join(fourth.time).tick();

          const ops = await ssd.operationsSince(remote.time);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await firstValueFrom(ops.pipe(toArray())) : [];
          // We expect only the missed remote op
          expect(opArray.length).toBe(1);
          expect(opArray[0].data).toEqual(thirdOp.data);
        });

        // @see https://github.com/m-ld/m-ld-js/issues/29
        test('accepts own un-persisted update', async () => {
          await ssd.apply(local.sentOperation(
              {}, { '@id': 'wilma', 'name': 'Wilma' }),
            local);

          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/wilma'
          }))).resolves.toEqual([wilma]);
        });

        // @see https://github.com/m-ld/m-ld-js/issues/29
        test('answers un-persisted remote op', async () => {
          // Remote knows about first entry
          remote.join(local.time);
          const remoteTime = remote.time;
          // Create a remote entry that the remote fails to persist fully
          await ssd.apply(
            remote.sentOperation({ 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' }, {}),
            local.join(remote.time));
          // The remote asks for its previous time
          const ops = await ssd.operationsSince(remoteTime);
          expect(ops).not.toBeUndefined();
          const opArray = ops ? await firstValueFrom(ops.pipe(toArray())) : [];
          expect(opArray.length).toBe(1);
          expect(remote.time.equals(opArray[0].time)).toBe(true);
        });

        test('cuts stale message from incoming fusion', async () => {
          const third = remote.fork();
          // The remote will have two transactions, which fuse in its journal.
          // The local sees the first, adding wilma...
          const one = remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' });
          const oneTid = one.time.hash;
          await ssd.apply(one, local);
          // ... and then get a third-party txn, deleting wilma
          await ssd.apply(third.sentOperation(
              { 'tid': oneTid, 'o': 'Wilma', 'p': '#name', 's': 'wilma' }, {}),
            local);
          // Finally the local gets the remote fusion (as a rev-up), which still
          // includes the insert of wilma
          remote.tick();
          await ssd.apply(OperationMessage.fromOperation(one.time.ticks, testOp(remote.time, {}, [
            { 'tid': oneTid, 'o': 'Wilma', 'p': '#name', 's': 'wilma' },
            {
              'tid': remote.time.hash,
              'o': 'Barney',
              'p': '#name',
              's': 'barney'
            }], { from: one.time.ticks }), null), local);
          // Result should not include wilma because of the third-party delete
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/wilma'
          }))).resolves.toEqual([]);
        });

        test('cuts stale fusion from incoming fusion', async () => {
          remote.fork();
          // The remote will have two transactions, which fuse in its journal.
          // The local sees the first, adding wilma...
          const one = remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' });
          const oneTid = one.time.hash;
          await ssd.apply(one, local);
          // ... and a second, adding betty (this will fuse with the first)
          await ssd.apply(remote.sentOperation({}, { '@id': 'betty', 'name': 'Betty' }),
            local);
          // Finally the local gets the remote fusion (as a rev-up), which still
          // includes the insert of wilma but not betty
          remote.tick();
          await ssd.apply(OperationMessage.fromOperation(one.time.ticks, testOp(remote.time, {}, [
            { 'tid': oneTid, 'o': 'Wilma', 'p': '#name', 's': 'wilma' },
            {
              'tid': remote.time.hash,
              'o': 'Barney',
              'p': '#name',
              's': 'barney'
            }], { from: one.time.ticks }), null), local);
          // Result should not include betty because the fusion omits it
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/betty'
          }))).resolves.toEqual([]);
        });

        test('cuts snapshot last-seen message from incoming fusion', async () => {
          // Restart the clone with its own snapshot (unrealistic but benign)
          const snapshot = await ssd.takeSnapshot();
          const staticData = await firstValueFrom(snapshot.data.pipe(toArray()));
          // Get a new clock for the rejuvenated clone
          const newLocal = remote.join(local.time).fork();
          await ssd.applySnapshot(remote.snapshot(staticData), newLocal.time);
          // SSD should now have the 'fred' operation as a 'last operation'
          // The remote deletes fred...
          await ssd.apply(remote.sentOperation(
              { 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' }, {}),
            newLocal);
          // Finally the local gets its own data back in a rev-up, which still
          // includes the insert of fred, plus barney who it forgot about
          const firstTick = local.time.ticks;
          local.tick();
          await ssd.apply(OperationMessage.fromOperation(0, testOp(local.time, {}, [
            { 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' },
            {
              'tid': local.time.hash,
              'o': 'Barney',
              'p': '#name',
              's': 'barney'
            }], { from: firstTick }), null), newLocal);
          // Result should not include fred because of the remote delete
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }))).resolves.toEqual([]);
        });
      });
    });
  });

  describe('constraints', () => {
    let local: MockProcess, remote: MockProcess;
    let constraint: MeldConstraint;

    beforeEach(async () => {
      let { left, right } = TreeClock.GENESIS.forked();
      local = new MockProcess(left);
      remote = new MockProcess(right);
      constraint = {
        check: () => Promise.resolve()
      };
      ssd = new SuSetDataset(state.dataset,
        {},
        { constraints: [constraint] }, {},
        { '@id': 'test', '@domain': 'test.m-ld.org' });
      await ssd.initialise();
      await ssd.resetClock(local.tick().time);
      await ssd.allowTransact();
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
        firstValueFrom(state.read<Describe>({ '@describe': 'http://test.m-ld.org/wilma' }));
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
      await expect(drain(ssd.read(<Describe>{ '@describe': wilma['@id'] })))
        .resolves.toEqual([wilma]);
    });

    test('can upgrade an update to an agreement', async () => {
      constraint.check = async (_, interim) => {
        interim.assert({ '@insert': wilma, '@agree': true });
      };
      const msg = await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred })
      ]);
      expect(msg?.data[EncodedOperation.Key.agreed]).toEqual([2, true]);
    });

    test('can accumulate agreements', async () => {
      constraint.check = async (_, interim) => {
        interim.assert({ '@insert': wilma, '@agree': 2 });
        interim.assert({ '@insert': barney, '@agree': 3 });
      };
      const msg = await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred }),
        1
      ]);
      expect(msg?.data[EncodedOperation.Key.agreed]).toEqual([2, [1, 2, 3]]);
    });

    test('can downgrade an agreement to an update', async () => {
      constraint.check = async (_, interim) => {
        interim.assert({ '@insert': wilma, '@agree': false });
      };
      const msg = await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred }),
        true
      ]);
      expect(msg?.data[EncodedOperation.Key.agreed]).toBe(null);
    });

    test('applies an inserting constraint', async () => {
      constraint.apply = async (_, update) => update.assert({ '@insert': wilma });
      const willUpdate = captureUpdate();
      remote.join(local.time);
      const msg = await ssd.apply(
        remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }),
        local.join(remote.time));
      await expect(willUpdate).resolves.toEqual(
        { '@delete': [], '@insert': [fred, wilma], '@ticks': local.time.ticks });

      expect(msg).not.toBeNull();
      if (msg != null) {
        expect(msg.time.equals(local.time)).toBe(true);
        const [, from, time, upd, enc] = msg.data;
        expect(from).toBe(TreeClock.fromJson(time)?.ticks);
        expect(MeldEncoder.jsonFromBuffer(upd, enc)).toEqual([{}, {
          '@id': 'wilma',
          'name': 'Wilma'
        }]);
      }
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([wilma]);

      const newLocalTime = (<TreeClock>await ssd.loadClock());
      expect(newLocalTime.equals(local.time)).toBe(true);

      // Check that we have a valid journal
      const ops = await ssd.operationsSince(remote.time);
      const entries = await firstValueFrom(ops!.pipe(toArray()));
      expect(entries.length).toBe(1);
      expect(entries[0].time.equals(local.time)).toBe(true);
      const [, from, time, upd, enc] = entries[0].data;
      expect(from).toBe(TreeClock.fromJson(time)?.ticks);
      expect(MeldEncoder.jsonFromBuffer(upd, enc)).toEqual([{}, {
        '@id': 'wilma',
        'name': 'Wilma'
      }]);
    });

    test('applies a deleting constraint', async () => {
      constraint.apply = async (_, update) => update.assert({ '@delete': wilma });

      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': wilma })
      ]);

      const willUpdate = captureUpdate();
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }),
        local.join(remote.time));
      await expect(willUpdate).resolves.toEqual(
        { '@insert': [fred], '@delete': [wilma], '@ticks': local.time.ticks });

      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([]);
    });

    test('applies a self-deleting constraint', async () => {
      // Constraint is going to delete the data we're inserting
      constraint.apply = async (_, update) => update.assert({ '@delete': wilma });

      const willUpdate = captureUpdate();
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }),
        local.join(remote.time));

      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([]);

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
      ]))!.data[1];

      const willUpdate = captureUpdate();
      await ssd.apply(
        remote.sentOperation({ tid, o: 'Wilma', p: '#name', s: 'wilma' }, {}),
        local.join(remote.time));

      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([wilma]);

      // The deleted data was re-inserted, but Wilma may not have existed before
      await expect(willUpdate).resolves.toEqual(
        { '@insert': [wilma], '@delete': [], '@ticks': local.time.ticks });
    });
  });

  describe('agreements', () => {
    let local: MockProcess, remote: MockProcess;
    let checkpoints: Subject<JournalCheckPoint>;
    let agreementConditions: AgreementCondition[];

    beforeEach(async () => {
      let { left, right } = TreeClock.GENESIS.forked();
      local = new MockProcess(left);
      remote = new MockProcess(right);
      checkpoints = new Subject<JournalCheckPoint>();
      agreementConditions = [];
      ssd = new SuSetDataset(state.dataset, {}, { agreementConditions },
        { journalAdmin: { checkpoints } },
        { '@id': 'test', '@domain': 'test.m-ld.org', journal: { adminDebounce: 0 } });
      await ssd.initialise();
      await ssd.resetClock(local.time);
      await ssd.allowTransact();
    });

    test('emits a forced agreed operation', async () => {
      const agreement = (await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred }),
        true
      ]))!;
      const [, , , , , , agreed] = agreement.data;
      expect(agreed).toEqual([local.time.ticks, true]);
    });

    test('does not enforce conditions on local agreement', async () => {
      agreementConditions.push({ test: mockFn().mockRejectedValue('nope') });
      await expect(ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred }),
        true
      ])).resolves.toBeDefined();
    });

    test('admits an agreed operation caused after a local write', async () => {
      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred })
      ]);
      remote.join(local.time); // Remote has received the agreement
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, true),
        local.join(remote.time))).resolves.toBe(null);
    });

    test('enforces conditions on remote operation', async () => {
      agreementConditions.push({ test: mockFn().mockRejectedValue('nope') });
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, true),
        local.join(remote.time))).rejects.toBe('nope');
    });

    test('ignores an operation concurrent with a local agreement', async () => {
      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred }),
        true
      ]);
      // Not joining with local time here
      const willUpdate = captureUpdate();
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }),
        local.join(remote.time))).resolves.toBe(null);
      await expectNoUpdate(willUpdate);
    });

    test('voids a local operation concurrent with a remote agreement', async () => {
      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred })
      ]);
      // Not joining with local time here
      const willUpdate = captureUpdate();
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, true),
        local.join(remote.time))).resolves.toBe(null);
      // Check fred gets voided out
      await expect(willUpdate).resolves.toMatchObject({
        '@delete': [fred],
        '@insert': [wilma]
      });
      const snapshot = await ssd.takeSnapshot();
      // Check GWT has been reset (to genesis, for the local process)
      expect(snapshot.gwc.getTicks(local.time)).toBe(0);
      expect(snapshot.gwc.getTicks(remote.time)).toBe(1);
      // Snapshot data should have one operation and one triple
      const data = await firstValueFrom(snapshot.data.pipe(toArray()));
      expect(data.length).toBe(2);
      expect(data.every(v => {
        if ('operation' in v) {
          const [, from, time] = v.operation;
          expect(from).toBe(remote.time.ticks);
          expect(TreeClock.fromJson(time).equals(remote.time)).toBe(true);
          return true;
        } else if ('inserts' in v) {
          expect(MeldEncoder.jsonFromBuffer<any>(v.inserts, v.encoding).s).toBe('wilma');
          return true;
        }
      })).toBe(true);
    });

    test('resets to the causes of an agreement', async () => {
      // The local has seen another process
      const third = local.fork();
      // Which transacts
      await ssd.apply(
        third.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }),
        local.join(third.time));
      // Local time should have ticks from the third process
      expect(local.time.getTicks(third.time)).toBe(1);
      // Now receive agreement, but not joining with local time
      const willUpdate = captureUpdate();
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, true),
        local.join(remote.time))).resolves.toBe(null);
      // Check fred gets voided out
      await expect(willUpdate).resolves.toMatchObject({
        '@delete': [fred],
        '@insert': [wilma]
      });
      // Local time should no longer have ticks from the third process
      expect(local.time.getTicks(third.time)).toBe(0);
    });

    test('voids an operation in which deleted triples not found', async () => {
      const firstTid = (await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred })
      ]))!.time.hash;
      remote.join(local.time); // Remote has received our insert
      // Both we and a third process are going to delete Fred
      const third = local.fork();
      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@delete': fred })
      ]);
      remote.join(local.time); // Remote has received our delete (but third has not)
      // Third deletes Fred based on the first transaction - this will do nothing
      await ssd.apply(
        third.sentOperation({ 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' }, {}),
        local.join(third.time));
      // Now the remote enacts an agreement, voiding third's operation
      const willUpdate = captureUpdate();
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, true),
        local.join(remote.time))).resolves.toBe(null);
      // Check fred does not get re-inserted
      await expect(willUpdate).resolves.toMatchObject({ '@insert': [wilma] });
      // Local time should no longer have ticks from the third process
      expect(third.time.ticks).toBe(2);
      expect(local.time.getTicks(third.time)).toBe(1);
    });

    test('voids a fused operation and re-connects', async () => {
      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': fred })
      ]);
      remote.join(local.time); // Remote has received our first insert only
      await ssd.transact(async () => [
        local.tick().time,
        await ssd.write({ '@insert': wilma })
      ]);
      // This should provoke an immediate fusion
      checkpoints.next(JournalCheckPoint.SAVEPOINT);
      // Not joining with local time here
      const willUpdate = captureUpdate();
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'barney', 'name': 'Barney' }, true),
        local.join(remote.time))).rejects.toBeInstanceOf(MeldError);
      // Check fred & wilma got voided out but the agreement was not applied
      await expect(willUpdate).resolves.toMatchObject({
        '@delete': [fred, wilma],
        '@insert': []
      });
      // Check we have rewound all the way to before the first fused op
      expect(local.time.ticks).toBe(0);
    });
  });

  test('enforces operation size limit', async () => {
    ssd = new SuSetDataset(state.dataset, {}, {}, {}, {
      '@id': 'test',
      '@domain': 'test.m-ld.org',
      maxOperationSize: 1
    });
    await ssd.initialise();
    await ssd.resetClock(TreeClock.GENESIS);
    await ssd.allowTransact();
    await expect(ssd.transact(async () => [
      TreeClock.GENESIS.ticked(),
      await ssd.write({ '@insert': fred })
    ])).rejects.toThrow();
  });
});