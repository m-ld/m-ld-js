import { SuSetDataset } from '../src/engine/dataset/SuSetDataset';
import { decodeOpUpdate, MockProcess, MockState, testOp } from './testClones';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { take, toArray } from 'rxjs/operators';
import { EmptyError, firstValueFrom, lastValueFrom, Subject } from 'rxjs';
import {
  AgreementCondition, combinePlugins, Describe, JournalCheckPoint, MeldConstraint, MeldError,
  MeldPlugin, MeldTransportSecurity, MeldUpdate, Select
} from '../src';
import { jsonify } from './testUtil';
import { MeldEncoder } from '../src/engine/MeldEncoding';
import { BufferEncoding, EncodedOperation, Snapshot } from '../src/engine';
import { drain } from 'rx-flowable';
import { mockFn } from 'jest-mock-extended';
import { MeldOperationMessage } from '../src/engine/MeldOperationMessage';
import { byteArrayDatatype, jsonDatatype } from '../src/datatype';
import { CounterType } from './datatypeFixtures';

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

  function expectNoUpdate(willUpdate: Promise<MeldUpdate>) {
    return expect(Promise.race([
      // Give it a tick, just in case the update is async
      willUpdate, new Promise(resolve => setImmediate(resolve))]
    )).resolves.toBeUndefined();
  }

  describe('with basic config', () => {
    beforeEach(async () => {
      ssd = new SuSetDataset(state.dataset,
        {},
        combinePlugins([]),
        {},
        { '@id': 'test', '@domain': 'test.m-ld.org' });
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
        const willUpdate = firstValueFrom(ssd.updates);
        const msg = await ssd.transact(local.tick().time, { '@insert': [] });
        expect(msg).toBeNull();
        await expectNoUpdate(willUpdate);
      });

      test('transacts an insert', async () => {
        const willUpdate = firstValueFrom(ssd.updates);

        const msg = await ssd.transact(local.tick().time, { '@insert': fred });
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
        const willUpdate = firstValueFrom(ssd.updates);

        await ssd.apply(
          remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }),
          local.join(remote.time));
        await expect(willUpdate).resolves.toHaveProperty('@insert', [fred]);

        await expect(drain(ssd.read<Describe>({
          '@describe': 'http://test.m-ld.org/fred'
        }))).resolves.toEqual([fred]);
      });

      test('applies a no-op operation', async () => {
        const willUpdate = firstValueFrom(ssd.updates);

        const msg = await ssd.apply(remote.sentOperation({}, {}),
          local.join(remote.time));

        expect(msg).toBeNull();
        await expectNoUpdate(willUpdate);
      });

      describe('with an initial triple', () => {
        let firstTid: string;

        beforeEach(async () => {
          firstTid = (await ssd.transact(local.tick().time, { '@insert': fred }))!.time.hash;
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
          const tid2 = (await ssd.transact(local.tick().time, { '@insert': wilma }))!.time.hash;
          const snapshot = await ssd.takeSnapshot();
          expect(snapshot.gwc.equals(local.gwc)).toBe(true);
          const data = await firstValueFrom(snapshot.data.pipe(toArray()));
          expect(data.length).toBe(2);
          const expectReifiedJson = expect.arrayContaining([
            { '@id': expect.any(String), 's': 'fred', 'p': '#name', 'o': 'Fred', 'tid': firstTid },
            { '@id': expect.any(String), 's': 'wilma', 'p': '#name', 'o': 'Wilma', 'tid': tid2 }
          ]);
          expect(data.every(v => {
            if ('operation' in v) {
              const [ver, from, time, upd, enc] = v.operation;
              expect(ver).toBe(4);
              expect(from).toBe(firstTick);
              expect(TreeClock.fromJson(time).equals(local.time)).toBe(true);
              const [del, ins]: [{}, []] = MeldEncoder.jsonFromBuffer(upd, enc);
              expect(del).toEqual({});
              expect(ins).toEqual(expectReifiedJson);
              expect(ins.length).toBe(2);
              return true;
            } else if ('inserts' in v) {
              const ins = MeldEncoder.jsonFromBuffer<[]>(v.inserts, v.encoding);
              expect(ins).toEqual(expectReifiedJson);
              expect(ins.length).toBe(2);
              expect(v.encoding).toEqual([BufferEncoding.MSGPACK]);
              return true;
            }
          })).toBe(true);
        });

        test('answers a snapshot with fused duplicating operations', async () => {
          const firstTick = local.time.ticks;
          const tid2 = (await ssd.transact(
            local.tick().time,
            { '@insert': fred } // again
          ))!.time.hash;
          const snapshot = await ssd.takeSnapshot();
          const data = await firstValueFrom(snapshot.data.pipe(toArray()));
          expect(data.length).toBe(2);
          const expectReifiedJson = {
            '@id': expect.any(String),
            's': 'fred',
            'p': '#name',
            'o': 'Fred',
            'tid': expect.arrayContaining([firstTid, tid2])
          };
          expect(data.every(v => {
            if ('operation' in v) {
              const [ver, from, time, upd, enc] = v.operation;
              expect(ver).toBe(4);
              expect(from).toBe(firstTick);
              expect(TreeClock.fromJson(time).equals(local.time)).toBe(true);
              const [del, ins]: [{}, []] = MeldEncoder.jsonFromBuffer(upd, enc);
              expect(del).toEqual({});
              expect(ins).toEqual(expectReifiedJson);
              return true;
            } else if ('inserts' in v) {
              const ins = MeldEncoder.jsonFromBuffer<[]>(v.inserts, v.encoding);
              expect(ins).toEqual(expectReifiedJson);
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
          }, state.newTxnContext()));
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
          const willUpdate = firstValueFrom(ssd.updates);

          const msg = await ssd.transact(
            local.tick().time, { '@delete': { '@id': 'http://test.m-ld.org/fred' } });
          const update = await willUpdate;
          expect(update).toHaveProperty('@delete', [fred]);
          expect(msg!.time.equals(local.time)).toBe(true);
          const [, , , upd, enc] = msg!.data;
          expect(MeldEncoder.jsonFromBuffer(upd, enc)).toMatchObject([{
            'tid': firstTid,
            's': 'fred',
            'p': '#name',
            'o': 'Fred'
          }, {}]);
          // Check audit trace
          const trace = update.trace();
          expect(trace.trigger.operation).toMatchObject(msg!.data);
          // Expecting no new buffers: same buffer object used
          expect(trace.trigger.data).toBe(MeldOperationMessage.enc(msg!));
          expect(trace.trigger.attribution).toBeNull();
          expect(trace.applicable).toBeUndefined();
          expect(trace.resolution).toBeUndefined();
          expect(trace.voids).toEqual([]);
        });

        test('applies a delete operation', async () => {
          const willUpdate = firstValueFrom(ssd.updates);

          await ssd.apply(
            remote.sentOperation({ 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' }, {}),
            local.join(remote.time));
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/fred'
          }))).resolves.toEqual([]);
          const update = await willUpdate;
          // Check update JSON in full (no trace in JSON)
          expect(jsonify(update)).toEqual({
            '@delete': [fred], '@insert': [], '@update': [], '@ticks': 2
          });
          // Check audit trace
          const trace = update.trace();
          const [, , , upd, enc] = trace.trigger.operation;
          expect(MeldEncoder.jsonFromBuffer(upd, enc)).toEqual([{
            'tid': firstTid, 's': 'fred', 'p': '#name', 'o': 'Fred'
          }, {}]);
          expect(trace.trigger.data
            .equals(EncodedOperation.toBuffer(trace.trigger.operation))).toBe(true);
          expect(trace.trigger.attribution).toBeNull();
          expect(trace.applicable).toEqual(trace.trigger);
          expect(trace.resolution).toBeUndefined();
          expect(trace.voids).toEqual([]);
        });

        test('applies a concurrent delete', async () => {
          // Insert Fred again, with Wilma and a new TID
          const remoteOp = remote.sentOperation({},
            [{ '@id': 'fred', 'name': 'Fred' }, { '@id': 'wilma', 'name': 'Wilma' }]);
          await ssd.apply(remoteOp, local.join(remote.time));
          // Delete the remotely-inserted Flintstones
          const willUpdate = firstValueFrom(ssd.updates);
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
          const msg = await ssd.transact(local.tick().time, { '@insert': barney });
          expect(msg!.time.equals(local.time)).toBe(true);

          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/barney'
          }))).resolves.toEqual([barney]);
        });

        test('deletes a duplicated insert', async () => {
          const tid2 = (await ssd.transact(local.tick().time, { '@insert': fred }))!.time.hash;
          const msg = (await ssd.transact(local.tick().time, { '@delete': fred }))!;
          const [del] = decodeOpUpdate(msg);
          expect(del).toEqual({
            '@id': expect.any(String),
            's': 'fred', 'p': '#name', 'o': 'Fred',
            'tid': expect.arrayContaining([firstTid, tid2])
          });
        });

        test('deletes a duplicated insert after snapshot', async () => {
          const tid2 = (await ssd.transact(local.tick().time, { '@insert': fred }))!.time.hash;
          const snapshot = await ssd.takeSnapshot();
          const staticData = await firstValueFrom(snapshot.data.pipe(toArray()));
          await ssd.applySnapshot(local.snapshot(staticData), local.tick().time);
          const msg = (await ssd.transact(local.tick().time, { '@delete': fred }))!;
          const [del] = decodeOpUpdate(msg);
          expect(del).toEqual({
            '@id': expect.any(String),
            's': 'fred', 'p': '#name', 'o': 'Fred',
            'tid': expect.arrayContaining([firstTid, tid2])
          });
        });

        test('transacts a duplicating insert', async () => {
          const willUpdate = firstValueFrom(ssd.updates);
          const moreFred = { ...fred, 'http://test.m-ld.org/#height': 6 };
          await ssd.transact(local.tick().time, moreFred);
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
          await ssd.transact(local.tick().time, { '@insert': barney });
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
          const localOp = await ssd.transact(local.tick().time, { '@insert': barney });
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
          const willUpdate = firstValueFrom(ssd.updates);
          await ssd.apply(MeldOperationMessage.fromOperation(
            one.time.ticks, testOp(remote.time, {}, [
              { 'tid': oneTid, 'o': 'Wilma', 'p': '#name', 's': 'wilma' },
              { 'tid': remote.time.hash, 'o': 'Barney', 'p': '#name', 's': 'barney' }
            ], { from: one.time.ticks }), null), local);
          // Result should not include wilma because of the third-party delete
          await expect(drain(ssd.read<Describe>({
            '@describe': 'http://test.m-ld.org/wilma'
          }))).resolves.toEqual([]);
          //////////////////////////////////////////////////////////////////////
          // Check audit trace
          const trace = (await willUpdate).trace();
          let [, from, time, upd, enc] = trace.trigger.operation;
          expect(MeldEncoder.jsonFromBuffer(upd, enc)).toMatchObject([{}, [
            { 'tid': oneTid, 'o': 'Wilma', 'p': '#name', 's': 'wilma' },
            { 'tid': remote.time.hash, 'o': 'Barney', 'p': '#name', 's': 'barney' }
          ]]);
          expect(from).toBeLessThan(TreeClock.fromJson(time).ticks);
          expect(trace.applicable).toBeDefined();
          [, from, time, upd, enc] = trace.applicable!.operation;
          expect(MeldEncoder.jsonFromBuffer(upd, enc)).toMatchObject([
            // Compact form because no longer a fusion
            {}, { '@id': 'barney', name: 'Barney' }
          ]);
          expect(from).toBe(TreeClock.fromJson(time).ticks);
          expect(trace.resolution).toBeUndefined();
          expect(trace.voids).toEqual([]);
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
          await ssd.apply(MeldOperationMessage.fromOperation(
            one.time.ticks, testOp(remote.time, {}, [
              { 'tid': oneTid, 'o': 'Wilma', 'p': '#name', 's': 'wilma' },
              { 'tid': remote.time.hash, 'o': 'Barney', 'p': '#name', 's': 'barney' }
            ], { from: one.time.ticks }), null), local);
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
          await ssd.apply(MeldOperationMessage.fromOperation(0, testOp(local.time, {}, [
            { 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' },
            { 'tid': local.time.hash, 'o': 'Barney', 'p': '#name', 's': 'barney' }
          ], { from: firstTick }), null), newLocal);
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
      constraint = { check: () => Promise.resolve() };
      ssd = new SuSetDataset(state.dataset,
        {}, combinePlugins([{ constraints: [constraint] }]), {},
        { '@id': 'test', '@domain': 'test.m-ld.org' });
      await ssd.initialise();
      await ssd.resetClock(local.tick().time);
      await ssd.allowTransact();
    });

    test('checks the constraint', async () => {
      constraint.check = () => Promise.reject('Failed!');
      await expect(ssd.transact(local.tick().time, { '@insert': fred })).rejects.toBe('Failed!');
    });

    test('provides state to the constraint', async () => {
      await ssd.transact(local.tick().time, { '@insert': wilma });
      constraint.check = async state =>
        firstValueFrom(state.read<Describe>({ '@describe': wilma['@id'] }));
      await expect(ssd.transact(local.tick().time, { '@insert': fred })).resolves.toBeDefined();
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
      await expect(ssd.transact(local.tick().time, { '@insert': fred })).resolves.toBeDefined();
      await expect(drain(ssd.read(<Describe>{ '@describe': wilma['@id'] })))
        .resolves.toEqual([wilma]);
    });

    test('can upgrade an update to an agreement', async () => {
      constraint.check = async (_, interim) => {
        interim.assert({ '@insert': wilma, '@agree': true });
      };
      const msg = await ssd.transact(local.tick().time, { '@insert': fred });
      expect(msg?.data[EncodedOperation.Key.agreed]).toEqual([2, true]);
    });

    test('can accumulate agreements', async () => {
      constraint.check = async (_, interim) => {
        interim.assert({ '@insert': wilma, '@agree': 2 });
        interim.assert({ '@insert': barney, '@agree': 3 });
      };
      const msg = await ssd.transact(local.tick().time, { '@insert': fred, '@agree': 1 });
      expect(msg?.data[EncodedOperation.Key.agreed]).toEqual([2, [1, 2, 3]]);
    });

    test('can downgrade an agreement to an update', async () => {
      constraint.check = async (_, interim) => {
        interim.assert({ '@insert': wilma, '@agree': false });
      };
      const msg = await ssd.transact(local.tick().time, { '@insert': fred, '@agree': true });
      expect(msg?.data[EncodedOperation.Key.agreed]).toBe(null);
    });

    test('applies an inserting constraint', async () => {
      constraint.apply = async (_, update) => update.assert({ '@insert': wilma });
      const willUpdate = firstValueFrom(ssd.updates);
      remote.join(local.time);
      const msg = await ssd.apply(
        remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }),
        local.join(remote.time));
      await expect(willUpdate).resolves.toMatchObject(
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
      const firstTid = (await ssd.transact(local.tick().time, { '@insert': wilma }))!.time.hash;
      const willUpdate = firstValueFrom(ssd.updates);
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }),
        local.join(remote.time));
      const update = await willUpdate;
      expect(update).toMatchObject(
        { '@insert': [fred], '@delete': [wilma], '@ticks': local.time.ticks });
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([]);
      // Check audit trace
      const trace = update.trace();
      let [, , , upd, enc] = trace.trigger.operation;
      expect(MeldEncoder.jsonFromBuffer(upd, enc)).toMatchObject([{}, {
        '@id': 'fred', name: 'Fred'
      }]);
      expect(trace.applicable).toEqual(trace.trigger);
      expect(trace.resolution).toBeDefined();
      [, , , upd, enc] = trace.resolution!.operation;
      expect(MeldEncoder.jsonFromBuffer(upd, enc)).toMatchObject([{
        'tid': firstTid,
        's': 'wilma',
        'p': '#name',
        'o': 'Wilma'
      }, {}]);
      expect(trace.resolution!.data
        .equals(EncodedOperation.toBuffer(trace.resolution!.operation))).toBe(true);
      expect(trace.resolution!.attribution).toBeNull();
      expect(trace.voids).toEqual([]);
    });

    test('applies a self-deleting constraint', async () => {
      // Constraint is going to delete the data we're inserting
      constraint.apply = async (_, update) => update.assert({ '@delete': wilma });

      const willUpdate = firstValueFrom(ssd.updates);
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }),
        local.join(remote.time));

      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([]);

      // The inserted data was deleted, but Wilma may have existed before
      await expect(willUpdate).resolves.toMatchObject(
        { '@insert': [], '@delete': [wilma], '@ticks': local.time.ticks });
    });

    test('applies a self-inserting constraint', async () => {
      // Constraint is going to insert the data we're deleting
      constraint.apply = async (_, update) => update.assert({ '@insert': wilma });

      const tid = (await ssd.transact(local.tick().time, { '@insert': wilma }))!.time.hash;

      const willUpdate = firstValueFrom(ssd.updates);
      await ssd.apply(
        remote.sentOperation({ tid, o: 'Wilma', p: '#name', s: 'wilma' }, {}),
        local.join(remote.time));

      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([wilma]);

      // The deleted data was re-inserted, but Wilma may not have existed before
      await expect(willUpdate).resolves.toMatchObject(
        { '@insert': [wilma], '@delete': [], '@ticks': local.time.ticks });
    });

    test('bad operation issues no-op error update', async () => {
      constraint.apply = () => Promise.reject(new MeldError('Unauthorised'));

      const willUpdate = firstValueFrom(ssd.updates);
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, {
          attr: { pid: 'http://box.ex.org/#me', sig: Buffer.of() }
        }),
        local.join(remote.time));

      const update = await willUpdate;
      expect(update).toMatchObject({
        '@delete': expect.objectContaining({ length: 0 }),
        '@insert': expect.objectContaining({ length: 0 }),
        '@ticks': local.time.ticks,
        '@principal': { '@id': 'http://box.ex.org/#me' }
      });
      const trace = update.trace();
      expect(trace.error?.status).toBe(4030);
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([]);
    });

    test('bad operation blocks clone', async () => {
      constraint.apply = () => Promise.reject(new MeldError('Unauthorised'));

      await ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }),
        local.join(remote.time));

      const preRemoteTicks = remote.time.ticks;
      const willUpdate = firstValueFrom(ssd.updates);
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Flintstone' }),
        local.join(remote.time));

      expect(local.time.getTicks(remote.time)).toBe(preRemoteTicks);
      await expectNoUpdate(willUpdate);
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/wilma'
      }))).resolves.toEqual([]);
    });

    test('blocked clone is included in snapshot', async () => {
      constraint.apply = () => Promise.reject(new MeldError('Unauthorised'));
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }),
        local.join(remote.time));

      const snapshot = await ssd.takeSnapshot();
      // Note that the blocked flag is strictly an internal detail
      expect(snapshot.gwc.tid(remote.time)).toBe('!blocked!');
    });

    test('entailed deletions retain TIDs', async () => {
      // Constraint is going to entail deletion of the data we're inserting
      constraint.check = async (_, update) => update.entail({ '@delete': wilma });
      const { data: [, , , upd1, enc1], time } = (await ssd.transact(
        local.tick().time, { '@insert': wilma }))!;
      // Wilma asserted in the update
      expect(MeldEncoder.jsonFromBuffer(upd1, enc1)).toEqual([{}, {
        '@id': 'wilma', name: 'Wilma'
      }]);
      // Wilma does not match in the current dataset (entailed deleted)
      await expect(drain(ssd.read(<Describe>{ '@describe': wilma['@id'] })))
        .resolves.toEqual([]);
      // Now also assert the deletion of wilma
      const { data: [, , , upd2, enc2] } = (await ssd.transact(
        local.tick().time, { '@delete': wilma }))!;
      // Wilma asserted deleted in the update
      expect(MeldEncoder.jsonFromBuffer(upd2, enc2)).toMatchObject([
        { tid: time.hash, s: 'wilma', p: '#name', o: 'Wilma' }, {}
      ]);
    });
  });

  describe.each(
    [Infinity, 0] // Testing with unbounded data cache and none
  )('indirected datatypes', (maxDataCacheSize) => {
    let local: MockProcess, remote: MockProcess;
    let extensions: MeldPlugin[];

    beforeEach(async () => {
      extensions = [];
      extensions.push(jsonDatatype);
      let { left, right } = TreeClock.GENESIS.forked();
      local = new MockProcess(left);
      remote = new MockProcess(right);
      ssd = new SuSetDataset(state.dataset,
        {},
        combinePlugins(extensions),
        {},
        { '@id': 'test', '@domain': 'test.m-ld.org', maxDataCacheSize }
      );
      await ssd.initialise();
      await ssd.resetClock(local.time);
      await ssd.allowTransact();
    });

    test('insert indirected datatype', async () => {
      const validate = jest.spyOn(byteArrayDatatype, 'validate').mockReset();
      const getDataId = jest.spyOn(byteArrayDatatype, 'getDataId').mockReset();
      extensions.push(byteArrayDatatype);
      const willUpdate = firstValueFrom(ssd.updates);
      const photo = new Buffer('abc');
      const fredProfile = {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#photo': photo
      };
      const msg = await ssd.transact(local.tick().time, fredProfile);
      expect(validate).toBeCalledWith(photo);
      const validationResult = validate.mock.results[0];
      expect(getDataId).toBeCalled();
      // Operation has buffer value
      const [, , , upd, enc] = msg!.data;
      expect(MeldEncoder.jsonFromBuffer(upd, enc)).toEqual([{}, { '@id': 'fred', photo }]);
      // Update has value as given
      await expect(willUpdate).resolves.toMatchObject({ '@insert': [fredProfile] });
      // Retrieved state has value as given
      const [retrieved] = await drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }));
      expect(retrieved).toEqual(fredProfile);
      // If caching, we expect the photo retrieved to be the validation result
      expect(validationResult.value === retrieved['http://test.m-ld.org/#photo'])
        .toBe(maxDataCacheSize > photo.length);
      // Query equates value
      await expect(drain(ssd.read<Select>({
        '@select': '?f', '@where': { ...fredProfile, '@id': '?f' }
      }))).resolves.toMatchObject([{ '?f': { '@id': 'http://test.m-ld.org/fred' } }]);
    });

    test('apply operation with indirected data', async () => {
      const validate = jest.spyOn(byteArrayDatatype, 'validate').mockReset();
      const getDataId = jest.spyOn(byteArrayDatatype, 'getDataId').mockReset();
      extensions.push(byteArrayDatatype);
      const photo = new Buffer('abc');
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'fred', photo }),
        local.join(remote.time)
      )).resolves.toBe(null);
      expect(validate).not.toBeCalled(); // coming from protocol, not app
      expect(getDataId).toBeCalled();
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toEqual([{
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#photo': photo
      }]);
    });

    test('datatype data included in snapshot', async () => {
      extensions.push(byteArrayDatatype);
      const photo = new Buffer('abc');
      const fredProfile = {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#photo': photo
      };
      await ssd.transact(local.tick().time, fredProfile);
      const snapshot = await ssd.takeSnapshot();
      // Snapshot data should have one operation and one triple
      const data = await firstValueFrom(snapshot.data.pipe(toArray()));
      expect(data.length).toBe(2);
      const datum = data.find((v): v is Snapshot.Inserts => 'inserts' in v)!;
      expect(datum).toBeDefined();
      expect(MeldEncoder.jsonFromBuffer<any>(datum.inserts, datum.encoding).o).toEqual(photo);
      // Re-applying the snapshot recovers binary data
      await ssd.applySnapshot(local.snapshot(data), local.tick().time);
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toEqual([fredProfile]);
    });

    test('inserts and deletes a shared datatype', async () => {
      extensions.push(new CounterType('http://test.m-ld.org/#likes'));
      const willUpdates = firstValueFrom(ssd.updates.pipe(take(2), toArray()));
      const fred = {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 0
      };
      await ssd.transact(local.tick().time, fred);
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toMatchObject([fred]);
      await ssd.transact(local.tick().time, {
        '@delete': {
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': '?'
        }
      });
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toEqual([]);
      await expect(willUpdates).resolves.toMatchObject([{
        '@insert': [fred]
      }, {
        '@delete': [fred]
      }]);
    });

    test('operates on shared datatype', async () => {
      const counterType = new CounterType('http://test.m-ld.org/#likes');
      const counterUpdate = jest.spyOn(counterType, 'update');
      extensions.push(counterType);
      await ssd.transact(local.tick().time, {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 0
      });
      const willUpdate = firstValueFrom(ssd.updates);
      const msg = await ssd.transact(local.tick().time, {
        '@update': {
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': { '@plus': 1 }
        }
      });
      expect(counterUpdate).toHaveBeenCalled();
      // Operation has custom operation
      const [, , , upd, enc] = msg!.data;
      expect(MeldEncoder.jsonFromBuffer(upd, enc)).toEqual([{}, {}, {
        '@id': expect.stringMatching(/[0-9a-z_]{6,}/),
        s: 'fred',
        p: '#likes',
        o: { '@type': 'http://ex.org/#Counter', '@value': 'counter1' },
        op: { '@type': '@json', '@value': '+1' }
      }]);
      // Update has custom operation
      await expect(willUpdate).resolves.toMatchObject({
        '@update': [{
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': { '@plus': 1 }
        }]
      });
      // Final state has increment
      const counterApply = jest.spyOn(counterType, 'apply');
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toMatchObject([{
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 1
      }]);
      // If caching, should return from cache; otherwise, should re-apply
      if (maxDataCacheSize > counterType.sizeOf(1))
        expect(counterApply).not.toHaveBeenCalled();
      else
        expect(counterApply).toHaveBeenCalled();
    });

    test('rolls back shared data update on failure', async () => {
      const counterType = new CounterType('http://test.m-ld.org/#likes');
      extensions.push(counterType);
      await ssd.transact(local.tick().time, {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 0
      });
      // Also push a constraint which summarily fails
      extensions.push({ constraints: [{ check: () => Promise.reject('bork') }] });
      const counterUpdate = jest.spyOn(counterType, 'update');
      await expect(ssd.transact(local.tick().time, {
        '@update': {
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': { '@plus': 1 }
        }
      })).rejects.toBe('bork');
      expect(counterUpdate).toHaveBeenCalledWith(0, { '@plus': 1 });
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toMatchObject([{
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 0
      }]);
    });

    test('applies shared datatype operation', async () => {
      extensions.push(new CounterType('http://test.m-ld.org/#likes'));
      await expect(ssd.apply(
        remote.sentOperation({}, {
          '@id': 'fred',
          likes: { '@id': 'counterX', '@type': 'http://ex.org/#Counter', '@value': 0 }
        }),
        local.join(remote.time)
      )).resolves.toBe(null);
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, {}, {
          operations: {
            '@id': 'ab',
            s: 'fred',
            p: '#likes',
            o: { '@type': 'http://ex.org/#Counter', '@value': 'counterX' },
            op: { '@type': '@json', '@value': '+1' }
          }
        }),
        local.join(remote.time)
      )).resolves.toBe(null);
      await expect(willUpdate).resolves.toMatchObject({
        '@update': [{
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': { '@plus': 1 }
        }]
      });
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toMatchObject([{
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 1
      }]);
    });

    test('shared data included in snapshot', async () => {
      extensions.push(new CounterType('http://test.m-ld.org/#likes'));
      const fredProfile = {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 1
      };
      await ssd.transact(local.tick().time, fredProfile);
      await ssd.transact(local.tick().time, {
        '@update': {
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': { '@plus': 1 }
        }
      });
      const snapshot = await ssd.takeSnapshot();
      // Snapshot data should have one operation and one triple
      const data = await firstValueFrom(snapshot.data.pipe(toArray()));
      expect(data.length).toBe(2);
      const datum = data.find((v): v is Snapshot.Inserts => 'inserts' in v)!;
      expect(datum).toBeDefined();
      expect(MeldEncoder.jsonFromBuffer<any>(datum.inserts, datum.encoding).o).toEqual({
        // identity must be included in value, so operations can match
        '@id': expect.stringMatching(/counter\d/),
        '@type': 'http://ex.org/#Counter',
        '@value': 2
      });
    });

    test('prevents conflicting shared inserts in write', async () => {
      extensions.push(new CounterType('http://test.m-ld.org/#likes'));
      await expect(ssd.transact(local.tick().time, {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': [0, 10]
      })).rejects.toMatch(/Multiple shared data values/);
    });

    test('prevents conflicting shared inserts in state', async () => {
      extensions.push(new CounterType('http://test.m-ld.org/#likes'));
      await ssd.transact(local.tick().time, {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 0
      });
      await expect(ssd.transact(local.tick().time, {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 10
      })).rejects.toMatch(/Multiple shared data values/);
    });

    test('picks one from concurrent shared inserts', async () => {
      extensions.push(new CounterType('http://test.m-ld.org/#likes'));
      await ssd.transact(local.tick().time, {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 0
      });
      await expect(ssd.apply(
        remote.sentOperation({}, {
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': {
            '@id': 'counterX', '@type': 'http://ex.org/#Counter', '@value': 10
          }
        }, { agree: true }),
        local.join(remote.time)
      )).resolves.toBe(null);
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toMatchObject([{
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': 10
      }]);
    });

    test.skip('voids shared operations', async () => {
      extensions.push(new CounterType('http://test.m-ld.org/#likes'));
      await ssd.transact(local.tick().time, {
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': {
          '@type': 'http://ex.org/#Counter',
          '@value': 0
        }
      });
      remote.join(local.time);
      await ssd.transact(local.tick().time, {
        '@update': {
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': { '@plus': 1 }
        }
      });
      // Not joining with local time here
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, { agree: true }),
        local.join(remote.time)
      )).resolves.toBe(null);
      // Check fred's likes have been voided out
      const update = await willUpdate;
      expect(update).toMatchObject({
        '@insert': [wilma],
        '@update': [{
          '@id': 'http://test.m-ld.org/fred',
          'http://test.m-ld.org/#likes': { '@plus': -1 }
        }]
      });
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toMatchObject([{
        '@id': 'http://test.m-ld.org/fred',
        'http://test.m-ld.org/#likes': { '@type': 'http://ex.org/#Counter', '@value': 0 }
      }]);
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
      ssd = new SuSetDataset(state.dataset,
        {},
        combinePlugins([{ agreementConditions }]),
        { journalAdmin: { checkpoints } },
        { '@id': 'test', '@domain': 'test.m-ld.org', journal: { adminDebounce: 0 } }
      );
      await ssd.initialise();
      await ssd.resetClock(local.time);
      await ssd.allowTransact();
    });

    test('emits a forced agreed operation', async () => {
      const agreement = (await ssd.transact(
        local.tick().time, { '@insert': fred, '@agree': true }))!;
      const [, , , , , , agreed] = agreement.data;
      expect(agreed).toEqual([local.time.ticks, true]);
    });

    test('does not enforce conditions on local agreement', async () => {
      agreementConditions.push({ test: mockFn().mockRejectedValue('nope') });
      await expect(ssd.transact(local.tick().time, { '@insert': fred, '@agree': true }))
        .resolves.toBeDefined();
    });

    test('admits an agreed operation caused after a local write', async () => {
      await ssd.transact(local.tick().time, { '@insert': fred });
      remote.join(local.time); // Remote has received the agreement
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, { agree: true }),
        local.join(remote.time))).resolves.toBe(null);
    });

    test('enforces conditions on remote operation', async () => {
      agreementConditions.push({ test: mockFn().mockRejectedValue('nope') });
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, { agree: true }),
        local.join(remote.time))).rejects.toBe('nope');
    });

    test('ignores an operation concurrent with a local agreement', async () => {
      await ssd.transact(local.tick().time, { '@insert': fred, '@agree': true });
      // Not joining with local time here
      const preRemoteTicks = remote.time.ticks;
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }),
        local.join(remote.time))).resolves.toBe(null);
      expect(local.time.getTicks(remote.time)).toBe(preRemoteTicks);
      await expectNoUpdate(willUpdate);
    });

    test('voids a local operation concurrent with a remote agreement', async () => {
      await ssd.transact(local.tick().time, { '@insert': fred });
      // Not joining with local time here
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, { agree: true }),
        local.join(remote.time))).resolves.toBe(null);
      // Check fred gets voided out
      const update = await willUpdate;
      expect(update).toMatchObject({ '@delete': [fred], '@insert': [wilma] });
      //////////////////////////////////////////////////////////////////////////
      // Check audit trace
      const trace = update.trace();
      let [, , , upd, enc] = trace.trigger.operation;
      expect(MeldEncoder.jsonFromBuffer(upd, enc)).toMatchObject(
        [{}, { '@id': 'wilma', 'name': 'Wilma' }]);
      expect(trace.applicable).toEqual(trace.trigger);
      expect(trace.resolution).toBeUndefined();
      expect(trace.voids.length).toBe(1);
      [, , , upd, enc] = trace.voids[0].operation;
      expect(MeldEncoder.jsonFromBuffer(upd, enc)).toMatchObject(
        [{}, { '@id': 'fred', 'name': 'Fred' }]);
      expect(trace.voids[0].data
        .equals(EncodedOperation.toBuffer(trace.voids[0].operation))).toBe(true);
      expect(trace.voids[0].attribution).toBeNull();
      //////////////////////////////////////////////////////////////////////////
      // Check snapshot content
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
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' },
          { agree: true }),
        local.join(remote.time))).resolves.toBe(null);
      // Check fred gets voided out
      await expect(willUpdate).resolves.toMatchObject({
        '@delete': [fred],
        '@insert': [wilma]
      });
      // Local time should no longer have ticks from the third process
      expect(local.time.getTicks(third.time)).toBe(0);
    });

    test('does not void pre-agreement operation', async () => {
      // The local has seen another process
      const third = local.fork();
      // Which transacts, seen by remote but not local
      const preOp = third.sentOperation({}, { '@id': 'fred', 'name': 'Fred' });
      remote.join(third.time);
      // We transact, unseen by remote (will be voided)
      await ssd.transact(local.tick().time, { '@insert': barney });
      // See the third-party pre-agreement op (will not be voided)
      await ssd.apply(preOp, local.join(third.time));
      // Now receive agreement from remote
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' },
          { agree: true }),
        local.join(remote.time))).resolves.toBe(null);
      // Check fred gets voided out
      await expect(willUpdate).resolves.toMatchObject({
        '@delete': [barney],
        '@insert': [wilma]
      });
      // Pre-agreement Fred should have survived
      await expect(drain(ssd.read<Describe>({
        '@describe': 'http://test.m-ld.org/fred'
      }))).resolves.toEqual([fred]);
    });

    test('does not void a concurrent agreement', async () => {
      await ssd.transact(local.tick().time, { '@insert': fred, '@agree': true });
      // Not joining with local time here
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, { agree: true }),
        local.join(remote.time))).resolves.toBe(null);
      // Check fred does not get voided out
      const update = await willUpdate;
      expect(update).toMatchObject({ '@insert': [wilma] });
      expect(update).not.toMatchObject({ '@delete': [fred] });
    });

    test('voids an operation in which deleted triples not found', async () => {
      const firstTid = (await ssd.transact(local.tick().time, { '@insert': fred }))!.time.hash;
      remote.join(local.time); // Remote has received our insert
      // Both we and a third process are going to delete Fred
      const third = local.fork();
      await ssd.transact(local.tick().time, { '@delete': fred });
      remote.join(local.time); // Remote has received our delete (but third has not)
      // Third deletes Fred based on the first transaction - this will do nothing
      await ssd.apply(
        third.sentOperation({ 'tid': firstTid, 'o': 'Fred', 'p': '#name', 's': 'fred' }, {}),
        local.join(third.time));
      // Now the remote enacts an agreement, voiding third's operation
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, { agree: true }),
        local.join(remote.time))).resolves.toBe(null);
      // Check fred does not get re-inserted
      await expect(willUpdate).resolves.toMatchObject({ '@insert': [wilma] });
      // Local time should no longer have ticks from the third process
      expect(third.time.ticks).toBe(2);
      expect(local.time.getTicks(third.time)).toBe(1);
    });

    test('voids a fused operation and re-connects', async () => {
      await ssd.transact(local.tick().time, { '@insert': fred });
      remote.join(local.time); // Remote has received our first insert only
      await ssd.transact(local.tick().time, { '@insert': wilma });
      // This should provoke an immediate fusion
      checkpoints.next(JournalCheckPoint.SAVEPOINT);
      // Not joining with local time here
      const willUpdate = firstValueFrom(ssd.updates);
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'barney', 'name': 'Barney' }, { agree: true }),
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

  describe('transport security', () => {
    let local: MockProcess, remote: MockProcess;
    let transportSecurity: MeldTransportSecurity;
    let constraint: MeldConstraint;

    beforeEach(async () => {
      let { left, right } = TreeClock.GENESIS.forked();
      local = new MockProcess(left);
      remote = new MockProcess(right);
      transportSecurity = { wire: data => data };
      constraint = { check: () => Promise.resolve() };
      ssd = new SuSetDataset(state.dataset,
        {},
        combinePlugins([{ transportSecurity, constraints: [constraint] }]),
        {},
        { '@id': 'test', '@domain': 'test.m-ld.org' });
      await ssd.initialise();
      await ssd.resetClock(local.tick().time);
      await ssd.allowTransact();
    });

    test('applies operations wire security', async () => {
      transportSecurity.wire = data => Buffer.concat([Buffer.from([0]), data]);
      const msg = await ssd.transact(local.tick().time, { '@insert': fred });
      const [, , , upd, enc] = msg!.data;
      expect(enc).toEqual([BufferEncoding.MSGPACK, BufferEncoding.SECURE]);
      expect(upd.readUint8(0)).toBe(0);
      const [expectedUpd] = MeldEncoder.bufferFromJson([{}, { '@id': 'fred', name: 'Fred' }]);
      expect(upd.subarray(1).equals(expectedUpd)).toBe(true);
    });

    test('signs operations', async () => {
      transportSecurity.sign = () => ({ pid: 'alice', sig: Buffer.from('alice') });
      const willUpdate = firstValueFrom(ssd.updates);
      const msg = await ssd.transact(local.tick().time, { '@insert': fred });
      expect(msg!.attr!.pid).toBe('alice');
      expect(msg!.attr!.sig.subarray(0, 'alice'.length).toString()).toBe('alice');
      const update = await willUpdate;
      expect(update.trace().trigger.attribution).toEqual(msg!.attr);
    });

    test('verifies operations', async () => {
      transportSecurity.verify = (_, attr) => {
        if (attr == null || attr.sig.subarray(0, attr.pid.length).toString() !== attr.pid)
          throw new Error('Invalid signature');
      };
      const willUpdate = firstValueFrom(ssd.updates);
      const attr = { pid: 'bob', sig: Buffer.from('bob') };
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }, { attr }),
        local.join(remote.time));
      const update = await willUpdate;
      expect(update).toHaveProperty('@insert', [fred]);
      expect(update.trace().trigger.attribution).toEqual(attr);
    });

    test('rejects unverifiable operation', async () => {
      transportSecurity.verify = (_, attr) => {
        if (attr == null || attr.sig.subarray(0, attr.pid.length).toString() !== attr.pid)
          throw new Error('Invalid signature');
      };
      const attr = { pid: 'bob', sig: Buffer.from('claire') };
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }, { attr }),
        local.join(remote.time))).rejects.toThrowError('Invalid signature');
    });

    test('ignores unverifiable operation concurrent with agreement', async () => {
      transportSecurity.verify = () => { throw new Error('Invalid signature'); };
      await ssd.transact(local.tick().time, { '@insert': fred, '@agree': true });
      const attr = { pid: 'bob', sig: Buffer.from('bob') };
      await expect(ssd.apply(
        remote.sentOperation({}, { '@id': 'wilma', 'name': 'Wilma' }, { attr }),
        local.join(remote.time))).resolves.toBe(null);
    });

    test('constraint resolution is locally signed', async () => {
      const aliceAttr = { pid: 'alice', sig: Buffer.from('alice') };
      transportSecurity.sign = () => aliceAttr;
      constraint.apply = async (_, update) => update.assert({ '@delete': wilma });
      const willUpdate = firstValueFrom(ssd.updates);
      await ssd.apply(
        remote.sentOperation({}, { '@id': 'fred', 'name': 'Fred' }),
        local.join(remote.time));
      const update = await willUpdate;
      expect(update).toHaveProperty('@insert', [fred]);
      const trace = update.trace();
      expect(trace.resolution).toBeDefined();
      expect(trace.resolution!.attribution).toEqual(aliceAttr);
    });
  });

  test('enforces operation size limit', async () => {
    ssd = new SuSetDataset(state.dataset, {}, combinePlugins([]), {}, {
      '@id': 'test',
      '@domain': 'test.m-ld.org',
      maxOperationSize: 1
    });
    await ssd.initialise();
    await ssd.resetClock(TreeClock.GENESIS);
    await ssd.allowTransact();
    await expect(ssd.transact(TreeClock.GENESIS.ticked(), { '@insert': fred }))
      .rejects.toThrow();
  });
});