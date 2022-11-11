import { Journal, JournalEntry } from '../src/engine/journal';
import { memStore, MockProcess } from './testClones';
import { MeldEncoder } from '../src/engine/MeldEncoding';
import { Dataset } from '../src/engine/dataset';
import { TreeClock } from '../src/engine/clocks';
import { delay, take, toArray } from 'rxjs/operators';
import { EmptyError, firstValueFrom, Observer, of, Subject } from 'rxjs';
import { JournalClerk } from '../src/engine/journal/JournalClerk';
import { JournalAdmin, JournalCheckPoint, JournalConfig } from '../src';
import { MeldOperation } from '../src/engine/MeldOperation';
import { TripleMap } from '../src/engine/quads';

describe('Dataset Journal', () => {
  let store: Dataset;
  let journal: Journal;
  let encoder: MeldEncoder;

  beforeEach(async () => {
    store = await memStore();
    encoder = new MeldEncoder('test.m-ld.org', store.rdf);
    journal = new Journal(store, encoder);
  });

  /**
   * Operation content doesn't matter to the journal, so in these tests we just commit no-ops.
   * Correct behaviour of e.g. fusions is covered in other tests.
   * @param process simulated operation source
   * @param content tuple of deletes and inserts , defaults to no-op
   * @param agree whether this operation is an agreement
   */
  function opAt(process: MockProcess, content: [object, object] = [{}, {}], agree?: true) {
    const [deletes, inserts] = content;
    return MeldOperation.fromEncoded(encoder, process.operated(deletes, inserts, agree));
  }

  function addEntry(
    local: MockProcess, remote?: MockProcess, content: [object, object] = [{}, {}]) {
    const op = opAt(remote ?? local, content);
    const localTime = remote ? local.join(op.time).tick().time : local.time;
    return store.transact({
      prepare: () => ({
        async kvps(batch) {
          const state = await journal.state();
          return state.builder().next(op, new TripleMap, localTime, null).commit(batch);
        },
        return: op
      })
    });
  }

  /** Utility for mixing-in journal clerk testing */
  class TestClerk extends JournalClerk {
    checkpoints: Observer<JournalCheckPoint>;

    constructor(config: JournalConfig = {}, admin: JournalAdmin = {}) {
      const checkpoints = new Subject<JournalCheckPoint>();
      super(journal, () => Promise.resolve(null), {
        '@id': 'test',
        logLevel: 'debug',
        // Disable automatic admin, we use explicit checkpoints, unless override
        journal: { adminDebounce: 0, ...config }
      }, {
        checkpoints,
        // Respond immediately (async) to entries and checkpoints
        schedule: of(0).pipe(delay(0)),
        // Allow override in a test
        ...admin
      });
      this.checkpoints = checkpoints;
    }
  }

  test('starts uninitialised', async () => {
    await expect(journal.initialised()).resolves.toBe(false);
  });

  describe('when reset to snapshot', () => {
    let pastOp: MeldOperation;
    let remote: MockProcess, local: MockProcess;

    beforeEach(async () => {
      // The remote will be genesis
      remote = new MockProcess(TreeClock.GENESIS);
      pastOp = opAt(remote);
      //  We are a new clone starting from after the pastOp
      local = remote.fork();
      // Simulate a snapshot with the past operation
      await store.transact({
        prepare: () => ({
          kvps(batch) {
            journal.reset(remote.time, remote.gwc, TreeClock.GENESIS)(batch);
            journal.insertPastOperation(pastOp.encoded)(batch);
          }
        })
      });
    });

    test('has the past operation', async () => {
      const op = await journal.operation(pastOp.time.hash);
      expect(op).toBeDefined();
    });

    test('does not dispose an operation in the GWC', async () => {
      await expect(journal.disposeOperationIfUnreferenced(pastOp.time.hash)).resolves.toBe(false);
    });

    test('does not dispose a remote operation in the GWC', async () => {
      // We have moved on, but the remote genesis clone is still at the fork
      await addEntry(local);
      await expect(journal.disposeOperationIfUnreferenced(pastOp.time.hash)).resolves.toBe(false);
    });

    test('disposes an unreferenced operation', async () => {
      // Both we and the remote genesis clone have moved on
      const firstOpAfter = await addEntry(local);
      await addEntry(local, remote);
      await expect(journal.disposeOperationIfUnreferenced(pastOp.time.hash)).resolves.toBe(true);
      // No entry left in the journal
      const firstInJournal = (await journal.entryAfter())!;
      expect(firstInJournal.operation.time.equals(firstOpAfter.time)).toBe(true);
    });

    test('clerk does not dispose contiguous op', async () => {
      const clerk = new TestClerk();
      await addEntry(local, remote);
      // Close is the only way to ensure activity is processed
      await clerk.close();
      const op = await journal.operation(pastOp.time.hash);
      expect(op).toBeDefined();
    });

    test('clerk disposes non-contiguous op', async () => {
      const clerk = new TestClerk();
      await addEntry(local);
      remote.join(local.time).tick(true); // Breaks the causal chain at the remote
      await addEntry(local, remote);
      // Close is the only way to ensure clerk activity is fully processed
      await clerk.close();
      const op = await journal.operation(pastOp.time.hash);
      expect(op).toBeUndefined();
    });
  });

  describe('when genesis', () => {
    let local: MockProcess;

    beforeEach(async () => {
      local = new MockProcess(TreeClock.GENESIS);
      await store.transact({
        prepare: () => ({ kvps: journal.reset(local.time, local.gwc, TreeClock.GENESIS) })
      });
    });

    test('is initialised', async () => {
      await expect(journal.initialised()).resolves.toBe(true);
    });

    test('has genesis state', async () => {
      const state = await journal.state();
      expect(state.time.equals(local.time)).toBe(true);
      expect(state.gwc.equals(local.gwc)).toBe(true);
      expect(state.start).toBe(0);
    });

    describe('with a local operation', () => {
      // Using a promise to allow listening to the entry being added
      let commitOp: Promise<MeldOperation>;
      let entry: Promise<JournalEntry>;

      beforeEach(() => {
        entry = firstValueFrom(journal.tail);
        // Don't return the promise because Jest will wait for it
        commitOp = addEntry(local);
      });

      test('emits observable entry', async () => {
        const e = await entry;
        const op = await commitOp;
        expect(e).toBeDefined();
        expect(e.prev).toEqual([0, TreeClock.GENESIS.hash]);
        expect(e.operation.time.equals(local.time)).toBe(true);
        expect(e.operation.encoded).toEqual(op.encoded);
        expect(e.operation.tid).toEqual(op.time.hash);
        expect(e.operation.from).toBe(local.time.ticks);
        await expect(e.next()).resolves.toBeUndefined();
      });

      test('has new state', async () => {
        const state = await commitOp.then(() => journal.state());
        expect(state.time.equals(local.time)).toBe(true);
        expect(state.gwc.equals(local.gwc)).toBe(true);
        expect(state.start).toBe(0);
      });

      test('has prev details for entry', async () => {
        const op = await commitOp;
        const [prevTick, prevTid] = await journal.entryPrev(op.time.hash) ?? [];
        expect(prevTick).toBe(0);
        expect(prevTid).toBe(TreeClock.GENESIS.hash);
      });

      test('has saved the first entry', async () => {
        const entry = await commitOp.then(() => journal.entryAfter());
        expect(entry!.prev).toEqual([local.prev, TreeClock.GENESIS.hash]);
      });

      test('has the operation', async () => {
        const op = await commitOp;
        const saved = await journal.operation(op.time.hash);
        expect(saved!.time.equals(local.time)).toBe(true);
        expect(saved!.encoded).toEqual(op.encoded);
        expect(saved!.tid).toEqual(op.time.hash);
        expect(saved!.tick).toEqual(local.time.ticks);
        expect(saved!.from).toBe(local.time.ticks);
      });

      test('can get operation past', async () => {
        const op = await commitOp;
        const saved = await journal.operation(op.time.hash);
        // Fused past of a single op is just the op
        await expect(saved!.fusedPast()).resolves.toEqual(op.encoded);
      });

      test('reconstitutes the m-ld operation', async () => {
        const op = await commitOp;
        const savedOp = await journal.operation(op.time.hash, 'require');
        expect(savedOp.time.equals(op.time)).toBe(true);
        expect(savedOp.encoded).toEqual(op.encoded);
      });

      test('does not dispose the referenced operation', async () => {
        const op = await commitOp;
        await expect(journal.disposeOperationIfUnreferenced(op.time.hash)).resolves.toBe(false);
      });
    });

    describe('with local and remote operation', () => {
      let remote: MockProcess;
      let localOp: MeldOperation, remoteOp: MeldOperation;
      let localEntry: JournalEntry, remoteEntry: JournalEntry;

      beforeEach(async () => {
        remote = local.fork();
        const expectEntries = firstValueFrom(journal.tail.pipe(take(2), toArray()));
        localOp = await addEntry(local);
        remoteOp = await addEntry(local, remote);
        [localEntry, remoteEntry] = await expectEntries;
      });

      test('cuts from remote fused operation', async () => {
        const next = opAt(remote);
        const fused = MeldOperation.fromOperation(
          encoder, remoteOp.fusion().next(next).commit());
        const state = await journal.state();
        // Hmm, this duplicates the code in SuSetDataset
        const seenTicks = state.gwc.getTicks(fused.time);
        const seenTid = fused.time.ticked(seenTicks).hash;
        const seenOp = await journal.operation(seenTid);
        const cut = await seenOp!.cutSeen(fused);
        expect(cut.encoded).toEqual(next.encoded);
      });

      test('can get fused operation past', async () => {
        await addEntry(local);
        const next = await addEntry(local, remote);
        const saved = await journal.operation(next.time.hash);
        const expectedFused = MeldOperation.fromOperation(
          encoder, remoteOp.fusion().next(next).commit());
        await expect(saved!.fusedPast()).resolves.toEqual(expectedFused.encoded);
      });

      test('can splice in a fused operation', async () => {
        const expectNext = firstValueFrom(journal.tail);
        const nextOp = await addEntry(local, remote);
        const nextEntry = await expectNext;
        const fused = MeldOperation.fromOperation(
          encoder, remoteOp.fusion().next(nextOp).commit());
        await store.transact({
          prepare: () => ({
            kvps: journal.spliceEntries(
              [remoteEntry.index, nextEntry.index],
              [JournalEntry.fromOperation(
                journal, nextEntry.key, remoteEntry.prev, fused, new TripleMap, null)],
              { appending: false })
          })
        });
        const read = await journal.operation(nextOp.time.hash);
        expect(read!.encoded).toEqual(fused.encoded);
      });

      test('fused past crosses a fusion', async () => {
        const expectEntries = firstValueFrom(journal.tail.pipe(take(5), toArray()));
        await addEntry(local);
        const r2o = await addEntry(local, remote);
        const r3o = await addEntry(local, remote);
        await addEntry(local);
        await addEntry(local, remote);
        const [, r2, r3, , r4] = await expectEntries;
        // fuse r2-r3, so journal says l1 -> r1 -> l2 -> (r2-r3) -> l3 -> r4
        const fusedOp = MeldOperation.fromOperation(encoder, r2o.fusion().next(r3o).commit());
        await journal.spliceEntries([r2.index, r3.index],
          [JournalEntry.fromOperation(journal, r3.key, r2.prev, fusedOp, new TripleMap, null)],
          { appending: false });
        // first of causal reduce from r4 should be r1
        const [, from, time] = await r4.operation.fusedPast();
        expect(TreeClock.fromJson(time).equals(r4.operation.time)).toBe(true);
        expect(from).toBe(remoteOp.from);
      });
    });

    describe('with remote after local operation', () => {
      let remote: MockProcess;
      let preForkOp: MeldOperation, preForkEntry: JournalEntry;
      let remoteOp: MeldOperation, remoteEntry: JournalEntry;

      beforeEach(async () => {
        const expectPreForkEntry = firstValueFrom(journal.tail);
        preForkOp = await addEntry(local); // Have an entry before the fork
        preForkEntry = await expectPreForkEntry;
        remote = local.fork();
        const expectRemoteEntry = firstValueFrom(journal.tail);
        remoteOp = await addEntry(local, remote);
        remoteEntry = await expectRemoteEntry;
      });

      test('fused operation past does not cross the fork', async () => {
        const next = await addEntry(local, remote);
        const saved = await journal.operation(next.time.hash);
        const expectedFused = MeldOperation.fromOperation(
          encoder, remoteOp.fusion().next(next).commit());
        await expect(saved!.fusedPast()).resolves.toEqual(expectedFused.encoded);
      });

      test('can drop pre-fork entry', async () => {
        await addEntry(local); // So the GWC no longer has the pre-fork
        // This simulates a journal truncation or a post-fork snapshot
        await journal.spliceEntries([preForkEntry.index], [], { appending: false });
        const next = await addEntry(local, remote);
        const saved = await journal.operation(next.time.hash);
        const expectedFused = MeldOperation.fromOperation(
          encoder, remoteOp.fusion().next(next).commit());
        await expect(saved!.fusedPast()).resolves.toEqual(expectedFused.encoded);
      });

      test('past does not cross the fork if prev is fused', async () => {
        const expectEntries = firstValueFrom(journal.tail.pipe(take(2), toArray()));
        const nextOp1 = await addEntry(local, remote);
        const nextOp2 = await addEntry(local, remote);
        const [nextEntry1, nextEntry2] = await expectEntries;
        const fused = MeldOperation.fromOperation(
          encoder, remoteOp.fusion().next(nextOp1).commit());
        const fusedEntry = JournalEntry.fromOperation(
          journal, nextEntry1.key, remoteEntry.prev, fused, new TripleMap, null);
        await journal.spliceEntries([remoteEntry.index, nextEntry1.index],
          [fusedEntry], { appending: false });
        const expectedFused = MeldOperation.fromOperation(
          encoder, fused.fusion().next(nextOp2).commit());
        await expect(nextEntry2.operation.fusedPast()).resolves.toEqual(expectedFused.encoded);
      });
    });

    describe('Clerk', () => {
      test('does nothing on first local entry', async () => {
        const clerk = new TestClerk();
        const willBeActive = firstValueFrom(clerk.activity.pipe(toArray()));
        await addEntry(local);
        // Close is the only way to ensure activity is processed
        await clerk.close();
        await expect(willBeActive).resolves.toHaveLength(0);
      });

      test('appends local entries to fusion', async () => {
        const clerk = new TestClerk();
        const willBeActive = firstValueFrom(clerk.activity.pipe(toArray()));
        await addEntry(local);
        await addEntry(local);
        await addEntry(local);
        // Close incurs a savepoint, hence expecting a commit
        await clerk.close();
        const activity = await willBeActive;
        expect(activity).toHaveLength(3);
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('appended');
        expect(activity[2].action).toBe('committed');

        const entry = await journal.entryAfter(); // Gets first entry
        // Expect the entry to be a fusion of all three
        expect(entry!.operation.time.equals(local.time)).toBe(true);
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash]);
      });

      test('appends remote entries to fusion', async () => {
        const clerk = new TestClerk();
        const remote = local.fork();
        const willBeActive = firstValueFrom(clerk.activity.pipe(toArray()));
        await addEntry(local);
        await addEntry(local, remote);
        await addEntry(local, remote);
        // Close incurs a savepoint, hence expecting a commit
        await clerk.close();
        const activity = await willBeActive;
        expect(activity).toHaveLength(2);
        expect(activity[0].action).toBe('appended');

        const entry = await journal.entryAfter(local.time.ticks - 1);
        // Expect the entry to be a fusion of the two remote entries
        expect(entry!.operation.time.equals(remote.time)).toBe(true);
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash]);
      });

      test('does not add remote entries to local fusion', async () => {
        const clerk = new TestClerk();
        const willBeActive = firstValueFrom(clerk.activity.pipe(toArray()));
        const localOp = await addEntry(local);
        const remote = local.fork();
        // Add a remote entry immediately after the fork
        await addEntry(local, remote);
        // Close incurs a savepoint, hence expecting a commit
        await clerk.close();
        const activity = await willBeActive;
        expect(activity).toHaveLength(0);

        // Expect the local and remote ops to have independent entries
        const localEntry = (await journal.entryAfter())!;
        expect(localEntry!.operation.time.equals(localOp.time)).toBe(true);
        expect(localEntry!.prev).toEqual([0, TreeClock.GENESIS.hash]);
        const remoteEntry = await journal.entryAfter(localEntry.key);
        expect(remoteEntry!.operation.time.equals(remote.time)).toBe(true);
        expect(remoteEntry!.prev).toEqual([localOp.time.ticks, localOp.time.hash]);
      });

      test('follows one fusion with another', async () => {
        const clerk = new TestClerk();
        const second = local.fork();
        const third = local.fork();
        const willBeActive = firstValueFrom(clerk.activity.pipe(toArray()));
        await addEntry(local, second);
        await addEntry(local, second);
        await addEntry(local, third);
        await addEntry(local, third);
        // Close incurs a savepoint, hence expecting a commit
        await clerk.close();
        const activity = await willBeActive;
        expect(activity).toHaveLength(4);
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('committed');
        expect(activity[2].action).toBe('appended');
        expect(activity[3].action).toBe('committed');

        let entry = await journal.entryAfter();
        // Expect the first entry to be a fusion of the two second entries
        expect(entry!.operation.time.equals(second.time)).toBe(true);
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash]);
        entry = await entry!.next();
        // Expect the second entry to be a fusion of the two third entries
        expect(entry!.operation.time.equals(third.time)).toBe(true);
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash]);
      });

      test('commits savepoint when instructed', async () => {
        const clerk = new TestClerk();
        const willBeActive = firstValueFrom(clerk.activity.pipe(take(2), toArray()));
        await addEntry(local);
        await addEntry(local);
        clerk.checkpoints.next(JournalCheckPoint.SAVEPOINT);

        const activity = await willBeActive;
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('committed');

        const entry = await journal.entryAfter(); // Gets first entry
        // Expect the entry to be a fusion of all three
        expect(entry!.operation.time.equals(local.time)).toBe(true);
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash]);
      });

      test('does nothing if disabled', async () => {
        const clerk = new TestClerk({ adminDebounce: 0 }, { checkpoints: undefined });
        const willBeActive = firstValueFrom(clerk.activity);
        // Do some stuff that would normally prompt some activity
        await addEntry(local);
        await addEntry(local);
        clerk.checkpoints.next(JournalCheckPoint.SAVEPOINT);
        await clerk.close();
        // No clerk operations happened
        await expect(willBeActive).rejects.toBeInstanceOf(EmptyError);
      });

      test('starts new fusion if previous single entry above threshold', async () => {
        const clerk = new TestClerk({ maxEntryFootprint: 1 });
        const willBeActive = firstValueFrom(clerk.activity);
        // Create an entry that will exceed the tiny threshold
        await addEntry(local, undefined, [{}, { '@id': 'fred', 'name': 'Fred' }]);
        await addEntry(local);
        await clerk.close();
        // No clerk operations happened
        await expect(willBeActive).rejects.toBeInstanceOf(EmptyError);
        // Expect two entries
        const entry = await journal.entryAfter(); // Gets first entry
        expect(entry).toBeDefined();
        await expect(entry!.next()).resolves.toBeDefined();
      });

      test('starts new fusion if previous fusion above threshold', async () => {
        const clerk = new TestClerk({ maxEntryFootprint: 1 });
        const willBeActive = firstValueFrom(clerk.activity.pipe(take(2), toArray()));
        // Create an entry that will exceed the tiny threshold
        await addEntry(local);
        await addEntry(local, undefined, [{}, { '@id': 'fred', 'name': 'Fred' }]);
        await addEntry(local);
        // Expect two entries
        const activity = await willBeActive;
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('committed');
      });

      test('commits on admin if fusion above threshold', async () => {
        const clerk = new TestClerk({ maxEntryFootprint: 1 });
        const willBeActive = firstValueFrom(clerk.activity.pipe(take(2), toArray()));
        // Create an entry that will exceed the tiny threshold
        const insertFred = { '@id': 'fred', 'name': 'Fred' };
        await addEntry(local);
        await addEntry(local, undefined, [{}, insertFred]);
        clerk.checkpoints.next(JournalCheckPoint.ADMIN);

        const activity = await willBeActive;
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('committed');
      });
    });
  });
});