import { Journal, JournalEntry } from '../src/engine/journal';
import { memStore, MockProcess } from './testClones';
import { MeldEncoder, MeldOperation } from '../src/engine/MeldEncoding';
import { Dataset } from '../src/engine/dataset';
import { TreeClock } from '../src/engine/clocks';
import { delay, take, toArray } from 'rxjs/operators';
import { Observer, of, Subject } from 'rxjs';
import { CheckPoint, JournalClerk } from '../src/engine/journal/JournalClerk';
import { JournalConfig } from '../src';

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
   */
  function opAt(process: MockProcess, content: [any, any] = [{}, {}]) {
    const [deletes, inserts] = content.map(c => JSON.stringify(c));
    return MeldOperation.fromEncoded(encoder, process.operated(deletes, inserts));
  }

  function addEntry(local: MockProcess, remote?: MockProcess, content: [any, any] = [{}, {}]) {
    const op = opAt(remote ?? local, content);
    const localTime = remote ? local.join(op.time).tick().time : local.time;
    return store.transact({
      prepare: () => ({
        async kvps(batch) {
          const state = await journal.state();
          return state.builder().next(op, localTime).commit(batch);
        },
        return: op
      })
    });
  }

  /** Utility for mixing-in journal clerk testing */
  class TestClerk extends JournalClerk {
    checkpoints: Observer<CheckPoint>;

    constructor(config: JournalConfig = {}) {
      const checkpoints = new Subject<CheckPoint>();
      super(journal, { '@id': 'test', logLevel: 'debug', journal: config }, {
        checkpoints,
        // Respond immediately (async) to entries and checkpoints
        schedule: of(0).pipe(delay(0))
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
      remote = new MockProcess(TreeClock.GENESIS);
      pastOp = opAt(remote);
      local = remote.fork();
      await store.transact({
        prepare: () => ({
          kvps(batch) {
            journal.reset(remote.time, remote.gwc)(batch);
            journal.insertPastOperation(pastOp.encoded)(batch);
          }
        })
      });
    });

    test('has the past operation', async () => {
      const op = await journal.operation(pastOp.time.hash());
      expect(op).toBeDefined();
    });

    test('disposes an unreferenced operation', async () => {
      await expect(journal.disposeOperationIfUnreferenced(pastOp.time.hash())).resolves.toBe(true);
      // No entry left in the journal
      await expect(journal.entryAfter()).resolves.toBeUndefined();
    });

    test('clerk does not dispose contiguous op', async () => {
      const clerk = new TestClerk();
      await addEntry(local, remote);
      // Close is the only way to ensure activity is processed
      await clerk.close();
      const op = await journal.operation(pastOp.time.hash());
      expect(op).toBeDefined();
    });

    test('clerk disposes non-contiguous op', async () => {
      const clerk = new TestClerk();
      await addEntry(local);
      remote.join(local.time).tick(true); // Breaks the causal chain at the remote
      await addEntry(local, remote);
      // Close is the only way to ensure clerk activity is fully processed
      await clerk.close();
      const op = await journal.operation(pastOp.time.hash());
      expect(op).toBeUndefined();
    });
  });

  describe('when genesis', () => {
    let local: MockProcess;

    beforeEach(async () => {
      local = new MockProcess(TreeClock.GENESIS);
      await store.transact({
        prepare: () => ({ kvps: journal.reset(local.time, local.gwc) })
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

      beforeEach(() => {
        // Don't return the promise because Jest will wait for it
        commitOp = addEntry(local);
      });

      test('emits observable entry', async () => {
        const entry = await journal.tail.pipe(take(1)).toPromise();
        const op = await commitOp;
        expect(entry).toBeDefined();
        expect(entry.prev).toEqual([0, TreeClock.GENESIS.hash()]);
        expect(entry.operation.time.equals(local.time)).toBe(true);
        expect(entry.operation.operation).toEqual(op.encoded);
        expect(entry.operation.tid).toEqual(op.time.hash());
        expect(entry.operation.from).toBe(local.time.ticks);
        await expect(entry.next()).resolves.toBeUndefined();
      });

      test('has new state', async () => {
        const state = await commitOp.then(() => journal.state());
        expect(state.time.equals(local.time)).toBe(true);
        expect(state.gwc.equals(local.gwc)).toBe(true);
        expect(state.start).toBe(0);
      });

      test('has prev details for entry', async () => {
        const op = await commitOp;
        const [prevTick, prevTid] = await journal.entryPrev(op.time.hash()) ?? [];
        expect(prevTick).toBe(0);
        expect(prevTid).toBe(TreeClock.GENESIS.hash());
      });

      test('has saved the first entry', async () => {
        const entry = await commitOp.then(() => journal.entryAfter());
        expect(entry!.prev).toEqual([local.prev, TreeClock.GENESIS.hash()]);
      });

      test('has the operation', async () => {
        const op = await commitOp;
        const saved = await journal.operation(op.time.hash());
        expect(saved!.time.equals(local.time)).toBe(true);
        expect(saved!.operation).toEqual(op.encoded);
        expect(saved!.tid).toEqual(op.time.hash());
        expect(saved!.tick).toEqual(local.time.ticks);
        expect(saved!.from).toBe(local.time.ticks);
      });

      test('can get operation past', async () => {
        const op = await commitOp;
        const saved = await journal.operation(op.time.hash());
        // Fused past of a single op is just the op
        await expect(saved!.fusedPast()).resolves.toEqual(op.encoded);
      });

      test('reconstitutes the m-ld operation', async () => {
        const op = await commitOp;
        const saved = await journal.meldOperation(op.time.hash());
        expect(saved.time.equals(op.time)).toBe(true);
        expect(saved.encoded).toEqual(op.encoded);
      });

      test('does not dispose the referenced operation', async () => {
        const op = await commitOp;
        await expect(journal.disposeOperationIfUnreferenced(op.time.hash())).resolves.toBe(false);
      });
    });

    describe('with local and remote operation', () => {
      let remote: MockProcess;
      let localOp: MeldOperation, remoteOp: MeldOperation;
      let localEntry: JournalEntry, remoteEntry: JournalEntry;

      beforeEach(async () => {
        remote = local.fork();
        const expectEntries = journal.tail.pipe(take(2), toArray()).toPromise();
        localOp = await addEntry(local);
        remoteOp = await addEntry(local, remote);
        [localEntry, remoteEntry] = await expectEntries;
      });

      test('cuts from remote fused operation', async () => {
        const next = opAt(remote);
        const fused = MeldOperation.fromOperation(encoder, remoteOp.fuse(next));
        const state = await journal.state();
        // Hmm, this duplicates the code in SuSetDataset
        const seenTicks = state.gwc.getTicks(fused.time);
        const seenTid = fused.time.ticked(seenTicks).hash();
        const seenOp = await journal.operation(seenTid);
        const cut = await seenOp!.cutSeen(fused);
        expect(cut.encoded).toEqual(next.encoded);
      });

      test('can get fused operation past', async () => {
        await addEntry(local);
        const next = await addEntry(local, remote);
        const saved = await journal.operation(next.time.hash());
        const expectedFused = MeldOperation.fromOperation(encoder, remoteOp.fuse(next));
        await expect(saved!.fusedPast()).resolves.toEqual(expectedFused.encoded);
      });

      test('can splice in a fused operation', async () => {
        const expectNext = journal.tail.pipe(take(1)).toPromise();
        const nextOp = await addEntry(local, remote);
        const nextEntry = await expectNext;
        const fused = MeldOperation.fromOperation(encoder, remoteOp.fuse(nextOp));

        await journal.spliceEntries(
          [remoteEntry.index, nextEntry.index],
          JournalEntry.fromOperation(journal, nextEntry.key, remoteEntry.prev, fused));

        const read = await journal.operation(nextOp.time.hash());
        expect(read!.operation).toEqual(fused.encoded);
      });
    });

    describe('Clerk', () => {
      test('does nothing on first local entry', async () => {
        const clerk = new TestClerk();
        const willBeActive = clerk.activity.pipe(toArray()).toPromise();
        await addEntry(local);
        // Close is the only way to ensure activity is processed
        await clerk.close();
        await expect(willBeActive).resolves.toHaveLength(0);
      });

      test('appends local entries to fusion', async () => {
        const clerk = new TestClerk();
        const willBeActive = clerk.activity.pipe(toArray()).toPromise();
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
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash()]);
      });

      test('appends remote entries to fusion', async () => {
        const clerk = new TestClerk();
        const remote = local.fork();
        const willBeActive = clerk.activity.pipe(toArray()).toPromise();
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
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash()]);
      });

      test('follows one fusion with another', async () => {
        const clerk = new TestClerk();
        const second = local.fork();
        const third = local.fork();
        const willBeActive = clerk.activity.pipe(toArray()).toPromise();
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
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash()]);
        entry = await entry!.next();
        // Expect the second entry to be a fusion of the two third entries
        expect(entry!.operation.time.equals(third.time)).toBe(true);
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash()]);
      });

      test('commits savepoint when instructed', async () => {
        const clerk = new TestClerk();
        const willBeActive = clerk.activity.pipe(take(2), toArray()).toPromise();
        await addEntry(local);
        await addEntry(local);
        clerk.checkpoints.next(CheckPoint.SAVEPOINT);

        const activity = await willBeActive;
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('committed');

        const entry = await journal.entryAfter(); // Gets first entry
        // Expect the entry to be a fusion of all three
        expect(entry!.operation.time.equals(local.time)).toBe(true);
        expect(entry!.prev).toEqual([0, TreeClock.GENESIS.hash()]);
      });

      test('starts new fusion if previous single entry above threshold', async () => {
        const clerk = new TestClerk({ maxEntryFootprint: 1 });
        const willBeActive = clerk.activity.toPromise();
        // Create an entry that will exceed the tiny threshold
        await addEntry(local, undefined, [{}, { '@id': 'fred', 'name': 'Fred' }]);
        await addEntry(local);
        await clerk.close();
        // No clerk operations happened
        await expect(willBeActive).resolves.toBeUndefined();
        // Expect two entries
        const entry = await journal.entryAfter(); // Gets first entry
        expect(entry).toBeDefined();
        await expect(entry!.next()).resolves.toBeDefined();
      });

      test('starts new fusion if previous fusion above threshold', async () => {
        const clerk = new TestClerk({ maxEntryFootprint: 1 });
        const willBeActive = clerk.activity.pipe(take(2), toArray()).toPromise();
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
        const willBeActive = clerk.activity.pipe(take(2), toArray()).toPromise();
        // Create an entry that will exceed the tiny threshold
        const insertFred = { '@id': 'fred', 'name': 'Fred' };
        await addEntry(local);
        await addEntry(local, undefined, [{}, insertFred]);
        clerk.checkpoints.next(CheckPoint.ADMIN);

        const activity = await willBeActive;
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('committed');
      });
    });
  });
});