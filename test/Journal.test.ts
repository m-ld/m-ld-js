import { Journal, JournalEntry } from '../src/engine/journal';
import { memStore, MockProcess } from './testClones';
import { MeldEncoder, MeldOperation } from '../src/engine/MeldEncoding';
import { Dataset } from '../src/engine/dataset';
import { TreeClock } from '../src/engine/clocks';
import { take, toArray } from 'rxjs/operators';
import { Observer, of, Subject } from 'rxjs';
import { CheckPoint, JournalClerk } from '../src/engine/journal/JournalClerk';

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
   */
  function noopAt(process: MockProcess) {
    return MeldOperation.fromEncoded(encoder, process.operated('{}', '{}'));
  }

  function addEntry(local: MockProcess, op: MeldOperation) {
    local.join(op.time);
    return store.transact({
      prepare: () => ({
        async kvps(batch) {
          const state = await journal.state();
          return state.builder().next(op, local.time).commit(batch);
        },
        return: op
      })
    });
  }

  /** Utility for mixing-in journal clerk testing */
  class TestClerk extends JournalClerk {
    checkpoints: Observer<CheckPoint>;

    constructor() {
      const checkpoints = new Subject<CheckPoint>();
      super(journal, { '@id': 'test' }, {
        checkpoints,
        // Respond immediately to entries and checkpoints
        schedule: of(0)
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
      pastOp = noopAt(remote);
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
      await addEntry(local, noopAt(remote));
      // Close is the only way to ensure activity is processed
      await clerk.close();
      const op = await journal.operation(pastOp.time.hash());
      expect(op).toBeDefined();
    });

    test('clerk disposes non-contiguous op', async () => {
      const clerk = new TestClerk();
      await addEntry(local, noopAt(local));
      remote.join(local.time).tick(true); // Breaks the causal chain at the remote
      await addEntry(local, noopAt(remote));
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
        commitOp = addEntry(local, noopAt(local));
      });

      test('emits observable entry', async () => {
        const entry = await journal.entries.pipe(take(1)).toPromise();
        const op = await commitOp;
        expect(entry).toBeDefined();
        expect(entry.prev).toEqual([local.prev, TreeClock.GENESIS.hash()]);
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
        const [prevTick, prevTid] = await journal.entryPrev(op.time.hash());
        expect(prevTick).toBe(local.prev);
        expect(prevTid).toBe(TreeClock.GENESIS.hash());
      });

      test('has saved the first entry', async () => {
        const entry = await commitOp.then(() => journal.entryAfter());
        if (entry == null) return fail();
        expect(entry.prev).toEqual([local.prev, TreeClock.GENESIS.hash()]);
      });

      test('has the operation', async () => {
        const op = await commitOp;
        const saved = await journal.operation(op.time.hash());
        if (saved == null) return fail();
        expect(saved.time.equals(local.time)).toBe(true);
        expect(saved.operation).toEqual(op.encoded);
        expect(saved.tid).toEqual(op.time.hash());
        expect(saved.tick).toEqual(local.time.ticks);
        expect(saved.from).toBe(local.time.ticks);
      });

      test('can get operation past', async () => {
        const op = await commitOp;
        const saved = await journal.operation(op.time.hash());
        if (saved == null) return fail();
        // Fused past of a single op is just the op
        await expect(saved.fusedPast()).resolves.toEqual(op.encoded);
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
        const expectEntries = journal.entries.pipe(take(2), toArray()).toPromise();
        localOp = await addEntry(local, noopAt(local));
        remoteOp = await addEntry(local, noopAt(remote));
        [localEntry, remoteEntry] = await expectEntries;
      });

      test('cuts from remote fused operation', async () => {
        const next = noopAt(remote);
        const fused = MeldOperation.fromOperation(encoder, remoteOp.fuse(next));
        const state = await journal.state();
        // Hmm, this duplicates the code in SuSetDataset
        const seenTicks = state.gwc.getTicks(fused.time);
        const seenTid = fused.time.ticked(seenTicks).hash();
        const seenOp = await journal.operation(seenTid);
        if (seenOp == null) return fail();
        const cut = await seenOp.cutSeen(fused);
        expect(cut.encoded).toEqual(next.encoded);
      });

      test('can get fused operation past', async () => {
        const next = await addEntry(local, noopAt(local));
        const saved = await journal.operation(next.time.hash());
        if (saved == null) return fail();
        const expectedFused = MeldOperation.fromOperation(encoder, localOp.fuse(next));
        await expect(saved.fusedPast()).resolves.toEqual(expectedFused.encoded);
      });

      test('can splice in a fused operation', async () => {
        const expectNext = journal.entries.pipe(take(1)).toPromise();
        const nextOp = await addEntry(local, noopAt(remote));
        const nextEntry = await expectNext;
        const fused = MeldOperation.fromOperation(encoder, remoteOp.fuse(nextOp));

        await journal.spliceEntries(
          [remoteEntry.index, nextEntry.index],
          JournalEntry.fromOperation(journal, nextEntry.key, remoteEntry.prev, fused));

        const read = await journal.operation(nextOp.time.hash());
        if (read == null) return fail();
        expect(read.operation).toEqual(fused.encoded);
      });
    });

    describe('Clerk', () => {
      let clerk: TestClerk;

      beforeEach(() => {
        clerk = new TestClerk();
      });

      test('does nothing on first local entry', async () => {
        const willBeActive = clerk.activity.pipe(toArray()).toPromise();
        await addEntry(local, noopAt(local));
        // Close is the only way to ensure activity is processed
        await clerk.close();
        await expect(willBeActive).resolves.toHaveLength(0);
      });

      test('appends local entries to fusion', async () => {
        const willBeActive = clerk.activity.pipe(toArray()).toPromise();
        await addEntry(local, noopAt(local));
        await addEntry(local, noopAt(local));
        await addEntry(local, noopAt(local));
        // Close incurs a savepoint, hence expecting a commit
        await clerk.close();
        const activity = await willBeActive;
        expect(activity).toHaveLength(3);
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('appended');
        expect(activity[2].action).toBe('committed');

        const entry = await journal.entryAfter(); // Gets first entry
        if (entry == null) return fail();
        // Expect the entry to be a fusion of all three
        expect(entry.operation.time.equals(local.time)).toBe(true);
        expect(entry.prev).toEqual([0, TreeClock.GENESIS.hash()]);
      });

      test('appends remote entries to fusion', async () => {
        const remote = local.fork();
        const willBeActive = clerk.activity.pipe(toArray()).toPromise();
        await addEntry(local, noopAt(local));
        await addEntry(local, noopAt(remote));
        await addEntry(local, noopAt(remote));
        // Close incurs a savepoint, hence expecting a commit
        await clerk.close();
        const activity = await willBeActive;
        expect(activity).toHaveLength(2);
        expect(activity[0].action).toBe('appended');

        const entry = await journal.entryAfter(local.time.ticks - 1);
        if (entry == null) return fail();
        // Expect the entry to be a fusion of the two remote entries
        expect(entry.operation.time.equals(remote.time)).toBe(true);
        expect(entry.prev).toEqual([0, TreeClock.GENESIS.hash()]);
      });

      test('commits savepoint when instructed', async () => {
        const willBeActive = clerk.activity.pipe(take(2), toArray()).toPromise();
        await addEntry(local, noopAt(local));
        await addEntry(local, noopAt(local));
        clerk.checkpoints.next(CheckPoint.SAVEPOINT);

        const activity = await willBeActive;
        expect(activity[0].action).toBe('appended');
        expect(activity[1].action).toBe('committed');

        const entry = await journal.entryAfter(); // Gets first entry
        if (entry == null) return fail();
        // Expect the entry to be a fusion of all three
        expect(entry.operation.time.equals(local.time)).toBe(true);
        expect(entry.prev).toEqual([0, TreeClock.GENESIS.hash()]);
      });
    });
  });
});