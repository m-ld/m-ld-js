import { Journal, JournalEntry } from '../src/engine/journal';
import { memStore, MockProcess } from './testClones';
import { MeldEncoder, MeldOperation } from '../src/engine/MeldEncoding';
import { Dataset } from '../src/engine/dataset';
import { TreeClock } from '../src/engine/clocks';
import { take, toArray } from 'rxjs/operators';

describe('Dataset Journal', () => {
  let store: Dataset;
  let journal: Journal;
  let encoder: MeldEncoder;

  /**
   * Operation content doesn't matter to the journal, so in these tests we just commit no-ops.
   * @param process simulated operation source
   */
  function getNoop(process: MockProcess) {
    return MeldOperation.fromEncoded(encoder, process.operated('{}', '{}'));
  }

  function addNoopFrom(process: MockProcess) {
    const op = getNoop(process);
    return store.transact({
      prepare: () => ({
        async kvps(batch) {
          const state = await journal.state();
          return state.builder().next(op, process.time).commit(batch);
        },
        return: op
      })
    });
  }

  beforeEach(async () => {
    store = await memStore();
    encoder = new MeldEncoder('test.m-ld.org', store.rdf);
    journal = new Journal(store, encoder);
  });

  test('starts uninitialised', async () => {
    await expect(journal.initialised()).resolves.toBe(false);
  });

  describe('when reset to snapshot', () => {
    let pastOp: MeldOperation;

    beforeEach(async () => {
      const remote = new MockProcess(TreeClock.GENESIS);
      pastOp = getNoop(remote);
      const local = remote.fork();
      await store.transact({
        prepare: () => ({
          kvps(batch) {
            journal.reset(local.time, local.gwc)(batch);
            journal.insertPastOperation(pastOp.encoded)(batch);
          }
        })
      });
    });

    test('disposes an unreferenced operation', async () => {
      await expect(journal.disposeOperationIfUnreferenced(pastOp.time.hash())).resolves.toBe(true);
      // No entry left in the journal
      await expect(journal.entryAfter()).resolves.toBeUndefined();
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
        commitOp = addNoopFrom(local);
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
        expect(entry).toBeDefined();
        expect(entry.prev).toEqual([local.prev, TreeClock.GENESIS.hash()]);
      });

      test('has the operation', async () => {
        const op = await commitOp;
        const saved = await journal.operation(op.time.hash());
        expect(saved.time.equals(local.time)).toBe(true);
        expect(saved.operation).toEqual(op.encoded);
        expect(saved.tid).toEqual(op.time.hash());
        expect(saved.tick).toEqual(local.time.ticks);
        expect(saved.from).toBe(local.time.ticks);
      });

      test('can get operation past', async () => {
        const op = await commitOp;
        const saved = await journal.operation(op.time.hash());
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
        localOp = await addNoopFrom(local);
        remoteOp = await addNoopFrom(remote);
        [localEntry, remoteEntry] = await expectEntries;
      });

      test('cuts from remote fused operation', async () => {
        const next = getNoop(remote);
        const fused = MeldOperation.fromOperation(encoder, remoteOp.fuse(next));
        const state = await journal.state();
        // Hmm, this duplicates the code in SuSetDataset
        const seenTicks = state.gwc.getTicks(fused.time);
        const seenTid = fused.time.ticked(seenTicks).hash();
        const seenOp = await journal.operation(seenTid);
        expect(seenOp).toBeDefined();
        const cut = await seenOp.cutSeen(fused);
        expect(cut.encoded).toEqual(next.encoded);
      });

      test('can get fused operation past', async () => {
        const next = await addNoopFrom(local);
        const saved = await journal.operation(next.time.hash());
        const expectedFused = MeldOperation.fromOperation(encoder, localOp.fuse(next));
        await expect(saved.fusedPast()).resolves.toEqual(expectedFused.encoded);
      });

      test('can splice in a fused operation', async () => {
        const expectNext = journal.entries.pipe(take(1)).toPromise();
        const nextOp = await addNoopFrom(remote);
        const nextEntry = await expectNext;
        const fused = MeldOperation.fromOperation(encoder, remoteOp.fuse(nextOp));

        await journal.spliceEntries(
          [remoteEntry.index, nextEntry.index],
          JournalEntry.fromOperation(journal, nextEntry.key, remoteEntry.prev, fused));

        const read = await journal.operation(nextOp.time.hash());
        expect(read.operation).toEqual(fused.encoded);
      });
    });
  });
});