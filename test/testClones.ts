import { DatasetClone } from '../src/dataset/DatasetClone';
import { MeldRemotes, DeltaMessage, MeldJournalEntry, MeldLocal } from '../src/m-ld';
import { mock } from 'jest-mock-extended';
import { Observable } from 'rxjs';
import { Dataset, QuadStoreDataset } from '../src/dataset';
import MemDown from 'memdown';
import { TreeClock } from '../src/clocks';

export async function genesisClone(remotes?: MeldRemotes) {
  const dataset = memStore();
  const clone = new DatasetClone(dataset, remotes || mock<MeldRemotes>({
    updates: mock<Observable<DeltaMessage>>(),
    newClock: () => Promise.resolve(TreeClock.GENESIS)
  }), {});
  await clone.initialise();
  return clone;
}

export function memStore(): Dataset {
  return new QuadStoreDataset(new MemDown, { id: 'test' });
}

export function mockLocal(updates: Observable<MeldJournalEntry>): MeldLocal {
  // This weirdness is due to jest-mock-extended trying to mock arrays
  return { ...mock<MeldLocal>(), updates };
}
