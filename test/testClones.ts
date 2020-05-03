import { DatasetClone } from '../src/dataset/DatasetClone';
import { MeldRemotes, DeltaMessage, MeldJournalEntry, MeldLocal } from '../src/m-ld';
import { mock } from 'jest-mock-extended';
import { Observable, of } from 'rxjs';
import { Dataset, QuadStoreDataset } from '../src/dataset';
import MemDown from 'memdown';
import { TreeClock } from '../src/clocks';

export async function genesisClone(remotes?: MeldRemotes) {
  const clone = new DatasetClone(memStore(), remotes ?? mockRemotes());
  await clone.initialise();
  return clone;
}

function mockRemotes(): MeldRemotes {
  return {
    ...mock<MeldRemotes>(),
    setLocal: () => { },
    updates: mock<Observable<DeltaMessage>>(),
    online: of(true),
    newClock: () => Promise.resolve(TreeClock.GENESIS)
  };
}

export function memStore(): Dataset {
  return new QuadStoreDataset(new MemDown, { id: 'test' });
}

export function mockLocal(
  updates: Observable<MeldJournalEntry>,
  online: Observable<boolean>): MeldLocal {
  // This weirdness is due to jest-mock-extended trying to mock arrays
  return { ...mock<MeldLocal>(), updates, online };
}
