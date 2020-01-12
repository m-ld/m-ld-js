import { DatasetClone } from '../src/DatasetClone';
import { MeldRemotes, DeltaMessage } from '../src/meld';
import { mock } from 'jest-mock-extended';
import { Observable } from 'rxjs';
import { Dataset, QuadStoreDataset } from '../src/Dataset';
import MemDown from 'memdown';

export async function genesisClone(remotes?: MeldRemotes) {
  const clone = new DatasetClone(memStore(), remotes || mock<MeldRemotes>({
    updates: mock<Observable<DeltaMessage>>()
  }));
  clone.genesis = true;
  await clone.initialise();
  return clone;
}

export function memStore(): Dataset {
  return new QuadStoreDataset(new MemDown, { id: 'test' });
}
