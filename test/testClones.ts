import { DatasetClone } from '../src/dataset/DatasetClone';
import { MeldRemotes, DeltaMessage } from '../src/m-ld';
import { mock } from 'jest-mock-extended';
import { Observable } from 'rxjs';
import { Dataset, QuadStoreDataset } from '../src/dataset';
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
