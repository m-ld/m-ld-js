import { Triple, tripleIndexKey, TripleMap } from '../quads';
import { MutableOperation } from '../ops';
import { UUID } from '../MeldEncoding';
import { IndexSet } from '../indices';
import { Kvps, KvpStore } from './index';
import { MsgPack } from '../util';

/**
 * Persists mappings from triples to transaction IDs (TIDs) in a {@link KvpStore}.
 * Existing mappings are cached, so it's important not to have two of these working against
 * the same underlying store.
 */
export class TidsStore {
  private cache: TripleMap<Set<UUID>> = new TripleMap();

  constructor(
    private store: KvpStore) {
  }

  /**
   * Retrieves the existing TIDs for the given set of triples, as a {@link TripleMap}.
   * @param triples the triples to retrieve the TIDs for
   * @param includeEmpty if set, the key set of the returned map will always equal the requested
   *   triples; otherwise, triples with no TIDs will be omitted.
   */
  async findTriplesTids(
    triples: Iterable<Triple>, includeEmpty?: 'includeEmpty'): Promise<TripleMap<UUID[]>> {
    const triplesTids = new TripleMap<UUID[]>();
    await Promise.all([...triples].map(async triple => {
      const tripleTids = await this.findTripleTids(triple);
      if (tripleTids.length || includeEmpty)
        triplesTids.set(triple, tripleTids);
    }));
    return triplesTids;
  }

  /**
   * Retrieves the existing TIDs for the given triple. If no TIDs are found the
   * returned array is empty.
   * @param triple the triple to retrieve the TIDs for
   */
  async findTripleTids(triple: Triple): Promise<UUID[]> {
    return [...await this.tripleTids(triple)];
  }

  /**
   * Obtains a {@link Kvps} for applying the given patch to the store.
   * @param patch the patch to be applied
   */
  async commit(patch: PatchTids): Promise<Kvps> {
    const affected = new TripleMap<Set<UUID>>();
    const affect =
      (tripleTids: Iterable<[Triple, UUID]>, effect: (tids: Set<UUID>, tid: UUID) => unknown) =>
        Promise.all([...tripleTids].map(async ([triple, tid]) => {
          const tids = await this.tripleTids(triple);
          affected.set(triple, tids);
          return effect(tids, tid);
        }));
    await affect(patch.deletes, (tids, tid) => tids.delete(tid));
    await affect(patch.inserts, (tids, tid) => tids.add(tid));
    // TODO: Smarter cache eviction
    this.cache.clear();
    return batch => {
      for (let [triple, tids] of affected) {
        if (tids.size)
          batch.put(tripleTidsKey(triple), MsgPack.encode([...tids]));
        else
          batch.del(tripleTidsKey(triple));
      }
    };
  }

  /**
   * @returns Set<UUID> mutable set in the cache
   */
  private async tripleTids(triple: Triple): Promise<Set<UUID>> {
    let tids = this.cache.get(triple);
    if (tids == null) { // Not found in cache
      const encoded = await this.store.get(tripleTidsKey(triple));
      tids = new Set<UUID>(encoded != null ? MsgPack.decode(encoded) : []);
      this.cache.set(triple, tids);
    }
    return tids;
  }
}

function tripleTidsKey(triple: Triple) {
  return `_qs:ttd:${tripleIndexKey(triple)}`;
}

export class PatchTids extends MutableOperation<[Triple, UUID]> {
  protected constructSet(items?: Iterable<[Triple, UUID]>): IndexSet<[Triple, UUID]> {
    return new PatchTidsSet(items);
  }
}

class PatchTidsSet extends IndexSet<[Triple, UUID]> {
  protected construct(ts?: Iterable<[Triple, UUID]>) {
    return new PatchTidsSet(ts);
  }

  protected getIndex([triple, uuid]: [Triple, UUID]) {
    return `${tripleIndexKey(triple)}^${uuid}`;
  }
}