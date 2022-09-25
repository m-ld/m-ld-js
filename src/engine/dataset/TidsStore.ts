import { Triple, tripleIndexKey, TripleMap } from '../quads';
import { MutableOperation, Operation } from '../ops';
import { UUID } from '../MeldEncoding';
import { IndexMatch, IndexSet } from '../indices';
import { Kvps, KvpStore } from './index';
import * as MsgPack from '../msgPack';

/**
 * Persists mappings from triples to transaction IDs (TIDs) in a {@link KvpStore}.
 * Existing mappings are cached, so it's important not to have two of these working against
 * the same underlying store.
 */
export class TidsStore {
  private cache: TripleMap<UUID[]> = new TripleMap();

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
    let tids = this.cache.get(triple);
    if (tids == null) { // Not found in cache
      const encoded = await this.store.get(tripleTidsKey(triple));
      tids = encoded != null ? MsgPack.decode(encoded) as UUID[] : [];
      this.cache.set(triple, tids);
    }
    return tids;
  }

  /**
   * Obtains a {@link Kvps} for applying the given patch to the store.
   * @param patch the patch to be applied
   */
  async commit(patch: PatchTids): Promise<Kvps> {
    const affected = await patch.affected;
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
}

function tripleTidsKey(triple: Triple) {
  return `_qs:ttd:${tripleIndexKey(triple)}`;
}

export class PatchTids extends MutableOperation<[Triple, UUID]> {
  private _affected?: Promise<TripleMap<Set<UUID>>>;

  constructor(
    readonly store: TidsStore,
    patch?: Partial<Operation<[Triple, UUID]>>
  ) {
    super(patch);
  }

  protected constructSet(items?: Iterable<[Triple, UUID]>): IndexSet<[Triple, UUID]> {
    return new PatchTidsSet(items);
  }

  /**
   * Gathers the resultant TIDs after affecting the store with the given patch
   */
  get affected() {
    return this._affected ??= this.loadAffected();
  }

  private async loadAffected() {
    const affected = new TripleMap<Set<UUID>>();
    // First populate existing TIDs for all triples affected
    const tidLoads: Promise<void>[] = [];
    const loadExisting = (triple: Triple) => {
      const tripleTids = new Set<UUID>();
      tidLoads.push(this.store.findTripleTids(triple)
        .then(tids => tids.forEach(tid => tripleTids.add(tid))));
      return tripleTids;
    };
    for (let [triple] of this.deletes)
      affected.with(triple, loadExisting);
    for (let [triple] of this.inserts)
      affected.with(triple, loadExisting);
    await Promise.all(tidLoads);
    // Then process modifications
    for (let [triple, deleted] of this.deletes)
      affected.get(triple)!.delete(deleted);
    for (let [triple, inserted] of this.inserts)
      affected.get(triple)!.add(inserted);
    return affected;
  }

  async stateOf(triple: Triple): Promise<UUID[]> {
    const affectedState = (await this.affected).get(triple);
    return affectedState != null ?
      [...affectedState] : await this.store.findTripleTids(triple);
  }

  /** @override to clear affected cache */
  append(patch: Partial<Operation<[Triple, UUID]>>): this {
    super.append(patch);
    delete this._affected;
    return this;
  }

  /** @override to clear affected cache */
  remove(key: keyof Operation<any>, items: IndexMatch<[Triple, UUID]>): [Triple, UUID][] {
    const removed = super.remove(key, items);
    if (removed.length)
      delete this._affected;
    return removed;
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