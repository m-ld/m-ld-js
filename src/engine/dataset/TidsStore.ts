import { Triple, tripleFromKey, tripleIndexKey, TripleMap } from '../quads';
import { MutableOperation, Operation } from '../ops';
import { UUID } from '../MeldEncoding';
import { IndexMatch, IndexSet } from '../indices';
import { Kvps, TripleKeyStore } from './index';
import * as MsgPack from '../msgPack';
import { map, takeWhile } from 'rxjs/operators';
import { Consumable } from 'rx-flowable';

/** Prefix for TID keys */
const KEY_PRE = '_qs:ttd:';

/**
 * Persists mappings from triples to transaction IDs (TIDs) in a {@link TripleKeyStore}.
 * Existing mappings are cached, so it's important not to have two of these working against
 * the same underlying store.
 */
export class TidsStore {
  private cache: TripleMap<UUID[]> = new TripleMap();

  constructor(
    private store: TripleKeyStore) {
  }

  /**
   * Retrieves the existing TIDs for the given set of triples, as a {@link TripleMap}.
   * @param triples the triples to retrieve the TIDs for
   * @param includeEmpty if set, the key set of the returned map will always equal the requested
   *   triples; otherwise, triples with no TIDs will be omitted.
   */
  async findTriplesTids(
    triples: Iterable<Triple>,
    includeEmpty?: 'includeEmpty'
  ): Promise<TripleMap<UUID[]>> {
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
    if (tids == null) // Not found in cache
      tids = this.cacheTids(triple, await this.store.get(this.tripleTidsKey(triple)));
    return tids;
  }

  /**
   * Retrieves all triple objects which match the given subject and predicate, for
   * which TIDs exist in this store.
   *
   * @param subject
   * @param predicate
   */
  findTriples(
    subject: Triple['subject'],
    predicate: Triple['predicate']
  ): Consumable<Triple['object']> {
    const gt = this.tripleTidsKey(this.store.rdf.quad(
      subject, predicate, this.store.rdf.variable('any')));
    return this.store.read({ gt }).pipe(
      takeWhile(({ value: [key] }) => key.startsWith(gt)),
      map(({ value: [key, encodedTids], next }) => {
        const triple = tripleFromKey(
          key.slice(KEY_PRE.length), this.store.rdf, this.store.prefixes);
        this.cacheTids(triple, encodedTids);
        return { value: triple.object, next };
      }));
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
          batch.put(this.tripleTidsKey(triple), MsgPack.encode([...tids]));
        else
          batch.del(this.tripleTidsKey(triple));
      }
    };
  }

  tripleTidsKey(triple: Triple) {
    return `${KEY_PRE}${tripleIndexKey(triple, this.store.prefixes)}`;
  }

  private cacheTids(triple: Triple, encodedTids: Buffer | undefined) {
    const tids = encodedTids != null ? MsgPack.decode(encodedTids) as UUID[] : [];
    this.cache.set(triple, tids);
    return tids;
  }
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