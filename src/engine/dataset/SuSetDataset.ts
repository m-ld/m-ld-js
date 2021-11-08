import { MeldConstraint, MeldUpdate } from '../../api';
import { OperationMessage, Snapshot } from '..';
import { GlobalClock, TickTree, TreeClock } from '../clocks';
import { Context, Write } from '../../jrql-support';
import { Dataset, PatchQuads, PatchResult } from '.';
import { JrqlGraph } from './JrqlGraph';
import { MeldEncoder, MeldOperation, TriplesTids, unreify, UUID } from '../MeldEncoding';
import { EMPTY, merge, Observable, of, Subject as Source } from 'rxjs';
import { bufferCount, expand, filter, map, mergeMap, takeWhile } from 'rxjs/operators';
import {
  check, completed, Future, getIdLogger, inflate, inflateArray, observeAsyncIterator
} from '../util';
import { Logger } from 'loglevel';
import { MeldError } from '../MeldError';
import { LocalLock } from '../local';
import { GraphSubject, MeldConfig, Read } from '../..';
import { Quad, Triple, tripleIndexKey, TripleMap } from '../quads';
import { CheckList } from '../../constraints/CheckList';
import { DefaultList } from '../../constraints/DefaultList';
import { InterimUpdatePatch } from './InterimUpdatePatch';
import { ActiveContext } from 'jsonld/lib/context';
import { activeCtx } from '../jsonld';
import { Journal, JournalEntry, JournalState } from '../journal';
import { JournalClerk } from '../journal/JournalClerk';
import { QueryableRdfSource } from '../../rdfjs-support';
import { PatchTids, TidsStore } from './TidsStore';
import { flattenItemTids } from '../ops';
import { EntryBuilder } from '../journal/JournalState';

type DatasetSnapshot = Omit<Snapshot, 'updates'>;

type DatasetConfig = Pick<MeldConfig,
  '@id' | '@domain' | 'maxOperationSize' | 'logLevel' | 'journal'>;

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant operations.
 */
export class SuSetDataset extends MeldEncoder implements QueryableRdfSource {
  private static checkNotClosed =
    check((d: SuSetDataset) => !d.dataset.closed, () => new MeldError('Clone has closed'));

  /** External context used for reads, writes and updates, but not for constraints. */
  /*readonly*/
  userCtx: ActiveContext;

  /*readonly*/
  match: QueryableRdfSource['match'];
  /*readonly*/
  query: QueryableRdfSource['query'];
  /*readonly*/
  countQuads: QueryableRdfSource['countQuads'];

  private /*readonly*/ userGraph: JrqlGraph;
  private readonly tidsStore: TidsStore;
  private readonly journal: Journal;
  private readonly journalClerk: JournalClerk;
  private readonly updateSource: Source<MeldUpdate> = new Source;
  private readonly datasetLock: LocalLock;
  private readonly maxOperationSize: number;
  private readonly log: Logger;
  private readonly constraint: CheckList;

  constructor(
    private readonly dataset: Dataset,
    private readonly context: Context,
    constraints: MeldConstraint[],
    config: DatasetConfig) {
    super(config['@domain'], dataset.rdf);
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.journal = new Journal(dataset, this);
    this.tidsStore = new TidsStore(dataset);
    this.journalClerk = new JournalClerk(this.journal, config);
    // Update notifications are strictly ordered but don't hold up transactions
    this.datasetLock = new LocalLock(config['@id'], dataset.location);
    this.maxOperationSize = config.maxOperationSize ?? Infinity;
    this.constraint = new CheckList(constraints.concat(new DefaultList(config['@id'])));
  }

  @SuSetDataset.checkNotClosed.async
  async initialise() {
    await super.initialise();
    this.userCtx = await activeCtx(this.context);
    this.userGraph = new JrqlGraph(this.dataset.graph());
    // Check for exclusive access to the dataset location
    try {
      await this.datasetLock.acquire();
    } catch (err) {
      throw new MeldError('Clone data is locked', err);
    }
    // Raw RDF methods just pass through to the user graph
    this.match = this.userGraph.match.bind(this.userGraph);
    this.query = this.userGraph.query.bind(this.userGraph);
    this.countQuads = this.userGraph.countQuads.bind(this.userGraph);
  }

  get updates(): Observable<MeldUpdate> {
    return this.updateSource;
  }

  read<R extends Read>(request: R): Observable<GraphSubject> {
    return this.userGraph.read(request, this.userCtx);
  }

  write(request: Write): Promise<PatchQuads> {
    return this.userGraph.write(request, this.userCtx);
  }

  @SuSetDataset.checkNotClosed.async
  async close(err?: any) {
    if (err) {
      this.log.warn('Shutting down due to', err);
      this.updateSource.error(err);
    } else {
      this.log.info('Shutting down normally');
      this.updateSource.complete();
    }
    this.journal.close();
    await this.journalClerk.close();
    this.datasetLock.release();
    return this.dataset.close();
  }

  @SuSetDataset.checkNotClosed.async
  async loadClock(): Promise<TreeClock | undefined> {
    if (await this.journal.initialised())
      return (await this.journal.state()).time;
  }

  @SuSetDataset.checkNotClosed.async
  async resetClock(localTime: TreeClock): Promise<unknown> {
    return this.dataset.transact({
      id: 'suset-reset-clock',
      prepare: async () => {
        return {
          kvps: this.journal.reset(localTime,
            GlobalClock.GENESIS.update(localTime))
        };
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  async saveClock(
    prepare: (gwc: GlobalClock) => Promise<TreeClock> | TreeClock): Promise<TreeClock> {
    return this.dataset.transact<TreeClock>({
      id: 'suset-save-clock',
      prepare: async () => {
        const journal = await this.journal.state(),
          newClock = await prepare(journal.gwc);
        return {
          kvps: journal.withTime(newClock).commit,
          return: newClock
        };
      }
    });
  }

  /**
   * Accepts a remote or local clock and provides operations since the last tick
   * of this dataset that the given clock has 'seen'.
   *
   * To ensure we have processed any prior updates we always process an
   * operations request in a transaction lock.
   *
   * @returns entries from the journal since the given time (exclusive), or
   * `undefined` if the given time is not found in the journal
   */
  @SuSetDataset.checkNotClosed.async
  async operationsSince(time: TickTree, gwc?: Future<GlobalClock>):
    Promise<Observable<OperationMessage> | undefined> {
    return this.dataset.transact<Observable<OperationMessage> | undefined>({
      id: 'suset-ops-since',
      prepare: async () => {
        const journal = await this.journal.state();
        if (gwc != null)
          gwc.resolve(journal.gwc);
        // How many ticks of mine has the requester seen?
        const tick = time.getTicks(journal.time);
        return {
          // If we don't have that tick any more, return undefined
          return: tick < journal.start ? undefined :
            of(await this.journal.entryAfter(tick)).pipe(
              expand(entry => entry != null ? entry.next() : EMPTY),
              takeWhile<JournalEntry>(entry => entry != null),
              // Don't emit an entry if it's all less than the requested time
              filter(entry => time.anyLt(entry.operation.time)),
              map(entry => entry.asMessage()))
        };
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  async transact(
    prepare: () => Promise<[TreeClock, PatchQuads]>): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        const [time, patch] = await prepare();
        if (patch.isEmpty)
          return { return: null };

        txc.sw.next('check-constraints');
        const interim = new InterimUpdatePatch(this.userGraph, time, patch, 'mutable');
        await this.constraint.check(this.userGraph, interim);
        const { update, entailments } = await interim.finalise(this.userCtx);

        txc.sw.next('find-tids');
        const deletedTriplesTids = await this.tidsStore.findTriplesTids(patch.deletes);
        const tid = time.hash();
        const op = this.txnOperation(tid, time, patch.inserts, deletedTriplesTids);

        // Include tid changes in final patch
        txc.sw.next('new-tids');
        const tidPatch = this.txnTidPatch(tid, patch.inserts, deletedTriplesTids);

        // Include journaling in final patch
        txc.sw.next('journal');
        const journal = await this.journal.state();
        const journaling = journal.builder().next(op, time);
        const opMsg = await this.operationMessage(journal, time, op);

        return this.txnResult(
          patch, entailments, tidPatch, journaling, opMsg, update);
      }
    });
  }

  /**
   * Rolls up the given transaction details into a single patch. This method is
   * just a type convenience for ensuring everything needed for a transaction is
   * present.
   */
  private async txnResult(
    assertions: PatchQuads,
    entailments: PatchQuads,
    tidPatch: PatchTids,
    journaling: EntryBuilder,
    op: OperationMessage | null,
    update: MeldUpdate): Promise<PatchResult<OperationMessage | null>> {
    const commitTids = await this.tidsStore.commit(tidPatch);
    this.log.debug(`patch ${journaling.entries.map(e => e.operation.time)}:
    deletes: ${[...assertions.deletes].map(tripleIndexKey)}
    inserts: ${[...assertions.inserts].map(tripleIndexKey)}`);
    return {
      patch: new PatchQuads(assertions).append(entailments),
      kvps: batch => {
        commitTids(batch);
        journaling.commit(batch);
      },
      return: op,
      after: () => this.emitUpdate(update)
    };
  }

  private emitUpdate(update: MeldUpdate) {
    if (update['@delete'].length || update['@insert'].length)
      this.updateSource.next(update);
  }

  private operationMessage(journal: JournalState, time: TreeClock, op: MeldOperation) {
    const prev = journal.gwc.getTicks(time);
    // Construct the operation message with the previous visible clock tick
    const operationMsg = new OperationMessage(prev, op.encoded, time);
    if (operationMsg.size > this.maxOperationSize)
      throw new MeldError('Delta too big');
    return operationMsg;
  }

  private txnTidPatch(
    tid: string, insert: Iterable<Quad>, deletedTriplesTids: TripleMap<UUID[]>) {
    return new PatchTids({
      deletes: flattenItemTids(deletedTriplesTids),
      inserts: [...insert].map(triple => [triple, tid])
    });
  }

  private txnOperation(tid: string, time: TreeClock,
    inserts: Iterable<Quad>, deletes: TripleMap<UUID[]>) {
    return MeldOperation.fromOperation(this, {
      from: time.ticks, time, deletes, inserts: [...inserts].map(triple => [triple, [tid]])
    });
  }

  toUserQuad = (triple: Triple): Quad => this.rdf.quad(
    triple.subject,
    triple.predicate,
    triple.object,
    this.rdf.defaultGraph());

  @SuSetDataset.checkNotClosed.async
  async apply(msg: OperationMessage,
    localTime: TreeClock, cxnTime: TreeClock): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        txc.sw.next('decode-op');
        const journal = await this.journal.state();
        this.log.debug(`Applying operation: ${msg.time} @ ${localTime}`);
        const op = await journal.applicableOperation(MeldOperation.fromEncoded(this, msg.data));

        txc.sw.next('apply-txn');
        const patch = new PatchQuads();
        // Process deletions and inserts
        const insertTids = new TripleMap(op.inserts);
        const tidPatch = await this.processSuDeletions(op.deletes, patch);
        patch.append({ inserts: op.inserts.map(([triple]) => this.toUserQuad(triple)) });

        txc.sw.next('apply-cx'); // "cx" = constraint
        const interim = new InterimUpdatePatch(this.userGraph, cxnTime, patch);
        await this.constraint.apply(this.userGraph, interim);
        const { update, assertions, entailments } = await interim.finalise(this.userCtx);
        const cxn = await this.constraintTxn(assertions, patch, insertTids, cxnTime);
        // After applying the constraint, some new quads might have been removed
        tidPatch.append(new PatchTids({
          inserts: flattenItemTids([...patch.inserts]
            .map(triple => [triple, insertTids.get(triple) ?? []]))
        }));

        // Done determining the applied operation patch. At this point we could
        // have an empty patch, but we still need to complete the journal
        // entry for it.
        txc.sw.next('journal');
        const journaling = journal.builder().next(op, localTime);

        // If the constraint has done anything, we need to merge its work
        if (cxn != null) {
          // update['@ticks'] = cxnTime.ticks;
          tidPatch.append(cxn.tidPatch);
          patch.append(assertions);
          // Also create a journal entry for the constraint "transaction"
          journaling.next(cxn.operation, cxnTime);
        }
        // FIXME: If this operation message exceeds max size, what to do?
        const opMsg = cxn != null && cxn.operation != null ?
          this.operationMessage(journal, cxnTime, cxn.operation) : null;
        return this.txnResult(patch, entailments, tidPatch, journaling, opMsg, update);
      }
    });
  }

  /**
   * The operation's delete contains reifications of deleted triples.
   * This method adds the resolved deletions to the given transaction patch.
   * @return tidPatch patch to the triple TIDs graph
   */
  private processSuDeletions(opDeletions: TriplesTids, patch: PatchQuads): Promise<PatchTids> {
    return opDeletions.reduce(async (tripleTidPatch, [triple, theirTids]) => {
      // For each unique deleted triple, subtract the claimed tids from the tids we have
      const ourTripleTids = await this.tidsStore.findTripleTids(triple);
      const toRemove = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid));
      // If no tids are left, delete the triple in our graph
      if (toRemove.length > 0 && toRemove.length == ourTripleTids.length) {
        patch.append({ deletes: [this.toUserQuad(triple)] });
      } else {
        this.log.debug(`Not removing ${tripleIndexKey(triple)}:
        Ours: ${ourTripleTids}
        Theirs: ${theirTids}`);
      }
      return (await tripleTidPatch).append({ deletes: toRemove.map(tid => [triple, tid]) });
    }, Promise.resolve(new PatchTids()));
  }

  /**
   * Caution: mutates patch
   */
  private async constraintTxn(
    assertions: PatchQuads, patch: PatchQuads, insertTids: TripleMap<UUID[]>, cxnTime: TreeClock) {
    if (!assertions.isEmpty) {
      // Triples that were inserted in the applied transaction may have been
      // deleted by the constraint - these need to be removed from the applied
      // transaction patch but still published in the constraint operation
      const deletedExistingTids = await this.tidsStore.findTriplesTids(assertions.deletes);
      const deletedTriplesTids = new TripleMap(deletedExistingTids);
      patch.remove('inserts', assertions.deletes)
        .forEach(delTriple => deletedTriplesTids.with(delTriple, () => [])
          .push(...(insertTids.get(delTriple) ?? [])));
      // Anything deleted by the constraint that did not exist before the
      // applied transaction can now be removed from the constraint patch
      assertions.remove('deletes', triple => deletedExistingTids.get(triple) == null);
      const cxnId = cxnTime.hash();
      return {
        operation: this.txnOperation(cxnId, cxnTime, assertions.inserts, deletedTriplesTids),
        tidPatch: await this.txnTidPatch(cxnId, assertions.inserts, deletedExistingTids)
      };
    }
  }

  /**
   * Applies a snapshot to this dataset.
   * Caution: uses multiple transactions, so the world must be held up by the caller.
   * @param snapshot snapshot with batches of quads and tids
   * @param localTime the time of the local process, to be saved
   */
  @SuSetDataset.checkNotClosed.async
  async applySnapshot(snapshot: DatasetSnapshot, localTime: TreeClock) {
    await this.dataset.clear();
    await this.dataset.transact({
      id: 'suset-reset',
      prepare: async () =>
        ({ kvps: this.journal.reset(localTime, snapshot.gwc) })
    });
    await completed(inflate(snapshot.data, async batch => this.dataset.transact({
      id: 'snapshot-batch',
      prepare: async () => {
        if ('inserts' in batch) {
          const triplesTids = unreify(this.triplesFromBuffer(batch.inserts));
          // For each triple in the batch, insert the TIDs into the tids graph
          const tidPatch = new PatchTids({ inserts: flattenItemTids(triplesTids) });
          // And include the triples themselves
          const patch = new PatchQuads({
            inserts: triplesTids.map(([triple]) => this.toUserQuad(triple))
          });
          return { kvps: await this.tidsStore.commit(tidPatch), patch };
        } else {
          return { kvps: this.journal.insertPastOperation(batch.operation) };
        }
      }
    })));
  }

  /**
   * Takes a snapshot of data, including transaction IDs and latest operations.
   * The data will be loaded from the same consistent snapshot per LevelDown.
   */
  @SuSetDataset.checkNotClosed.async
  async takeSnapshot(): Promise<DatasetSnapshot> {
    const journal = await this.journal.state();
    return {
      gwc: journal.gwc,
      data: merge(
        observeAsyncIterator(this.userGraph.graph.query()).pipe(
          bufferCount(10), // TODO batch size config
          mergeMap(async batch => {
            const tidQuads = await this.tidsStore.findTriplesTids(batch, 'includeEmpty');
            const reified = this.reifyTriplesTids([...tidQuads]);
            return { inserts: this.bufferFromTriples(reified) };
          })),
        inflateArray(journal.latestOperations()).pipe(
          map(operation => ({ operation }))))
    };
  }
}
