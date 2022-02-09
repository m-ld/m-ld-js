import { GraphSubject, MeldConstraint, MeldExtensions, MeldUpdate, WriteOptions } from '../../api';
import { BufferEncoding, EncodedOperation, OperationMessage, Snapshot } from '..';
import { GlobalClock, TickTree, TreeClock } from '../clocks';
import { Context, Query, Read, Write } from '../../jrql-support';
import { Dataset, PatchQuads, PatchResult } from '.';
import { JrqlGraph } from './JrqlGraph';
import { MeldEncoder, RefTriplesTids, UUID } from '../MeldEncoding';
import { EMPTY, merge, mergeMap, Observable, of, Subject as Source } from 'rxjs';
import { expand, filter, map, takeWhile } from 'rxjs/operators';
import { check, completed, Future, getIdLogger, inflate } from '../util';
import { Logger } from 'loglevel';
import { MeldError } from '../MeldError';
import { Quad, Triple, tripleIndexKey, TripleMap } from '../quads';
import { InterimUpdatePatch } from './InterimUpdatePatch';
import { ActiveContext } from 'jsonld/lib/context';
import { activeCtx } from '../jsonld';
import { EntryBuilder, Journal, JournalEntry, JournalState } from '../journal';
import { JournalClerk } from '../journal/JournalClerk';
import { PatchTids, TidsStore } from './TidsStore';
import { expandItemTids, flattenItemTids } from '../ops';
import { Consumable, flowable } from 'rx-flowable';
import { batch } from 'rx-flowable/operators';
import { consume } from 'rx-flowable/consume';
import { MeldMessageType } from '../../ns/m-ld';
import { CheckList } from '../../constraints/CheckList';
import { JournalAdmin, MeldConfig } from '../../config';
import { MeldOperation } from '../MeldOperation';
import { ClockHolder } from '../messages';

export type DatasetSnapshot = Omit<Snapshot, 'updates'>;

type DatasetConfig = Pick<MeldConfig,
  '@id' | '@domain' | 'maxOperationSize' | 'logLevel' | 'journal'>;

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant operations.
 */
export class SuSetDataset extends MeldEncoder {
  private static checkNotClosed = check((d: SuSetDataset) =>
    !d.dataset.closed, () => new MeldError('Clone has closed'));

  /** External context used for reads, writes and updates, but not for constraints. */
  /*readonly*/
  userCtx: ActiveContext;

  private /*readonly*/ userGraph: JrqlGraph;
  private readonly tidsStore: TidsStore;
  private readonly journal: Journal;
  private readonly journalClerk: JournalClerk;
  private readonly updateSource: Source<MeldUpdate> = new Source;
  private readonly maxOperationSize: number;
  private readonly log: Logger;

  constructor(
    private readonly dataset: Dataset,
    private readonly context: Context,
    private readonly extensions: MeldExtensions,
    config: DatasetConfig,
    journalAdmin?: JournalAdmin
  ) {
    super(config['@domain'], dataset.rdf);
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.journal = new Journal(dataset, this);
    this.tidsStore = new TidsStore(dataset);
    this.journalClerk = new JournalClerk(this.journal, config, journalAdmin);
    this.maxOperationSize = config.maxOperationSize ?? Infinity;
  }

  @SuSetDataset.checkNotClosed.async
  async initialise() {
    await super.initialise();
    this.userCtx = await activeCtx(this.context);
    this.userGraph = new JrqlGraph(this.dataset.graph());
  }

  private get constraint(): MeldConstraint {
    return new CheckList(this.extensions.constraints);
  }

  get readState() {
    return this.userGraph.asReadState;
  }

  get updates(): Observable<MeldUpdate> {
    return this.updateSource;
  }

  read<R extends Read>(request: R): Consumable<GraphSubject> {
    return this.userGraph.read(request, this.userCtx);
  }

  write(request: Write): Promise<PatchQuads> {
    return this.userGraph.write(request, this.userCtx);
  }

  ask(pattern: Query): Promise<boolean> {
    return this.userGraph.ask(pattern, this.userCtx);
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
    await this.journalClerk.close().catch(err => this.log.warn(err));
    return this.dataset.close().catch(err => this.log.warn(err));
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
            GlobalClock.GENESIS.set(localTime),
            TreeClock.GENESIS)
        };
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  async saveClock(
    prepare: (gwc: GlobalClock) => Promise<TreeClock> | TreeClock
  ): Promise<TreeClock> {
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
    prepare: () => Promise<[TreeClock, PatchQuads]>,
    { agree }: WriteOptions = {}
  ): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        const [time, patch] = await prepare();
        if (patch.isEmpty)
          return { return: null };

        txc.sw.next('check-constraints');
        const interim = new InterimUpdatePatch(
          this.userGraph, this.userCtx, time.ticks, patch, 'mutable');
        await this.constraint.check(this.readState, interim);
        const { update, entailments } = await interim.finalise();

        txc.sw.next('find-tids');
        const deletedTriplesTids = await this.tidsStore.findTriplesTids(patch.deletes);
        const tid = time.hash;
        const op = this.txnOperation(tid, time, patch.inserts, deletedTriplesTids, agree);

        // Include tid changes in final patch
        txc.sw.next('new-tids');
        const tidPatch = this.txnTidPatch(tid, patch.inserts, deletedTriplesTids);

        // Include journaling in final patch
        txc.sw.next('journal');
        const journal = await this.journal.state();
        const journaling = journal.builder().next(op, deletedTriplesTids, time);
        const opMsg = await this.operationMessage(journal, time, op);

        return this.txnResult(
          patch, entailments, tidPatch, journaling, opMsg, update);
      }
    });
  }

  /**
   * Rolls up the given transaction details into a single patch to the store.
   */
  private async txnResult(
    assertions: PatchQuads,
    entailments: PatchQuads,
    tidPatch: PatchTids,
    journaling: EntryBuilder,
    opMessage: OperationMessage | null,
    update: MeldUpdate
  ): Promise<PatchResult<OperationMessage | null>> {
    const commitTids = await this.tidsStore.commit(tidPatch);
    this.log.debug(`patch ${journaling.appendEntries.map(e => e.operation.time)}:
    deletes: ${[...assertions.deletes].map(tripleIndexKey)}
    inserts: ${[...assertions.inserts].map(tripleIndexKey)}`);
    return {
      patch: new PatchQuads(assertions).append(entailments),
      kvps: batch => {
        commitTids(batch);
        journaling.commit(batch);
      },
      return: opMessage,
      after: () => this.emitUpdate(update)
    };
  }

  private emitUpdate(update: MeldUpdate) {
    if (update['@delete'].length || update['@insert'].length)
      this.updateSource.next(update);
  }

  private async operationMessage(journal: JournalState, time: TreeClock, op: MeldOperation) {
    const prev = journal.gwc.getTicks(time);
    // Construct the operation message with the previous visible clock tick
    let [version, from, timeJson, update, encoding, agreed] = op.encoded;
    // Apply transport security to the encoded update
    const wired = await this.extensions.transportSecurity.wire(
      update, MeldMessageType.operation, 'out', this.readState);
    if (wired !== update) {
      update = wired;
      encoding = encoding.concat(BufferEncoding.SECURE);
    }
    const operationMsg = new OperationMessage(
      prev, [version, from, timeJson, update, encoding, agreed], time);
    if (operationMsg.size > this.maxOperationSize)
      throw new MeldError('Delta too big');
    return operationMsg;
  }

  private txnTidPatch(
    tid: string,
    insert: Iterable<Quad>,
    deletedTriplesTids: TripleMap<UUID[]>
  ) {
    return new PatchTids({
      deletes: flattenItemTids(deletedTriplesTids),
      inserts: [...insert].map(triple => [triple, tid])
    });
  }

  private txnOperation(
    tid: string,
    time: TreeClock,
    inserts: Iterable<Quad>,
    deletes: TripleMap<UUID[]>,
    agreed?: true
  ) {
    return MeldOperation.fromOperation(this, {
      from: time.ticks,
      time,
      deletes,
      inserts: [...inserts].map(triple => [triple, [tid]]),
      agreed: agreed ? time.ticks : undefined
    });
  }

  toUserQuad = (triple: Triple): Quad => this.rdf.quad(
    triple.subject,
    triple.predicate,
    triple.object,
    this.rdf.defaultGraph());

  /**
   * @param msg the remote message to apply to this dataset
   * @param clockHolder a holder carrying a clock which can be manipulated
   */
  @SuSetDataset.checkNotClosed.async
  async apply(
    msg: OperationMessage,
    clockHolder: ClockHolder<TreeClock>
  ): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        txc.sw.next('decode-op');
        const journal = await this.journal.state();
        const applicableOp = await journal.applicableOperation(
          MeldOperation.fromEncoded(this, await this.unSecureOperation(msg)));
        if (applicableOp == null) {
          this.log.debug(`Ignoring pre-agreement operation: ${msg.time} @ ${clockHolder.peek()}`);
          return { return: null };
        }

        this.log.debug(`Applying operation: ${msg.time} @ ${clockHolder.peek()}`);
        txc.sw.next('apply-txn');
        // TODO: Refactor this txn to a class with patches as members
        const patch = new PatchQuads(), tidPatch = new PatchTids(), journaling = journal.builder();
        if (applicableOp.agreed != null) {
          // TODO: check agreement conditions

          // void more recent entries
          await this.rewindTo(msg.time, patch, tidPatch, journaling);
          // If we now find that we are not ready for the agreement, we need to
          // re-connect to recover what's missing. This could include a rewound
          // local txn. But first, commit the rewind.
          const rewoundJoinTime = journaling.state.time.ticked(msg.time);
          if (rewoundJoinTime.anyLt(msg.time)) {
            clockHolder.push(journaling.state.time); // Not joining
            return this.missingCausesResult(patch, tidPatch, journaling);
          } else {
            clockHolder.push(rewoundJoinTime);
          }
        }
        const opTime = clockHolder.event(), cxnTime = clockHolder.event();

        // Process deletions and inserts
        const insertTids = new TripleMap(applicableOp.inserts);
        const deletions = await this.processSuDeletions(applicableOp.deletes);
        tidPatch.append({ deletes: deletions.tids });
        patch.append({
          deletes: deletions.triples.map(this.toUserQuad),
          inserts: applicableOp.inserts.map(([triple]) => this.toUserQuad(triple))
        });

        txc.sw.next('apply-cx'); // "cx" = constraint
        const interim = new InterimUpdatePatch(
          this.userGraph, this.userCtx, cxnTime.ticks, patch);
        await this.constraint.apply(this.readState, interim);
        const { update, assertions, entailments } = await interim.finalise();
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
        journaling.next(applicableOp, expandItemTids(deletions.tids, new TripleMap), opTime);

        // If the constraint has done anything, we need to merge its work
        if (cxn != null) {
          // update['@ticks'] = cxnTime.ticks;
          tidPatch.append(cxn.tidPatch);
          patch.append(assertions);
          // Also create a journal entry for the constraint "transaction"
          journaling.next(cxn.operation, cxn.deletedTriplesTids, cxnTime);
        }
        // FIXME: If this operation message exceeds max size, what to do?
        const opMsg = cxn != null && cxn.operation != null ?
          await this.operationMessage(journal, cxnTime, cxn.operation) : null;
        return this.txnResult(patch, entailments, tidPatch, journaling, opMsg, update);
      }
    });
  }

  private async rewindTo(
    agreementTime: TreeClock,
    patch: PatchQuads,
    tidPatch: PatchTids,
    journaling: EntryBuilder
  ) {
    for (
      let entry = await this.journal.entryBefore();
      entry != null && agreementTime.anyLt(entry.operation.time);
      entry = await this.journal.entryBefore(entry.key)
    ) {
      const entryOp = entry.operation.asMeldOperation();
      // Reversing a journal entry involves:
      tidPatch.append({
        // 1. Deleting triples that were inserted. The TIDs of the inserted
        // triples always come from the entry itself, so we know exactly
        // what TripleTids were added and we can safely remove them.
        deletes: flattenItemTids(entryOp.inserts),
        // 2. Inserting triples that were deleted. From the MeldOperation
        // by itself we don't know which TripleTids were actually removed
        // (a prior transaction may have removed the same ones). Instead,
        // the journal keeps track of the actual deletes made.
        inserts: flattenItemTids(entryOp.byTriple('deletes', entry.deleted))
      });
      journaling.void(entry);
    }
    // Now compare the affected triple-TIDs to the current state to find
    // actual triples to delete or insert
    for (let [triple, tids] of await this.tidsStore.affected(tidPatch))
      patch.append({ [tids.size ? 'inserts' : 'deletes']: [this.toUserQuad(triple)] });
  }

  private async missingCausesResult(
    patch: PatchQuads,
    tidPatch: PatchTids,
    journaling: EntryBuilder
  ): Promise<PatchResult<null>> {
    const { update } = await new InterimUpdatePatch(
      this.userGraph, this.userCtx, journaling.state.time.ticks, patch
    ).finalise();
    const commitTids = await this.tidsStore.commit(tidPatch);
    return {
      patch,
      kvps: batch => {
        commitTids(batch);
        journaling.commit(batch);
      },
      return: null,
      after: () => {
        this.emitUpdate(update);
        throw new MeldError('Update out of order',
          'Journal rewind missing agreement causes');
      }
    };
  }

  /**
   * Up-applies transport security from the encoded operation in the message
   */
  private async unSecureOperation(msg: OperationMessage): Promise<EncodedOperation> {
    let [version, from, timeJson, updated, encoding, agreed] = msg.data;
    if (encoding[encoding.length - 1] === BufferEncoding.SECURE) {
      updated = await this.extensions.transportSecurity.wire(
        updated, MeldMessageType.operation, 'in', this.readState);
      encoding = encoding.slice(0, -1);
    }
    return [version, from, timeJson, updated, encoding, agreed];
  }

  /**
   * The operation's delete contains reifications of deleted triples. This
   * method resolves the deletions into TID graph changes and deleted triples.
   * Note that the triples for which a TID is being deleted are not necessarily
   * deleted themselves, per SU-Set operation.
   *
   * @return deletions to the triple TIDs and the user graph
   */
  private processSuDeletions(
    opDeletions: RefTriplesTids
  ): Promise<{
    tids: [Triple, UUID][],
    triples: Triple[]
  }> {
    return opDeletions.reduce(async (resultSoFar, [triple, theirTids]) => {
      // For each unique deleted triple, subtract the claimed tids from the tids we have
      const [ourTripleTids, deletions] = await Promise.all(
        [this.tidsStore.findTripleTids(triple), resultSoFar]);
      const toRemove = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid));
      // If no tids are left, delete the triple in our graph
      if (toRemove.length > 0 && toRemove.length == ourTripleTids.length) {
        deletions.triples.push(triple);
      } else {
        this.log.debug(`Not removing ${tripleIndexKey(triple)}:
        Ours: ${ourTripleTids}
        Theirs: ${theirTids}`);
      }
      for (let tid of toRemove)
        deletions.tids.push([triple, tid]);
      return deletions;
    }, Promise.resolve({ tids: [] as [Triple, UUID][], triples: [] as Triple[] }));
  }

  /**
   * Caution: mutates patch
   */
  private async constraintTxn(
    assertions: PatchQuads,
    patch: PatchQuads,
    insertTids: TripleMap<UUID[]>,
    cxnTime: TreeClock
  ) {
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
      const cxnId = cxnTime.hash;
      return {
        operation: this.txnOperation(cxnId, cxnTime, assertions.inserts, deletedTriplesTids),
        tidPatch: await this.txnTidPatch(cxnId, assertions.inserts, deletedExistingTids),
        deletedTriplesTids // This is as-if the constraint was applied in isolation
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
        ({ kvps: this.journal.reset(localTime, snapshot.gwc, snapshot.agreed) })
    });
    await completed(inflate(snapshot.data, async batch => this.dataset.transact({
      id: 'snapshot-batch',
      prepare: async () => {
        if ('inserts' in batch) {
          const triplesTids = MeldEncoder.unreifyTriplesTids(
            this.triplesFromBuffer(batch.inserts, batch.encoding));
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
    const insData = consume(this.userGraph.graph.query()).pipe(
      batch(10), // TODO batch size config
      mergeMap(async ({ value: quads, next }) => {
        const tidQuads = await this.tidsStore.findTriplesTids(quads, 'includeEmpty');
        const reified = this.reifyTriplesTids(this.identifyTriplesTids(tidQuads));
        const [inserts, encoding] = this.bufferFromTriples(reified);
        return { value: { inserts, encoding }, next };
      }));
    const opData = inflate(journal.latestOperations(), operations => {
      return consume(operations.map(operation => ({ operation })));
    });
    return {
      gwc: journal.gwc,
      agreed: journal.agreed,
      data: flowable<Snapshot.Datum>(merge(insData, opData))
    };
  }
}
