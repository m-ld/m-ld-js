import {
  GraphSubject, GraphUpdate, MeldConstraint, MeldExtensions, MeldUpdate, noTransportSecurity
} from '../../api';
import { BufferEncoding, EncodedOperation, OperationMessage, Snapshot } from '..';
import { GlobalClock, TickTree, TreeClock } from '../clocks';
import { Context, Query, Read, Write } from '../../jrql-support';
import { Dataset, PatchQuads, PatchResult, TxnContext } from '.';
import { JrqlGraph } from './JrqlGraph';
import { MeldEncoder, UUID } from '../MeldEncoding';
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
import { JournalAdmin, MeldConfig } from '../../config';
import { MeldOperation } from '../MeldOperation';
import { ClockHolder } from '../messages';

export type DatasetSnapshot = Omit<Snapshot, 'updates'>;

type DatasetConfig = Pick<MeldConfig,
  '@id' | '@domain' | 'maxOperationSize' | 'logLevel' | 'journal'>;

type SuSetDataPatch = { quads: PatchQuads, tids: PatchTids };

/**
 * Writeable Graph, similar to a Dataset, but with a slightly different transaction API.
 * Journals every transaction and creates m-ld compliant operations.
 */
export class SuSetDataset extends MeldEncoder {
  private static checkNotClosed = check((d: SuSetDataset) =>
    !d.dataset.closed, () => new MeldError('Clone has closed'));
  private static checkStateLocked =
    check((ssd: SuSetDataset) => ssd.dataset.lock.state('state') !== null,
      () => new MeldError('Unknown error', 'Clone state not locked'));
  private static checkReadyForTxn = check((d: SuSetDataset) =>
    d.readyForTxn, () => new MeldError('Unknown error', 'Dataset not ready'));

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
  private readyForTxn = false;

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

  private get transportSecurity() {
    return this.extensions.transportSecurity ?? noTransportSecurity;
  }

  get readState() {
    return this.userGraph.asReadState;
  }

  get updates(): Observable<MeldUpdate> {
    return this.updateSource;
  }

  @SuSetDataset.checkStateLocked.async
  async allowTransact() {
    await this.extensions.initialise?.(this.readState);
    this.readyForTxn = true;
  }

  @SuSetDataset.checkStateLocked.rx
  read<R extends Read>(request: R): Consumable<GraphSubject> {
    return this.userGraph.read(request, this.userCtx);
  }

  @SuSetDataset.checkStateLocked.async
  write(request: Write): Promise<PatchQuads> {
    return this.userGraph.write(request, this.userCtx);
  }

  @SuSetDataset.checkStateLocked.async
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
  @SuSetDataset.checkStateLocked.async
  @SuSetDataset.checkReadyForTxn.async
  async transact(
    prepare: () => Promise<[TreeClock, PatchQuads, any?]>
  ): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        const [time, patch, agree] = await prepare();
        if (patch.isEmpty)
          return { return: null };

        txc.sw.next('check-constraints');
        const txn = await this.assertConstraints(patch, 'check');

        txc.sw.next('find-tids');
        const deletedTriplesTids = await this.tidsStore.findTriplesTids(txn.assertions.deletes);
        const tid = time.hash;
        const op = this.txnOperation(tid, time, txn.assertions.inserts, deletedTriplesTids, agree);

        // Include tid changes in final patch
        txc.sw.next('new-tids');
        const tidPatch = this.txnTidPatch(tid, txn.assertions.inserts, deletedTriplesTids);

        // Include journaling in final patch
        txc.sw.next('journal');
        const journal = await this.journal.state();
        const journaling = journal.builder().next(op, deletedTriplesTids, time);
        const opMessage = await this.operationMessage(journal, time, op);

        return this.txnResult({
          ...txn, tidPatch, journaling, opMessage
        });
      }
    });
  }

  private async assertConstraints(patch: PatchQuads, verb: keyof MeldConstraint) {
    const interim = new InterimUpdatePatch(
      this.userGraph, this.userCtx, patch, { mutable: verb === 'check' });
    for (let constraint of this.extensions.constraints ?? [])
      await constraint[verb](this.readState, interim);
    return interim.finalise();
  }

  /**
   * Rolls up the given transaction details into a single patch to the store.
   */
  private async txnResult(txn: {
    assertions: PatchQuads,
    entailments: PatchQuads,
    tidPatch: PatchTids,
    journaling: EntryBuilder,
    opMessage: OperationMessage | null,
    internalUpdate: GraphUpdate,
    userUpdate: GraphUpdate
  }): Promise<PatchResult<OperationMessage | null>> {
    const commitTids = await this.tidsStore.commit(txn.tidPatch);
    this.log.debug(`patch ${txn.journaling.appendEntries.map(e => e.operation.time)}:
    deletes: ${[...txn.assertions.deletes].map(tripleIndexKey)}
    inserts: ${[...txn.assertions.inserts].map(tripleIndexKey)}`);
    return {
      patch: new PatchQuads(txn.assertions).append(txn.entailments),
      kvps: batch => {
        commitTids(batch);
        txn.journaling.commit(batch);
      },
      return: txn.opMessage,
      after: async () => {
        if (this.readyForTxn && this.extensions.onUpdate != null)
          await this.extensions.onUpdate(txn.internalUpdate, this.readState);
        this.emitUpdate({
          ...txn.userUpdate,
          '@ticks': txn.journaling.state.time.ticks
        });
      }
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
    const wired = await this.transportSecurity.wire(
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
    return new PatchTids(this.tidsStore, {
      deletes: flattenItemTids(deletedTriplesTids),
      inserts: [...insert].map(triple => [triple, tid])
    });
  }

  private txnOperation(
    tid: string,
    time: TreeClock,
    inserts: Iterable<Quad>,
    deletes: TripleMap<UUID[]>,
    agreed?: any
  ) {
    return MeldOperation.fromOperation(this, {
      from: time.ticks,
      time,
      deletes,
      inserts: [...inserts].map(triple => [triple, [tid]]),
      agreed: agreed ? { tick: time.ticks, proof: agreed } : undefined
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
  @SuSetDataset.checkStateLocked.async
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
        } else {
          txc.sw.next('apply-txn');
          this.log.debug(`Applying operation: ${msg.time} @ ${clockHolder.peek()}`);
          return new SuSetDataset.OperationApplication(
            this, applicableOp, journal.builder(), txc, clockHolder).apply();
        }
      }
    });
  }

  private static OperationApplication = class {
    constructor(
      private ssd: SuSetDataset,
      private op: MeldOperation,
      private journaling: EntryBuilder,
      private txc: TxnContext,
      private clockHolder: ClockHolder<TreeClock>
    ) {}

    /**
     * Processes our operation into a patch to the dataset
     * @param patch quadstore and tid-store patch components
     * @param processAgreement whether to process an agreement in the operation
     */
    async apply(
      patch: SuSetDataPatch = { quads: new PatchQuads(), tids: new PatchTids(this.ssd.tidsStore) },
      processAgreement = true
    ): Promise<PatchResult<OperationMessage | null>> {
      // Process deletions and inserts
      const deleteTids = await this.processSuSetOpToPatch(patch);

      this.txc.sw.next('apply-cx'); // "cx" = constraint
      const { assertions: cxnAssertions, ...txn } =
        await this.ssd.assertConstraints(patch.quads, 'apply');

      if (processAgreement && this.op.agreed != null) {
        // Check agreement conditions. This is done against the non-rewound
        // state, because we may have to recover if the rewind goes back too
        // far. This is allowed because an agreement condition should only
        // inspect previously agreed state.
        for (let agreementCondition of this.ssd.extensions.agreementConditions ?? [])
          await agreementCondition.check(
            this.ssd.readState, txn.internalUpdate, this.op.agreed.proof);
        if (this.op.time.anyLt(this.journaling.state.time)) {
          // A rewind is required. This trumps the work we have already done.
          this.txc.sw.next('rewind');
          return this.rewindAndReapply();
        }
      }

      const opTime = this.clockHolder.event(), cxnTime = this.clockHolder.event();
      const insertTids = new TripleMap(this.op.inserts);
      const cxn = await this.constraintTxn(cxnAssertions, patch.quads, insertTids, cxnTime);
      // After applying the constraint, some new quads might have been removed
      patch.tids.append({
        inserts: flattenItemTids([...patch.quads.inserts]
          .map(triple => [triple, insertTids.get(triple) ?? []]))
      });

      // Done determining the applied operation patch. At this point we could
      // have an empty patch, but we still need to complete the journal entry.
      this.txc.sw.next('journal');
      this.journaling.next(this.op, expandItemTids(deleteTids, new TripleMap), opTime);

      // If the constraint has done anything, we need to merge its work
      if (cxn != null) {
        // update['@ticks'] = cxnTime.ticks;
        patch.tids.append(cxn.tidPatch);
        patch.quads.append(cxnAssertions);
        // Also create a journal entry for the constraint "transaction"
        this.journaling.next(cxn.operation, cxn.deletedTriplesTids, cxnTime);
      }
      // FIXME: If this operation message exceeds max size, what to do?
      return this.ssd.txnResult({
        ...txn,
        assertions: patch.quads,
        tidPatch: patch.tids,
        journaling: this.journaling,
        opMessage: cxn != null && cxn.operation != null ?
          await this.ssd.operationMessage(this.journaling.state, cxnTime, cxn.operation) : null
      });
    }

    private async rewindAndReapply() {
      // void more recent entries in conflict with the agreement
      const patch = await this.rewind();
      // If we now find that we are not ready for the agreement, we need to
      // re-connect to recover what's missing. This could include a rewound
      // local txn. But first, commit the rewind.
      const localTime = this.journaling.state.time;
      const rewoundJoinTime = localTime.ticked(this.op.time);
      if (rewoundJoinTime.anyLt(this.op.time)) {
        this.clockHolder.push(localTime); // Not joining
        return this.missingCausesResult(patch);
      } else {
        // We can proceed with the agreement application, but we need to
        // redo the SU-Set and constraints for the new rewound state
        this.clockHolder.push(rewoundJoinTime);
        return this.apply(patch, false);
      }
    }

    private async rewind(): Promise<SuSetDataPatch> {
      const tidPatch = new PatchTids(this.ssd.tidsStore);
      for (
        let entry = await this.ssd.journal.entryBefore();
        entry != null && this.op.time.anyLt(entry.operation.time);
        entry = await this.ssd.journal.entryBefore(entry.key)
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
        this.journaling.void(entry);
      }
      // Now compare the affected triple-TIDs to the current state to find
      // actual triples to delete or insert
      const quadPatch = new PatchQuads();
      for (let [triple, tids] of await tidPatch.affected)
        quadPatch.append({ [tids.size ? 'inserts' : 'deletes']: [this.ssd.toUserQuad(triple)] });
      return { tids: tidPatch, quads: quadPatch };
    }

    private async missingCausesResult(patch: SuSetDataPatch): Promise<PatchResult<null>> {
      const { userUpdate } = await new InterimUpdatePatch(
        this.ssd.userGraph, this.ssd.userCtx, patch.quads, { mutable: false }).finalise();
      const commitTids = await this.ssd.tidsStore.commit(patch.tids);
      return {
        patch: patch.quads,
        kvps: batch => {
          commitTids(batch);
          this.journaling.commit(batch);
        },
        return: null,
        after: () => {
          this.ssd.emitUpdate({
            ...userUpdate,
            '@ticks': this.journaling.state.time.ticks
          });
          throw new MeldError('Update out of order',
            'Journal rewind missing agreement causes');
        }
      };
    }

    /**
     * The operation's delete contains reifications of deleted triples. This
     * method resolves the deletions into TID graph changes and deleted triples.
     * Note that the triples for which a TID is being deleted are not necessarily
     * deleted themselves, per SU-Set operation.
     *
     * @return processed deletions to the triple TIDs
     */
    private async processSuSetOpToPatch(patch: SuSetDataPatch) {
      // First establish triples to be deleted according to the SU-Set
      const deletions = await this.op.deletes.reduce(async (resultSoFar, [triple, theirTids]) => {
        // For each unique deleted triple, subtract the claimed tids from the tids we have
        const [ourTripleTids, deletions] = await Promise.all(
          // Ensure that any prior patch updates are accounted for
          [patch.tids.stateOf(triple), resultSoFar]);
        const toRemove = ourTripleTids.filter(tripleTid => theirTids.includes(tripleTid));
        // If no tids are left, delete the triple in our graph
        if (toRemove.length > 0 && toRemove.length == ourTripleTids.length) {
          deletions.triples.push(triple);
        } else {
          this.ssd.log.debug(`Not removing ${tripleIndexKey(triple)}:\n` +
            `\tOurs: ${ourTripleTids}` +
            `\tTheirs: ${theirTids}`);
        }
        for (let tid of toRemove)
          deletions.tids.push([triple, tid]);
        return deletions;
      }, Promise.resolve({ tids: [] as [Triple, UUID][], triples: [] as Triple[] }));
      // Now modify the patch with deletions and insertions
      patch.tids.append({ deletes: deletions.tids });
      patch.quads.append({
        deletes: deletions.triples.map(this.ssd.toUserQuad),
        inserts: this.op.inserts.map(([triple]) => this.ssd.toUserQuad(triple))
      });
      return deletions.tids;
    }

    /**
     * Caution: mutates patch
     */
    private async constraintTxn(
      cxnAssertions: PatchQuads,
      patch: PatchQuads,
      insertTids: TripleMap<UUID[]>,
      cxnTime: TreeClock
    ) {
      if (!cxnAssertions.isEmpty) {
        // Triples that were inserted in the applied transaction may have been
        // deleted by the constraint - these need to be removed from the applied
        // transaction patch but still published in the constraint operation
        const deletedExistingTids = await this.ssd.tidsStore.findTriplesTids(cxnAssertions.deletes);
        const deletedTriplesTids = new TripleMap(deletedExistingTids);
        patch.remove('inserts', cxnAssertions.deletes)
          .forEach(delTriple => deletedTriplesTids.with(delTriple, () => [])
            .push(...(insertTids.get(delTriple) ?? [])));
        // Anything deleted by the constraint that did not exist before the
        // applied transaction can now be removed from the constraint patch
        cxnAssertions.remove('deletes', triple => deletedExistingTids.get(triple) == null);
        const cxnId = cxnTime.hash;
        return {
          operation: this.ssd.txnOperation(
            cxnId, cxnTime, cxnAssertions.inserts, deletedTriplesTids),
          tidPatch: await this.ssd.txnTidPatch(
            cxnId, cxnAssertions.inserts, deletedExistingTids),
          deletedTriplesTids // This is as-if the constraint was applied in isolation
        };
      }
    }
  };

  /**
   * Up-applies transport security from the encoded operation in the message
   */
  private async unSecureOperation(msg: OperationMessage): Promise<EncodedOperation> {
    let [version, from, timeJson, updated, encoding, agreed] = msg.data;
    if (encoding[encoding.length - 1] === BufferEncoding.SECURE) {
      updated = await this.transportSecurity.wire(
        updated, MeldMessageType.operation, 'in', this.readState);
      encoding = encoding.slice(0, -1);
    }
    return [version, from, timeJson, updated, encoding, agreed];
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
          const tidPatch = new PatchTids(this.tidsStore, { inserts: flattenItemTids(triplesTids) });
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
