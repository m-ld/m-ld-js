import {
  Attribution,
  AuditOperation,
  GraphSubject,
  GraphSubjects,
  MeldConstraint,
  MeldError,
  MeldExtensions,
  MeldPreUpdate,
  MeldUpdate,
  noTransportSecurity,
  UpdateTrace,
  UUID
} from '../../api';
import { BufferEncoding, EncodedOperation, OperationMessage, Snapshot, StateManaged } from '..';
import { GlobalClock, TickTree, TreeClock } from '../clocks';
import { Context, isUpdate, Query, Read, Write } from '../../jrql-support';
import { Dataset, Kvps, PatchQuads, PatchResult, TxnContext } from '.';
import { JrqlGraph } from './JrqlGraph';
import { MeldEncoder } from '../MeldEncoding';
import { EMPTY, merge, mergeMap, Observable, of, Subject as Source } from 'rxjs';
import { expand, filter, map, takeWhile } from 'rxjs/operators';
import { completed, inflate, mapIter } from '../util';
import { Logger } from 'loglevel';
import { Quad, Triple, tripleIndexKey, TripleMap } from '../quads';
import { InterimUpdatePatch } from './InterimUpdatePatch';
import { EntryBuilder, Journal, JournalEntry, JournalState } from '../journal';
import { JournalClerk } from '../journal/JournalClerk';
import { PatchTids, TidsStore } from './TidsStore';
import { CausalOperation, expandItemTids, flattenItemTids, ItemTids } from '../ops';
import { Consumable, drain, flowable } from 'rx-flowable';
import { batch, flatMap, ignoreIf } from 'rx-flowable/operators';
import { consume } from 'rx-flowable/consume';
import { MeldMessageType } from '../../ns/m-ld';
import { MeldApp, MeldConfig } from '../../config';
import { MeldOperation, OperationReversion } from '../MeldOperation';
import { ClockHolder } from '../messages';
import { Iri } from '@m-ld/jsonld';
import { M_LD } from '../../ns';
import { MeldOperationMessage } from '../MeldOperationMessage';
import { check } from '../check';
import { getIdLogger } from '../logging';
import { Future } from '../Future';
import { JrqlContext } from '../SubjectQuads';
import { JrqlPatchQuads, UpdateMeta } from './JrqlQuads';
import { RefTriple } from '../jrql-util';

export type DatasetSnapshot = Omit<Snapshot, 'updates'>;

type DatasetConfig = Pick<MeldConfig,
  '@id' | '@domain' | 'maxOperationSize' | 'logLevel' | 'journal'>;

type SuSetDataPatch = { quads: JrqlPatchQuads, tids: PatchTids };

const OpKey = EncodedOperation.Key;

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
  /*readonly*/ userCtx: JrqlContext;

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
    private readonly extensions: StateManaged & MeldExtensions,
    private readonly app: MeldApp,
    config: DatasetConfig
  ) {
    super(config['@domain'], dataset.rdf, extensions.datatypes);
    if (app.principal?.['@id'] === M_LD.localEngine)
      throw new MeldError('Unauthorised', 'Application principal cannot be local engine');
    this.log = getIdLogger(this.constructor, config['@id'], config.logLevel);
    this.journal = new Journal(dataset, this);
    this.tidsStore = new TidsStore(dataset);
    this.journalClerk = new JournalClerk(this.journal, this.sign, config, app.journalAdmin);
    this.maxOperationSize = config.maxOperationSize ?? Infinity;
  }

  @SuSetDataset.checkNotClosed.async
  async initialise() {
    await super.initialise();
    this.userCtx = (await JrqlContext.active(this.context))
      .withDatatypes(this.extensions.datatypes);
    this.userGraph = new JrqlGraph(this.dataset.graph());
  }

  private get transportSecurity() {
    return this.extensions.transportSecurity ?? noTransportSecurity;
  }

  get lock() {
    return this.dataset.lock;
  }

  get readState() {
    return this.userGraph.asReadState;
  }

  get updates(): Observable<MeldUpdate> {
    return this.updateSource;
  }

  @SuSetDataset.checkStateLocked.async
  async allowTransact() {
    await this.extensions.onInitial?.(this.readState);
    this.readyForTxn = true;
  }

  @SuSetDataset.checkStateLocked.rx
  read<R extends Read>(request: R): Consumable<GraphSubject> {
    return this.userGraph.read(request, this.userCtx);
  }

  @SuSetDataset.checkStateLocked.async
  write(request: Write) {
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
            // Set GWC and last agreed to a default; will re-set on snapshot
            GlobalClock.GENESIS, TreeClock.GENESIS)
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
  async operationsSince(
    time: TickTree,
    gwc?: Future<GlobalClock>
  ): Promise<Observable<OperationMessage> | undefined> {
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
            // Note use 'of' instead of 'from' to prevent unhandled
            of(await this.journal.entryAfter(tick)).pipe(
              expand(entry => entry != null ? entry.next() : EMPTY),
              takeWhile<JournalEntry>(entry => entry != null),
              // Don't emit an entry if it's all less than the requested time
              filter(entry => time.anyLt(entry.operation.time)),
              map(entry => entry.asMessage())
            )
        };
      }
    });
  }

  @SuSetDataset.checkNotClosed.async
  @SuSetDataset.checkStateLocked.async
  @SuSetDataset.checkReadyForTxn.async
  async transact(time: TreeClock, request: Write): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        const patch = await this.write(request);
        if (patch.isEmpty)
          return { return: null };

        txc.sw.next('check-constraints');
        const pid = this.app.principal?.['@id'] ?? null;
        const agree = isUpdate(request) ? request['@agree'] : undefined;
        const txn = await this.constrain(patch, 'check', pid, agree);

        txc.sw.next('find-tids');
        const deletedTriplesTids =
          await this.tidsStore.findTriplesTids(txn.assertions.deletes);
        const tid = time.hash;
        const { operation, reversion } = this.txnOperation(
          tid, time, txn.assertions, deletedTriplesTids, txn.agree);
        // Include tid changes in final patch
        txc.sw.next('new-tids');
        const tidPatch = this.txnTidPatch(tid, txn.assertions.inserts, deletedTriplesTids);
        // Include journaling in final patch
        txc.sw.next('journal');
        const journal = await this.journal.state();
        const msg = await this.operationMessage(journal, operation);
        const journaling = journal.builder().next(operation, reversion, time, msg.attr);
        const trace: MeldUpdate['trace'] = () => ({
          // No applicable, resolution or voids for local txn
          trigger: msg.toAuditOperation(), voids: []
        });
        return this.txnResult({
          ...txn, tidPatch, journaling, msg, trace
        });
      }
    });
  }

  private async constrain(
    patch: JrqlPatchQuads,
    verb: keyof MeldConstraint,
    principalId: Iri | null,
    agree: any | null
  ) {
    const interim = new InterimUpdatePatch(
      patch,
      this.userGraph,
      this.tidsStore,
      this.userCtx,
      principalId,
      agree,
      { mutable: verb === 'check' }
    );
    for (let constraint of this.extensions.constraints ?? [])
      await constraint[verb]?.(this.readState, interim);
    return interim.finalise();
  }

  /**
   * Rolls up the given transaction details into a single patch to the store.
   */
  private async txnResult({ tidPatch, journaling, ...txn }: {
    assertions: PatchQuads,
    entailments: PatchQuads,
    tidPatch: PatchTids,
    journaling: EntryBuilder,
    msg: OperationMessage | null,
    internalUpdate: MeldPreUpdate,
    userUpdate: MeldPreUpdate,
    trace: MeldUpdate['trace']
  }): Promise<PatchResult<OperationMessage | null>> {
    this.log.debug(`patch ${journaling.appendEntries.map(e => e.operation.time)}:
    deletes: ${[...txn.assertions.deletes].map(triple => tripleIndexKey(triple))}
    inserts: ${[...txn.assertions.inserts].map(triple => tripleIndexKey(triple))}`);
    const patch = new JrqlPatchQuads(txn.assertions).append(txn.entailments);
    return {
      patch,
      kvps: await this.txnKvps({ tidPatch, journaling, patch }),
      return: txn.msg,
      after: async () => {
        if (this.readyForTxn)
          await this.extensions.onUpdate?.(txn.internalUpdate, this.readState);
        this.emitUpdate({
          ...txn.userUpdate,
          '@ticks': journaling.state.time.ticks,
          trace: txn.trace
        });
      }
    };
  }

  private async txnKvps({ tidPatch, journaling, patch }: {
    tidPatch?: PatchTids,
    journaling?: EntryBuilder,
    patch?: JrqlPatchQuads
  }): Promise<Kvps> {
    const commitTids = tidPatch && await this.tidsStore.commit(tidPatch);
    return batch => {
      commitTids?.(batch);
      journaling?.commit(batch);
      patch && this.userGraph.jrql.saveData(
        patch, batch, journaling?.state.time.ticks);
    };
  }

  private emitUpdate(update: MeldUpdate, allowEmpty = false) {
    if (allowEmpty ||
      update['@delete'].length ||
      update['@insert'].length ||
      update['@update'].length)
      this.updateSource.next(update);
  }

  private async operationMessage(journal: JournalState, op: MeldOperation) {
    // Construct the operation message with the previous visible clock tick
    const prevTick = journal.gwc.getTicks(op.time);
    // Apply signature to the unsecured encoded operation
    const attribution = await this.sign(op);
    // Apply transport wire security to the encoded update
    let encoded: EncodedOperation = [...op.encoded];
    const wireUpdate = await this.transportSecurity.wire(
      encoded[OpKey.update], MeldMessageType.operation, 'out', this.readState);
    if (wireUpdate !== encoded[OpKey.update]) {
      encoded[OpKey.update] = wireUpdate;
      encoded[OpKey.encoding].push(BufferEncoding.SECURE);
    }
    // Re-package the encoded operation with the wire security applied
    const operationMsg = MeldOperationMessage
      .fromOperation(prevTick, encoded, attribution, op.time);
    if (operationMsg.size > this.maxOperationSize)
      throw new MeldError('Delta too big');
    return operationMsg;
  }

  private sign = async (op: MeldOperation) => {
    return this.transportSecurity.sign?.(
      EncodedOperation.toBuffer(op.encoded), this.readState) ?? null;
  };

  /**
   * Un-applies transport security from the encoded operation in the message
   */
  private async unSecureOperation(msg: OperationMessage): Promise<EncodedOperation> {
    const encoded: EncodedOperation = [...msg.data];
    const encoding = encoded[OpKey.encoding];
    if (encoding[encoding.length - 1] === BufferEncoding.SECURE) {
      // Un-apply wire security
      encoded[OpKey.update] = await this.transportSecurity.wire(
        encoded[OpKey.update], MeldMessageType.operation, 'in', this.readState);
      encoded[OpKey.encoding] = encoding.slice(0, -1);
      // Now verify the unsecured encoded update
      await this.transportSecurity.verify?.(
        EncodedOperation.toBuffer(encoded), msg.attr, this.readState);
    } else {
      // Signature applies to the already-encoded message data
      await this.transportSecurity.verify?.(
        MeldOperationMessage.enc(msg), msg.attr, this.readState);
    }
    return encoded;
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
    additions: JrqlPatchQuads,
    removals: TripleMap<UUID[]>,
    agreed?: any
  ) {
    const operation = MeldOperation.fromOperation(this, {
      from: time.ticks,
      time,
      deletes: removals,
      inserts: [...mapIter(
        additions.inserts,
        triple => [triple, [tid]] as ItemTids<Triple>
      )],
      updates: [...mapIter(
        additions.updates,
        ([triple, meta]) => [triple, [meta.operation]] as [Triple, unknown[]]
      )],
      principalId: this.app.principal?.['@id'] ?? null,
      // Note that agreement specifically checks truthy-ness, not just non-null
      agreed: agreed ? { tick: time.ticks, proof: agreed } : null
    });
    const reversion = this.reversion(
      operation, removals, additions.updates, time.ticks);
    return { operation, reversion };
  }

  private reversion(
    operation: MeldOperation,
    removals: TripleMap<UUID[]>,
    updates: JrqlPatchQuads['updates'],
    tick: number
  ) {
    // Here, we annotate the revert metadata with the clock tick, because shared
    // data operations may be unrolled by tick in the backend
    const revertMeta: Pick<TripleMap<unknown[]>, 'get'> = {
      get: triple => {
        const meta = updates
          .filter(([quad]) => triple.equals(quad))
          .map(([, meta]) => [meta.revert, tick]);
        return meta.length > 0 ? meta : null;
      }
    }
    return {
      ...operation.byRef(removals, 'deletes'),
      ...operation.byRef(revertMeta, 'updates')
    };
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
    msg: MeldOperationMessage,
    clockHolder: ClockHolder<TreeClock>
  ): Promise<OperationMessage | null> {
    return this.dataset.transact<OperationMessage | null>({
      prepare: async txc => {
        txc.sw.next('decode-op');
        const journal = await this.journal.state();
        if (journal.isBlocked(msg.time)) {
          return this.ignoreMsgResult(
            'blocked', msg.time, journal.gwc, clockHolder);
        } else if (!msg.data[OpKey.agreed] && msg.time.anyLt(journal.agreed)) {
          return this.ignoreMsgResult(
            'pre-agreement', msg.time, journal.gwc, clockHolder);
        } else {
          const receivedOp = MeldOperation.fromEncoded(this, await this.unSecureOperation(msg));
          const applicableOp = await journal.applicableOperation(receivedOp);
          txc.sw.next('apply-txn');
          this.log.debug(`Applying operation: ${msg.time} @ ${clockHolder.peek()}`);
          // If the operation is synthesised, we need to attribute it ourselves
          const attribution = applicableOp === receivedOp ?
            msg.attr : await this.sign(applicableOp);
          return new SuSetDataset.OperationApplication(
            this, applicableOp, attribution, journal.builder(), txc, clockHolder, msg).apply();
        }
      }
    });
  }

  private ignoreMsgResult(
    reason: string,
    msgTime: TreeClock,
    gwc: GlobalClock,
    clockHolder: ClockHolder<TreeClock>
  ): PatchResult<null> {
    const localTime = clockHolder.peek();
    this.log.debug(`Ignoring ${reason} operation: ${msgTime} @ ${localTime}`);
    clockHolder.push(localTime.ticked(msgTime.ticked(gwc.getTicks(msgTime))));
    return { return: null };
  }

  private static OperationApplication = class {
    constructor(
      private ssd: SuSetDataset,
      private operation: MeldOperation,
      private attribution: Attribution | null,
      private journaling: EntryBuilder,
      private txc: TxnContext,
      private clockHolder: ClockHolder<TreeClock>,
      private received: MeldOperationMessage
    ) {}

    /**
     * Processes our operation into a patch to the dataset
     * @param patch quadstore and tid-store patch components
     * @param processAgreement whether to process an agreement in the operation
     */
    async apply(
      patch: SuSetDataPatch = {
        quads: new JrqlPatchQuads(),
        tids: new PatchTids(this.ssd.tidsStore)
      },
      processAgreement = true
    ): Promise<PatchResult<OperationMessage | null>> {
      try {
        // Process deletions and inserts
        const reversion = await this.processSuSetOpToPatch(patch);

        this.txc.sw.next('apply-cx'); // "cx" = constraint
        const { assertions: cxnAssertions, ...txn } = await this.ssd.constrain(
          patch.quads, 'apply', this.operation.principalId, this.operation.agreed?.proof);

        if (processAgreement && this.operation.agreed != null) {
          // Check agreement conditions. This is done against the non-rewound
          // state, because we may have to recover if the rewind goes back too
          // far. This is allowed because an agreement condition should only
          // inspect previously agreed state.
          for (let agreementCondition of this.ssd.extensions.agreementConditions ?? [])
            await agreementCondition.test(this.ssd.readState, txn.internalUpdate);
          if (this.operation.time.anyLt(this.journaling.state.gwc)) {
            // A rewind is required. This trumps the work we have already done.
            this.txc.sw.next('rewind');
            return this.rewindAndReapply();
          }
        }

        const opTime = this.clockHolder.event(), cxnTime = this.clockHolder.event();
        const insertTids = new TripleMap(this.operation.inserts);
        const cxn = await this.constraintTxn(cxnAssertions, patch.quads, insertTids, cxnTime);
        // After applying the constraint, some new quads might have been removed
        patch.tids.append({
          inserts: flattenItemTids([...patch.quads.inserts]
            .map(triple => [triple, insertTids.get(triple) ?? []]))
        });

        // Done determining the applied operation patch. At this point we could
        // have an empty patch, but we still need to complete the journal entry.
        this.txc.sw.next('journal');
        this.journaling.next(
          this.operation, reversion(opTime.ticks), opTime, this.attribution);

        // If the constraint has done anything, we need to merge its work
        let cxnMsg: MeldOperationMessage | null = null;
        if (cxn != null) {
          // update['@ticks'] = cxnTime.ticks;
          patch.tids.append(cxn.tidPatch);
          patch.quads.append(cxnAssertions);
          // FIXME: If this synthetic operation message exceeds max size, what to do?
          cxnMsg = await this.ssd.operationMessage(this.journaling.state, cxn.operation);
          // Also create a journal entry for the constraint "transaction"
          this.journaling.next(cxn.operation, cxn.reversion, cxnTime, cxnMsg.attr);
        }
        return this.ssd.txnResult({
          ...txn,
          assertions: patch.quads,
          tidPatch: patch.tids,
          journaling: this.journaling,
          msg: cxnMsg,
          trace: this.tracer({ applied: true, cxnMsg })
        });
      } catch (e) {
        // 4000-5000 are bad request errors, leading to an error update
        if (e instanceof MeldError && e.status >= 4000 && e.status < 5000) {
          this.ssd.log.warn('Bad operation', e, this.operation);
          return this.badOperationResult(e);
        } else {
          this.ssd.log.error(e, this.operation);
          throw e;
        }
      }
    }

    private async rewindAndReapply() {
      // void more recent entries in conflict with the agreement
      const patch = await this.rewind();
      // If we now find that we are not ready for the agreement, we need to
      // re-connect to recover what's missing. This could include a rewound
      // local txn. But first, commit the rewind.
      const localTime = this.journaling.state.time;
      const rewoundJoinTime = localTime.ticked(this.operation.time);
      this.ssd.log.debug(`Rewinding to ${rewoundJoinTime}`);
      if (rewoundJoinTime.anyLt(this.operation.time)) {
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
      const quadPatch = new JrqlPatchQuads();
      const tidPatch = new PatchTids(this.ssd.tidsStore);
      const operationReversions = new TripleMap<OperationReversion[]>();
      // Work backwards through the journal, voiding entries that are not in the
      // causes of the applied operation. Stop when the GWC is all-less-than
      // applied op causes, OR we reach an agreement (because nothing prior
      // to an agreement in the journal can be concurrent with it).
      let entry = await this.ssd.journal.entryBefore();
      while (this.operation.time.anyLt(this.journaling.state.gwc) && !entry?.operation.agreed) {
        if (entry == null)
          throw new MeldError('Updates unavailable');
        // Only void if the entry itself is concurrent with the agreement
        if (this.operation.time.anyLt(entry.operation.time)) {
          const reversion = entry.revert();
          // Apply the SU-Set reversion (deletes & inserts)
          tidPatch.append(CausalOperation.flatten(reversion));
          // Accumulate reverse updates
          for (let [triple, revertOperations] of reversion.updates)
            operationReversions.with(triple, () => []).push(...revertOperations);
          // Tell the journal to lose this entry
          this.journaling.void(entry);
        }
        entry = await entry.previous();
      }
      // Now compare the affected triple-TIDs to the current state to find
      // actual triples to delete or insert
      for (let [triple, tids] of await tidPatch.affected) {
        quadPatch.append({
          [tids.size ? 'inserts' : 'deletes']: [this.ssd.toUserQuad(triple)]
        });
      }
      // TODO: apply reverse updates!
      return { tids: tidPatch, quads: quadPatch };
    }

    private async missingCausesResult(rewindPatch: SuSetDataPatch): Promise<PatchResult<null>> {
      const { userUpdate } = await new InterimUpdatePatch(
        rewindPatch.quads,
        this.ssd.userGraph,
        this.ssd.tidsStore,
        this.ssd.userCtx,
        M_LD.localEngine,
        null,
        { mutable: false }
      ).finalise();
      return {
        patch: rewindPatch.quads,
        kvps: await this.ssd.txnKvps(
          { tidPatch: rewindPatch.tids, journaling: this.journaling }),
        return: null,
        after: () => {
          this.ssd.emitUpdate({
            ...userUpdate,
            '@ticks': this.journaling.state.time.ticks,
            trace: this.tracer({ applied: false })
          });
          throw new MeldError('Update out of order',
            'Journal rewind missing agreement causes');
        }
      };
    }

    private badOperationResult(error: MeldError): PatchResult<null> {
      // Put a block on any more operations from this remote
      this.journaling.block(this.operation.time);
      return {
        return: null,
        kvps: this.journaling.commit, // Commit the block
        after: () => {
          this.ssd.emitUpdate({
            '@delete': GraphSubjects.EMPTY,
            '@insert': GraphSubjects.EMPTY,
            '@update': GraphSubjects.EMPTY,
            '@principal': InterimUpdatePatch.principalRef(
              this.operation.principalId, this.ssd.userCtx),
            '@ticks': this.journaling.state.time.ticks,
            trace: this.tracer({ error, applied: false })
          }, true);
        }
      };
    }

    private tracer({ cxnMsg, applied, error }: {
      applied: boolean,
      cxnMsg?: MeldOperationMessage | null,
      error?: MeldError
    }): MeldUpdate['trace'] {
      return () => {
        const { received, operation, attribution, journaling } = this;
        function audit(
          operation: EncodedOperation,
          attribution: Attribution | null
        ): AuditOperation {
          return { operation, attribution, data: EncodedOperation.toBuffer(operation) };
        }
        return new class implements UpdateTrace {
          error = error;
          get trigger() {
            return received.toAuditOperation();
          }
          get applicable() {
            if (applied)
              return audit(operation.encoded, attribution);
          }
          get resolution() {
            if (cxnMsg != null)
              return cxnMsg.toAuditOperation();
          }
          get voids() {
            return journaling.deleteEntries.map(voided =>
              audit(voided.operation.encoded, voided.attribution));
          }
        }();
      };
    }

    /**
     * The operation's delete contains reifications of deleted triples. This
     * method resolves the deletions into TID graph changes and deleted triples.
     * Note that the triples for which a TID is being deleted are not necessarily
     * deleted themselves, per SU-Set operation.
     *
     * @return reversion metadata
     */
    private async processSuSetOpToPatch(patch: SuSetDataPatch) {
      // First establish triples to be deleted according to the SU-Set
      const deletions = await this.operation.deletes.reduce(async (
        resultSoFar,
        [triple, theirTids]
      ) => {
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
      // Now modify the patch with deletions, insertions and custom operations
      patch.tids.append({ deletes: deletions.tids });
      const updates = await drain(inflate(
        this.operation.updates, this.ssd.toSharedDataOpMeta));
      patch.quads.append({
        deletes: deletions.triples.map(this.ssd.toUserQuad),
        inserts: this.operation.inserts.map(([triple]) => this.ssd.toUserQuad(triple)),
        updates
      });
      const deleteTids = expandItemTids(deletions.tids, new TripleMap<string[]>);
      // Defer creation of the reversion because we don't know the tick yet
      return (tick: number) => this.ssd.reversion(this.operation, deleteTids, updates, tick);
    }

    /**
     * Caution: mutates patch
     */
    private async constraintTxn(
      cxnAssertions: JrqlPatchQuads,
      patch: JrqlPatchQuads,
      insertTids: TripleMap<UUID[]>,
      cxnTime: TreeClock
    ) {
      if (!cxnAssertions.isEmpty) {
        // Triples that were inserted in the applied transaction may have been
        // deleted by the constraint - these need to be removed from the applied
        // transaction patch but still published in the constraint operation
        const deletedExistingTids =
          await this.ssd.tidsStore.findTriplesTids(cxnAssertions.deletes);
        const deletedTriplesTids = new TripleMap(deletedExistingTids);
        patch.remove('inserts', cxnAssertions.deletes)
          .forEach(delTriple => deletedTriplesTids.with(delTriple, () => [])
            .push(...(insertTids.get(delTriple) ?? [])));
        // Anything deleted by the constraint that did not exist before the
        // applied transaction can now be removed from the constraint patch
        cxnAssertions.remove(
          'deletes', triple => deletedExistingTids.get(triple) == null);
        const cxnId = cxnTime.hash;
        return {
          ...this.ssd.txnOperation(
            cxnId,
            cxnTime,
            cxnAssertions,
            deletedTriplesTids
          ),
          tidPatch: await this.ssd.txnTidPatch(
            cxnId,
            cxnAssertions.inserts,
            deletedExistingTids
          ),
          deletedTriplesTids // This is as-if the constraint was applied in isolation
        };
      }
    }
  };

  private toSharedDataOpMeta = ([triple, operations]: [RefTriple, unknown[]]) => {
    return this.tidsStore.findTriples(triple.subject, triple.predicate).pipe(
      flatMap(literal => {
        const quad = this.userGraph.quads.quad(triple.subject, triple.predicate, literal);
        return consume(operations).pipe(flatMap(operation =>
          consume(this.userGraph.jrql.applyTripleOperation(quad, operation, this.userCtx)
            .then(upMeta => upMeta && [this.toUserQuad(triple), upMeta] as [Quad, UpdateMeta]))));
      }),
      ignoreIf(null)
    );
  };

  /**
   * Applies a snapshot to this dataset.
   * Caution: uses multiple transactions, so the world must be held up by the caller.
   * @param snapshot snapshot with batches of quads and tids
   * @param localTime the time of the local process, to be saved
   */
  @SuSetDataset.checkNotClosed.async
  async applySnapshot(snapshot: DatasetSnapshot, localTime: TreeClock) {
    // Check that the provided snapshot is not concurrent with the last agreement
    if (await this.journal.initialised()) {
      const journal = await this.journal.state();
      if (snapshot.gwc.anyLt(journal.agreed))
        throw new Error('Snapshot is concurrent with last agreement');
    }
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
          const reified = this.triplesFromBuffer(batch.inserts, batch.encoding);
          const triplesTids = this.unreifyTriplesTids(reified);
          // For each triple in the batch, insert the TIDs into the tids graph
          const tidPatch = new PatchTids(
            this.tidsStore, { inserts: flattenItemTids(triplesTids) });
          // And include the triples themselves
          const patch = new JrqlPatchQuads({
            inserts: triplesTids.map(([triple]) => this.toUserQuad(triple))
          });
          return { kvps: await this.txnKvps({ tidPatch, patch }), patch };
        } else {
          return { kvps: this.journal.insertPastOperation(batch.operation) };
        }
      }
    })));
  }

  /**
   * Takes a snapshot of data, including transaction IDs and latest operations.
   * The data will be loaded from the same consistent snapshot per abstract-level.
   */
  @SuSetDataset.checkNotClosed.async
  async takeSnapshot(): Promise<DatasetSnapshot> {
    const journal = await this.journal.state();
    const allQuads = this.userGraph.quads.query();
    const insData = consume(allQuads).pipe(
      batch(10), // TODO batch size config
      mergeMap(async ({ value: quads, next }) => {
        const tidQuads = await this.tidsStore
          .findTriplesTids(quads, 'includeEmpty');
        await Promise.all(mapIter(tidQuads, ([triple]) =>
          this.userGraph.jrql.loadData(triple, this.userCtx)));
        const reified = this.reifyTriplesTids(this.identifyTriplesData(tidQuads));
        const [inserts, encoding] = this.bufferFromTriples(reified);
        return { value: { inserts, encoding }, next };
      }));
    const opData = inflate(journal.latestOperations(), operations => {
      return consume(operations.map(operation => ({ operation })));
    });
    return {
      gwc: journal.gwc,
      agreed: journal.agreed,
      data: flowable<Snapshot.Datum>(merge(insData, opData)),
      cancel: (cause?: Error) => allQuads.destroy(cause)
    };
  }
}
