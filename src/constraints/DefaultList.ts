import { Iri } from 'jsonld/jsonld-spec';
import { MeldConstraint, MeldReadState, InterimUpdate, Reference, Select, Subject } from '..';
import { jrql, toIndexNumber } from '../engine/dataset/JrqlQuads';
import { LseqDef, LseqIndexRewriter, PosItem } from '../engine/lseq';
import { meld } from '../engine/MeldEncoding';
import { lazy } from '../engine/util';
import { isPropertyObject, isReference } from '../jrql-support';
import { includesValue } from '../updates';
import { SingleValued } from './SingleValued';

/** @internal */
export class DefaultList implements MeldConstraint {
  private lseq = new LseqDef();
  private itemSingleValued = new SingleValued(jrql.item);

  constructor(
    /** Unique clone ID, used as lseq site. */
    readonly site: string) {
  }

  async check(state: MeldReadState, update: InterimUpdate) {
    await this.itemSingleValued.check(state, update);
    await this.doListRewrites('check', update, state);
    // An index deletion can be asserted in a delete-where, so in
    // all cases, remove any index assertions
    update.remove('@delete', update['@delete']
      .filter(s => jrql.isSlot(s) && s[jrql.index] != null)
      .map(s => ({ '@id': s['@id'], [jrql.index]: s[jrql.index] })));
  }

  async apply(state: MeldReadState, update: InterimUpdate) {
    await this.itemSingleValued.apply(state, update);
    // TODO: If someone deletes the type of a list, re-insert the default?
    // TODO: Ensure slots appear only once (lowest position wins)
    return this.doListRewrites('apply', update, state);
  }

  private doListRewrites(mode: keyof MeldConstraint,
    update: InterimUpdate, state: MeldReadState) {
    // Look for list slots being inserted (only used for check rewrite)
    const slotsInInsert = mode == 'check' ? update['@insert'].filter(jrql.isSlot) : [];
    const rewriters = lazy(listId =>
      new ListRewriter(mode, listId, this.lseq, this.site));
    // Go through the inserts looking for lists with inserted slots
    for (let subject of update['@insert'])
      this.findListInserts('check', subject, rewriters, slotsInInsert);
    // Go though the deletes looking for lists with deleted indexes
    for (let subject of update['@delete'])
      this.findListDeletes(subject, rewriters);
    return Promise.all([...rewriters].map(
      rewriter => rewriter.doRewrite(state, update)));
  }

  private findListInserts(mode: keyof MeldConstraint, subject: Subject,
    rewriter: (listId: string) => ListRewriter, slotsInInsert: jrql.Slot[]) {
    const listId = subject['@id'];
    if (listId != null) {
      if (mode == 'check') {
        // In 'check' mode, ignore non-default lists
        if (!includesValue(subject, '@type') ||
          includesValue(subject, '@type', { '@id': meld.rdflseq.value })) {
          for (let key in subject)
            this.checkIfListKey(listId, subject, key, slotsInInsert, rewriter);
        }
      } else {
        for (let key in subject)
          this.applyIfListKey(listId, subject, key, rewriter);
      }
    }
  }

  private findListDeletes(subject: Subject, rewriter: (listId: string) => ListRewriter) {
    const listId = subject['@id'];
    if (listId != null) {
      for (let [listKey, object] of Object.entries(subject)) {
        if (isPropertyObject(listKey, object)) {
          const posId = meld.matchRdflseqPosId(listKey);
          if (posId != null && isReference(object))
            rewriter(listId).addDelete(posId);
        }
      }
    }
  }

  /**
   * In 'check' mode (initial transaction), a list key might be:
   * - data URL with intended index and sub-index from an `@list` or a
   *   bound index variable; will generate a new rdflseq position ID
   * - an existing rdflseq position ID IRI from a #listKey binding; we
   *   don't re-use this even if the index turns out to be correct
   * - anything else is treated as a list property (not an index)
   */
  private checkIfListKey(listId: string, subject: Subject, listKey: string,
    slotsInInsert: jrql.Slot[], rewriter: (listId: string) => ListRewriter) {
    const indexKey = toIndexNumber(listKey) ?? meld.matchRdflseqPosId(listKey);
    // Value has a reference to a slot
    const slot = indexKey != null ? slotsInInsert.find(
      slot => includesValue(subject, listKey, slot)) : null;
    if (indexKey != null && slot != null) {
      const slotInList = { listKey, id: slot['@id'] };
      if (typeof indexKey == 'string') {
        // Existing rdflseq position ID
        rewriter(listId).addInsert(slotInList, indexKey);
      } else if (Array.isArray(indexKey)) {
        // Multiple-item insertion index with insertion order
        const [index, subIndex] = indexKey;
        rewriter(listId).addInsert(
          Object.assign([], { [subIndex]: slotInList }), index);
      } else {
        rewriter(listId).addInsert([slotInList], indexKey);
      }
    }
  }

  /**
   * In 'apply' mode, looking only for rdflseq position ID IRIs
   */
  private applyIfListKey(listId: string, subject: Subject, listKey: string,
    rewriter: (listId: string) => ListRewriter) {
    const posId = meld.matchRdflseqPosId(listKey), object = subject[listKey];
    if (posId != null && isPropertyObject(listKey, object) && isReference(object))
      rewriter(listId).addInsert({ listKey, id: object['@id'] }, posId);
  }
}

/** @internal */
interface SlotInList {
  /** Full predicate including IRI prefix */
  listKey: Iri,
  id: Iri,
  /** We don't re-write items, just indexes */
  index?: number,
}

/** @internal */
class ListRewriter extends LseqIndexRewriter<SlotInList> {
  constructor(
    readonly mode: keyof MeldConstraint,
    readonly listId: Iri,
    lseq: LseqDef,
    site: string) {
    super(lseq, site);
  }

  /**
   * A slot cannot appear in more than one position in the list. For every
   * affected slot ID, this determines the final position (posId or index), and
   * throws if not allowed.
   */
  private preProcess(existing: PosItem<SlotInList>[]) {
    const bySlot: { [slotId: string]: { old?: SlotInList; final?: string | number; }; } = {};
    for (let { value: slot, posId } of existing)
      bySlot[slot.id] = { old: slot, final: this.deletes.has(posId) ? undefined : posId };
    for (let { value: slot, posId, index } of this.inserts) {
      const prev = bySlot[slot.id]?.final;
      if (prev == null) {
        (bySlot[slot.id] ??= {}).final = posId ?? index;
      } else if (prev !== (posId ?? index)) {
        // - In check mode, throw if finally inserting in more than one position
        if (this.mode == 'check')
          throw 'Slot cannot appear more than once in list.';
        // - In apply mode, choose the lowest final position as authoritative
        if (posId != null && typeof prev == 'string' && prev > posId) {
          bySlot[slot.id].final = posId;
          // We will assert deletion of the other position later
          this.addDelete(prev);
        }
      }
    }
    return bySlot;
  }

  async doRewrite(state: MeldReadState, update: InterimUpdate) {
    if (await this.isDefaultList(state, update)) {
      const existing = await this.loadRelevantSlots(state);
      // Cache existing slots and their final positions, by slot ID
      const bySlot = this.preProcess(existing);
      // Re-write the indexes based on all deletions and insertions
      this.rewriteIndexes(existing, {
        deleted: slot => {
          update.entail({ '@delete': { '@id': slot.id, [jrql.index]: slot.index } });
          // Cascade the deletion of the slot in this position, if not moved
          if (this.mode == 'check' && bySlot[slot.id].final == null)
            update.assert({ '@delete': { '@id': slot.id } });
        },
        inserted: (slot, posId, index) => {
          const { old, final } = bySlot[slot.id];
          if (this.mode == 'check') {
            // Remove the original inserted slot index from the update.
            update.remove('@insert', {
              '@id': this.listId, [slot.listKey]: { '@id': slot.id }
            });
            // Add the new slot with updated details at the new position ID and index.
            update.assert({ // Asserting the new slot position
              '@insert': { '@id': this.listId, [meld.rdflseqPosId(posId)]: { '@id': slot.id } }
            });
          } else if (old != null && final != null && posId !== final) {
            // This slot has become duplicated due to a concurrent move. Assert
            // deletion of the old list key.
            update.assert({
              '@delete': { '@id': this.listId, [old.listKey]: { '@id': slot.id } }
            });
          }
          update.entail({ // Entailing the slot index
            '@insert': { '@id': slot.id, [jrql.index]: index },
            // Entail deletion of the old index if this slot has moved
            '@delete': old != null ? { '@id': slot.id, [jrql.index]: old.index } : []
          });
        },
        reindexed: (slot, _posId, index) => {
          update.entail({
            '@delete': { '@id': slot.id, [jrql.index]: slot.index },
            '@insert': { '@id': slot.id, [jrql.index]: index }
          });
        }
      });
    }
  }

  private async isDefaultList(state: MeldReadState, update: InterimUpdate): Promise<boolean> {
    const sel = await state.read<Select>({
      '@select': '?type', '@where': { '@id': this.listId, '@type': '?type' }
    });
    if (!includesValue(sel[0] ?? {}, '?type')) {
      // No type yet, insert the default
      update.assert({ '@insert': { '@id': this.listId, '@type': meld.rdflseq.value } });
      return true;
    }
    return includesValue(sel[0], '?type', { '@id': meld.rdflseq.value });
  }

  /**
   * Loads relevant position identifiers. Relevance is defined as:
   * - Index >= `this.minInsertIndex`
   * - PosId >= `this.minDeletePosId`
   * - SlotId in `this.insertSlots`, and any following
   */
  // TODO: This just loads the whole list and sorts in memory at the moment
  private async loadRelevantSlots(state: MeldReadState) {
    // Don't use Describe because that would generate an @list
    return (await state
      .read<Select>({
        '@select': ['?listKey', '?slot', '?index'],
        '@where': {
          '@id': this.listId, '?listKey': { '@id': '?slot', [jrql.index]: '?index' }
        }
      }))
      .map(sel => {
        const listKey = (<Reference>sel['?listKey'])['@id'];
        const posId = meld.matchRdflseqPosId(listKey);
        if (posId != null) {
          const id = (<Reference>sel['?slot'])['@id'], index = (<number>sel['?index']);
          return <PosItem<SlotInList>>{ posId, value: { listKey, id, index } };
        }
      }, [])
      .filter<PosItem<SlotInList>>((e): e is PosItem<SlotInList> => e != null)
      .sort((e1, e2) => e1.posId.localeCompare(e2.posId));
  }
}
