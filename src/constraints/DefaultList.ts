import { Iri } from 'jsonld/jsonld-spec';
import { MeldConstraint, MeldReadState, InterimUpdate, Reference, Select, Subject } from '..';
import { jrql, toIndexNumber } from '../engine/dataset/JrqlQuads';
import { LseqDef, LseqIndexRewriter } from '../engine/lseq';
import { meld } from '../engine/MeldEncoding';
import { isPropertyObject, isReference } from '../jrql-support';
import { includesValue } from '../updates';

/** @internal */
export class DefaultList implements MeldConstraint {
  private lseq = new LseqDef();

  constructor(
    /** Unique clone ID, used as lseq site. */
    readonly site: string) {
  }

  async check(state: MeldReadState, update: InterimUpdate) {
    // Look for list slots being inserted
    const slotsInInsert: jrql.Slot[] = update['@insert'].filter(jrql.isSlot);
    // Go through the inserts looking for lists with inserted slots
    const rewriters: { [listId: string]: ListRewriter } = {};
    for (let subject of update['@insert'])
      this.createInsertRewriter(subject, slotsInInsert, rewriters);
    // Go though the deletes looking for lists with deleted indexes
    for (let subject of update['@delete'])
      this.createDeleteRewriter(subject, rewriters);
    return Promise.all(Object.values(rewriters).map(
      rw => rw.doRewrite(state, update)));
  }

  async apply(state: MeldReadState, update: InterimUpdate) {
    // TODO: If someone deletes the type of a list, re-insert the default?
    // TODO: Ensure slots appear only once (one of the moves wins)
    // TODO: Renumber the affected existing slots
  }

  private createInsertRewriter(subject: Subject, slotsInInsert: jrql.Slot[],
    rewriters: { [listId: string]: ListRewriter }) {
    const listId = subject['@id'];
    if (listId != null && (!includesValue(subject, '@type') ||
      includesValue(subject, '@type', { '@id': meld.rdflseq.value }))) {
      for (let listKey in subject) {
        /**
         * Key might be: 
         * - data URL with intended index and sub-index from an `@list` or a
         *   bound index variable; will generate a new rdflseq position ID
         * - an existing rdflseq position ID IRI from a #listKey binding; we
         *   only re-use this if the index turns out to be correct
         * - anything else is treated as a list property (not an index)
         */
        const indexKey = toIndexNumber(listKey) ?? meld.matchRdflseqPosId(listKey);
        // Value has a reference to a slot
        const slot = indexKey != null ? slotsInInsert.find(
          slot => includesValue(subject, listKey, slot)) : null;
        // Only create the rewriter if there is anything to rewrite
        if (indexKey != null && slot != null) {
          rewriters[listId] ??= new ListRewriter(listId, this.lseq, this.site);
          const slotInList = { listKey, id: slot['@id'], index: slot[jrql.index] };
          if (typeof indexKey == 'string') {
            // Existing rdflseq position ID, use index from slot
            rewriters[listId].addInsert(slotInList, slot[jrql.index]);
          } else if (Array.isArray(indexKey)) {
            // Multiple-item insertion index with insertion order
            rewriters[listId].addInsert(slotInList, ...indexKey);
          } else {
            rewriters[listId].addInsert(slotInList, indexKey);
          }
        }
      }
    }
  }

  private createDeleteRewriter(subject: Subject,
    rewriters: { [listId: string]: ListRewriter; }) {
    const listId = subject['@id'];
    if (listId != null) {
      for (let listKey in subject) {
        const object = subject[listKey];
        if (isPropertyObject(listKey, object)) {
          const posId = meld.matchRdflseqPosId(listKey);
          if (posId != null && isReference(object))
            (rewriters[listId] ??= new ListRewriter(listId, this.lseq, this.site))
              .addDelete(posId);
        }
      }
    }
  }
}

/** @internal */
interface SlotInList {
  /** Full predicate including IRI prefix */
  listKey: Iri,
  id: Iri,
  /** We don't re-write items, just indexes */
  index: number,
}

/** @internal */
type PosSlot = { posId: string, item: SlotInList };

/** @internal */
class ListRewriter extends LseqIndexRewriter<SlotInList> {
  private insertSlots: { [slotId: string]: SlotInList } = {};

  constructor(readonly listId: Iri, lseq: LseqDef, site: string) {
    super(lseq, site);
  }

  /** @override to capture moves and duplications */
  addInsert(slot: SlotInList, i: number, ii: number = 0) {
    const already = this.insertSlots[slot.id];
    if (already == null) {
      this.insertSlots[slot.id] = slot;
      super.addInsert(slot, i, ii);
    } else {
      throw `List slot at index ${already.index} cannot be duplicated.`;
    }
  }

  async doRewrite(state: MeldReadState, update: InterimUpdate) {
    if (await this.isDefaultList(state, update)) {
      const existing = await this.loadExistingSlots(state);
      // Check for existing slots that have been moved: anything that exists in
      // the insert. Due to surrounding changes they might end up at the same
      // index as before, but we don't re-use the position ID.
      const movedSlots: ListRewriter['insertSlots'] = {};
      for (let { posId, item: slot } of existing) {
        if (slot.id in this.insertSlots) {
          this.addDelete(posId);
          movedSlots[slot.id] = slot;
        }
      }
      // Re-write the indexes based on all deletions and insertions
      this.rewriteIndexes(existing, {
        deleted: slot => {
          // Cascade the deletion of the slot in this position, if not moved
          if (!(slot.id in this.insertSlots))
            update.assert({ '@delete': { '@id': slot.id } })
        },
        inserted: (slot, posId, index) => {
          // Remove the original inserted slot index from the update.
          update.remove('@insert', {
            '@id': this.listId, [slot.listKey]: {
              '@id': slot.id, [jrql.index]: slot.index
            }
          });
          // Add the new slot with updated details at the new position ID and index.
          update.assert({ // Asserting the slot position
            '@insert': {
              '@id': this.listId,
              [meld.rdflseqPosId(posId)]: { '@id': slot.id }
            },
            // Assert deletion of the old key and index if this slot has moved.
            '@delete': movedSlots[slot.id] == null ? [] : {
              '@id': this.listId, [movedSlots[slot.id].listKey]: {
                '@id': slot.id, [jrql.index]: movedSlots[slot.id].index
              }
            }
          })
          update.entail({ // Entailing the slot index
            '@insert': { '@id': slot.id, [jrql.index]: index }
          });
        },
        reindexed: (slot, _posId, index) => {
          update.assert({
            '@delete': { '@id': slot.id, [jrql.index]: slot.index },
            '@insert': { '@id': slot.id, [jrql.index]: index }
          })
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
  private async loadExistingSlots(state: MeldReadState) {
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
          return { posId, item: { listKey, id, index } };
        }
      }, [])
      .filter<PosSlot>((e): e is PosSlot => e != null)
      .sort((e1, e2) => e1.posId.localeCompare(e2.posId));
  }
}
