import { Iri } from 'jsonld/jsonld-spec';
import { MeldConstraint, MeldReadState, InterimUpdate, Reference, Select, Subject } from '..';
import { jrql, toIndexNumber } from '../engine/dataset/JrqlQuads';
import { LseqDef, LseqIndexRewriter } from '../engine/lseq';
import { meld } from '../engine/MeldEncoding';
import { includesValue, includeValue } from '../updates';

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
    if (slotsInInsert.length) {
      // TODO: Account for slots being deleted
      // Look for the lists in the insert that will own those slots
      return Promise.all(update['@insert'].map(subject =>
        this.createRewriter(subject, slotsInInsert)?.doRewrite(state, update)));
    }
  }

  async apply(state: MeldReadState, update: InterimUpdate) {
    // TODO: If someone deletes the type of a list, re-insert the default?
    // TODO: Ensure slots appear only once (one of the moves wins)
    // TODO: Renumber the affected existing slots
  }

  createRewriter(subject: Subject, slotsInInsert: jrql.Slot[]): ListRewriter | undefined {
    const listId = subject['@id'];
    if (listId != null && (!includesValue(subject, '@type') ||
      includesValue(subject, '@type', { '@id': meld.rdflseq.value }))) {
      return Object.keys(subject).reduce<ListRewriter | undefined>((rewriter, listKey) => {
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
          rewriter ??= new ListRewriter(listId, this.lseq, this.site);
          if (typeof indexKey == 'string') {
            // Existing rdflseq position ID, use index from slot
            rewriter.addInsert({ listKey, slot }, slot[jrql.index]);
          } else if (Array.isArray(indexKey))
            // Multiple-item insertion index with insertion order
            rewriter.addInsert({ listKey, slot }, ...indexKey);
          else
            rewriter.addInsert({ listKey, slot }, indexKey);
        }
        return rewriter;
      }, undefined);
    }
  }
}

/** @internal */
interface SlotInList {
  listKey: Iri,
  /** We don't re-write items, just indexes */
  slot: { '@id': Iri, [jrql.index]: number }
}

/** @internal */
class ListRewriter extends LseqIndexRewriter<SlotInList> {
  constructor(readonly listId: Iri, lseq: LseqDef, site: string) {
    super(lseq, site);
  }

  async doRewrite(state: MeldReadState, update: InterimUpdate) {
    if (await this.isDefaultList(state, update)) {
      // Load position identifiers from min index
      // TODO: Load a minimal set of adjacent position identifiers & matching slots
      // Don't use Describe because that would generate an @list
      const [existingPosIds] = await this.loadExistingSlotsInList(state, this.listId);
      let rem: Subject = { '@id': this.listId }, ins: Subject = { '@id': this.listId };
      this.rewriteIndexes(existingPosIds, (slotInList, posId, index) => {
        // Setting the position of an inserted slot
        // Remove the original inserted slot and its index from the update
        includeValue(rem, slotInList.listKey, slotInList.slot);
        // Add the new slot with updated details at the new position ID and index
        includeValue(ins, meld.rdflseqPosId(posId.toString()), {
          ...slotInList.slot, [jrql.index]: index
        });
        // TODO: If a slot has moved, ensure it is removed from the old position
      }, (posId, index) => {
        // TODO: Setting the index of an existing position
      });
      await update.remove('@insert', rem);
      return update.assert({ '@insert': ins });
    }
  }

  private async isDefaultList(state: MeldReadState, update: InterimUpdate): Promise<boolean> {
    const sel = await state.read<Select>({
      '@select': '?type', '@where': { '@id': this.listId, '@type': '?type' }
    });
    if (!includesValue(sel[0] ?? {}, '?type')) {
      // No type yet, insert the default
      await update.assert({
        '@insert': { '@id': this.listId, '@type': meld.rdflseq.value }
      });
      return true;
    }
    return includesValue(sel[0], '?type', { '@id': meld.rdflseq.value });
  }

  private async loadExistingSlotsInList(state: MeldReadState, listId: string):
    Promise<[string[], { [posId: string]: SlotInList; }]> {
    const [existingPosIds, existingSlots] = (await state.read<Select>({
      '@select': ['?listKey', '?slot', '?index'],
      '@where': { '@id': listId, '?listKey': { '@id': '?slot', [jrql.index]: '?index' } }
    })).reduce<[string[], { [posId: string]: SlotInList; }]>(([existingPosIds, existingSlots], sel) => {
      const listKey = (<Reference>sel['?listKey'])['@id'];
      const posId = meld.matchRdflseqPosId(listKey);
      if (posId != null) {
        existingPosIds.push(posId);
        existingSlots[posId] = {
          listKey,
          slot: {
            '@id': (<Reference>sel['?slot'])['@id'],
            [jrql.index]: <number>sel['?index']
          }
        };
      }
      return [existingPosIds, existingSlots];
    }, [[], {}]);
    existingPosIds.sort();
    return [existingPosIds, existingSlots];
  }
}
