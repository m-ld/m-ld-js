import { Iri } from 'jsonld/jsonld-spec';
import { MeldConstraint, MeldReadState, InterimUpdate, Reference, Select, Subject, Update } from '..';
import { jrql, toIndexNumber } from '../engine/dataset/JrqlQuads';
import { LseqDef } from '../engine/lseq';
import { meld } from '../engine/MeldEncoding';
import { includesValue, includeValue } from '../updates';

/** @internal */
export class DefaultList implements MeldConstraint {
  private lseq = new LseqDef();

  constructor(
    /**
     * Unique clone ID, used as lseq site.
     */
    readonly site: string) {
  }

  async check(state: MeldReadState, update: InterimUpdate) {
    // Look for list slots being inserted
    const slotsInInsert: jrql.Slot[] = update['@insert'].filter(jrql.isSlot);
    if (!slotsInInsert.length)
      return;

    // TODO: Account for slots being deleted

    const { lseq, site } = this;
    class ListRewriter {
      static create(subject: Subject): ListRewriter | undefined {
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
            const indexNumber = toIndexNumber(listKey) ?? meld.matchRdflseqPosId(listKey);
            // Value has a reference to a slot
            const slot = indexNumber != null ? slotsInInsert.find(
              slot => includesValue(subject, listKey, slot)) : null;
            // Only create the rewriter if there is anything to rewrite
            if (indexNumber != null && slot != null) {
              rewriter ??= new ListRewriter(listId);
              if (typeof indexNumber == 'string') {
                // Existing rdflseq position ID, use index from slot
                rewriter.addInsertSlot(slot[jrql.index], 0, listKey, slot);
              } else if (Array.isArray(indexNumber))
                // Multiple-item insertion index with insertion order
                rewriter.addInsertSlot(...indexNumber, listKey, slot);
              else
                rewriter.addInsertSlot(indexNumber, 0, listKey, slot);
            }
            return rewriter;
          }, undefined);
        }
      }

      async doRewrite() {
        if (await this.isDefaultList())
          return this.rewriteIndexes();
      }

      private constructor(readonly listId: Iri) { }
      /** A sparse array of slots inserted at indexes and sub-indexes */
      private insertSlots: { listKey: Iri, slot: jrql.Slot }[][] = [];
      private minInsertIndex = Infinity;

      private addInsertSlot(i: number, ii: number, listKey: Iri, slot: jrql.Slot) {
        if (i < this.minInsertIndex)
          this.minInsertIndex = i;
        (this.insertSlots[i] ??= [])[ii] = { listKey, slot };
      }

      private async isDefaultList(): Promise<boolean> {
        const sel = await state.read<Select>({
          '@select': '?type', '@where': { '@id': this.listId, '@type': '?type' }
        });
        if (!includesValue(sel[0] ?? {}, '?type')) {
          // No type yet, insert the default
          await update.assert(updateToDefaultListType(this.listId));
          return true;
        }
        return includesValue(sel[0], '?type', { '@id': meld.rdflseq.value });
      }

      private async rewriteIndexes(): Promise<unknown> {
        // Load position identifiers from min index
        // TODO: Load a minimal set of adjacent position identifiers & matching slots
        // Don't use Describe because that would generate an @list
        const existing = (await state.read<Select>({
          '@select': ['?listKey', '?slot', '?index'],
          '@where': { '@id': this.listId, '?listKey': { '@id': '?slot', [jrql.index]: '?index' } }
        })).reduce<{ posId: string, slotId: Iri, index: Subject['any'] }[]>((existing, sel) => {
          const posId = meld.matchRdflseqPosId((<Reference>sel['?listKey'])['@id']);
          if (posId != null)
            existing.push({ posId, slotId: (<Reference>sel['?slot'])['@id'], index: sel['?index'] });
          return existing;
        }, []);
        existing.sort((p1, p2) => p1.posId.localeCompare(p2.posId));
        // Construct a new assertion of list slot positions
        let rem: Subject = { '@id': this.listId }, ins: Subject = { '@id': this.listId };
        // Starting from the minimum inserted index, generate LSEQ position
        // identifiers for the inserted indexes and rewrite existing indexes
        let oldIndex = this.minInsertIndex, newIndex = oldIndex,
          posId = (oldIndex - 1) in existing ? lseq.parse(existing[oldIndex - 1].posId) : lseq.min;
        while (oldIndex < this.insertSlots.length ||
          (oldIndex < existing.length && oldIndex !== newIndex)) {
          // Insert items here if requested
          if (this.insertSlots[oldIndex] != null) {
            const upper = oldIndex in existing ?
              lseq.parse(existing[oldIndex].posId) : lseq.max;
            for (let insertSlot of this.insertSlots[oldIndex]) {
              posId = posId.between(upper, site);
              // Remove the original inserted slot and its details from the update
              includeValue(rem, insertSlot.listKey, insertSlot.slot);
              // Add the new slot with updated details at the new position ID and index
              includeValue(ins, meld.rdflseqPosId(posId.toString()), {
                ...insertSlot.slot, [jrql.index]: newIndex
              });
              // TODO: If a slot has moved, ensure it is removed from the old position
              newIndex++;
            }
          }
          if (oldIndex in existing) {
            if (newIndex !== oldIndex) {
              // TODO: Renumber the existing slot
            }
            // Next loop iteration must jump over the old slot
            posId = lseq.parse(existing[oldIndex].posId);
            newIndex++;
          }
          oldIndex++;
        }
        await update.remove('@insert', rem);
        return update.assert({ '@insert': ins });
      }
    }
    // Look for the lists in the insert that will own those slots
    return Promise.all(update['@insert'].map(subject =>
      ListRewriter.create(subject)?.doRewrite()));
  }

  async apply(state: MeldReadState, update: InterimUpdate) {
    // TODO: If someone deletes the type of a list, re-insert the default
    // TODO: Ensure slots appear only once (one of the moves wins)
    // TODO: Renumber the existing slots
  }
}

/** @internal */
function updateToDefaultListType(listId: Iri): Update {
  return { '@insert': { '@id': listId, '@type': meld.rdflseq.value } };
}
