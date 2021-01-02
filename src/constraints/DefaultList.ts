import { Iri } from 'jsonld/jsonld-spec';
import { MeldConstraint, MeldReadState, MutableMeldUpdate, Reference, Select, Subject, Update } from '..';
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

  async check(state: MeldReadState, update: MutableMeldUpdate) {
    // Look for list slots being inserted
    const slotsInInsert: Reference[] = update['@insert']
      .filter(jrql.isSlot).map(slot => ({ '@id': slot['@id'] }));
    if (!slotsInInsert.length)
      return;

    // TODO: Handle slots being deleted

    const { lseq, site } = this;
    class ListRewriter {
      static create(subject: Subject): ListRewriter | undefined {
        const listId = subject['@id'];
        if (listId != null && (!includesValue(subject, '@type') ||
          includesValue(subject, '@type', { '@id': meld.rdflseq.value }))) {
          return Object.keys(subject).reduce<ListRewriter | undefined>((rewriter, index) => {
            // Key is a data URL
            const indexNumber = toIndexNumber(index);
            // Value has a reference to slots
            const slotRef = indexNumber != null ? slotsInInsert.find(
              slot => includesValue(subject, index, slot)) : null;

            if (indexNumber != null && slotRef != null) {
              rewriter ??= new ListRewriter(listId);
              if (Array.isArray(indexNumber))
                rewriter.addSlotRef(...indexNumber, index, slotRef);
              else
                rewriter.addSlotRef(indexNumber, 0, index, slotRef);
            }
            return rewriter;
          }, undefined);
        }
      }

      private constructor(readonly listId: Iri) { }
      /** A sparse array of slots inserted at indexes and sub-indexes */
      private slotRefs: { index: Iri, ref: Reference }[][] = [];

      private addSlotRef(i: number, ii: number, index: Iri, slotRef: Reference) {
        (this.slotRefs[i] ??= [])[ii] = { index, ref: slotRef };
      }

      async doRewrite() {
        if (await this.isDefaultList())
          return this.rewriteIndexes();
      }

      private async isDefaultList(): Promise<boolean> {
        const sel = await state.read<Select>({
          '@select': '?type', '@where': { '@id': this.listId, '@type': '?type' }
        });
        if (!includesValue(sel[0] ?? {}, '?type')) {
          // No type yet, insert the default
          await update.append(updateToDefaultListType(this.listId));
          return true;
        }
        return includesValue(sel[0], '?type', { '@id': meld.rdflseq.value });
      }

      private async rewriteIndexes(): Promise<unknown> {
        // Load position identifiers
        // TODO: Load a minimal set of adjacent position identifiers & matching slots
        // Don't use Describe because that would generate an @list
        const existing = (await state.read<Select>({
          '@select': ['?posId', '?slot'],
          '@where': { '@id': this.listId, '?posId': '?slot' }
        })).reduce<{ posIds: string[], slotPos: { [slot: string]: string } }>((existing, sel) => {
          let posId = (<Reference>sel['?posId'])['@id'], slot = <Reference>sel['?slot'];
          if (posId.startsWith(meld.rdflseqPosIdPre)) {
            posId = posId.slice(meld.rdflseqPosIdPre.length);
            existing.posIds.push(posId);
            existing.slotPos[slot['@id']] = posId;
          }
          return existing;
        }, { posIds: [], slotPos: {} });
        existing.posIds.sort();

        let del: Subject = { '@id': this.listId }, ins: Subject = { '@id': this.listId };
        // Generate LSEQ position identifiers for the inserted indexes
        let posId = lseq.min; // Next must always be bigger than previous
        this.slotRefs.forEach((slots, i) => { // forEach skips empty
          posId = (i - 1) in existing.posIds ? lseq.parse(existing.posIds[i - 1]) : posId;
          let upper = i in existing.posIds ? lseq.parse(existing.posIds[i]) : lseq.max;
          slots.forEach(slot => {
            posId = posId.between(upper, site);
            // Remove the data index
            includeValue(del, slot.index, slot.ref);
            // Add the position identifier
            includeValue(ins, meld.rdflseqPosIdPre + posId.toString(), slot.ref);
            // TODO: If a slot has moved, ensure it is removed from the old position
          });
        });
        return update.append({ '@delete': del, '@insert': ins });
      }
    }
    // Look for the lists in the insert that will own those slots
    return Promise.all(update['@insert'].map(subject =>
      ListRewriter.create(subject)?.doRewrite()));
  }

  async apply(state: MeldReadState, update: MutableMeldUpdate) {
    // TODO: If someone deletes the type of a list, re-insert the default
    // TODO: Ensure slots appear only once (one move wins)
  }
}

/** @internal */
function updateToDefaultListType(listId: Iri): Update {
  return { '@insert': { '@id': listId, '@type': meld.rdflseq.value } };
}
