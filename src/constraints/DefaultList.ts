import { Iri } from 'jsonld/jsonld-spec';
import {
  MeldConstraint, MeldReadState, InterimUpdate, Reference, Select, GraphSubject
} from '..';
import { LseqDef, LseqIndexRewriter, PosItem } from '../engine/lseq';
import * as meld from '../ns/m-ld';
import { lazy } from '../engine/util';
import {
  isList, isPropertyObject, isReference, isSlot, isSubjectObject, List, SubjectProperty
} from '../jrql-support';
import { includesValue } from '../updates';
import { SingleValued } from './SingleValued';
import { addPropertyObject, listItems } from '../engine/jrql-util';

/** @internal */
export class DefaultList implements MeldConstraint {
  private lseq = new LseqDef();
  private itemSingleValued = new SingleValued('@item');

  constructor(
    /** Unique clone ID, used as lseq site. */
    readonly site: string) {
  }

  async check(state: MeldReadState, interim: InterimUpdate) {
    await this.itemSingleValued.check(state, interim);
    const update = await interim.update;
    await this.doListRewrites('check', interim, state);
    // An index deletion can also be asserted in a delete-where, so in
    // all cases, remove any index assertions
    interim.remove('@delete', update['@delete']
      .filter(s => isSlot(s) && s['@index'] != null)
      .map(s => ({ '@id': s['@id'], ['@index']: s['@index'] })));
  }

  async apply(state: MeldReadState, update: InterimUpdate) {
    await this.itemSingleValued.apply(state, update);
    // TODO: If someone deletes the type of a list, re-insert the default?
    return this.doListRewrites('apply', update, state);
  }

  private async doListRewrites(mode: keyof MeldConstraint,
    interim: InterimUpdate, state: MeldReadState) {
    const update = await interim.update;
    // Look for list slots being inserted (only used for check rewrite)
    // TODO: replace with by-Subject indexing
    const rewriters = lazy(listId =>
      new ListRewriter(mode, listId, this.lseq, this.site));
    // Go through the inserts looking for lists with inserted slots
    for (let subject of update['@insert'].graph.values())
      this.findListInserts(mode, subject, rewriters);
    // Go though the deletes looking for lists with deleted positions
    for (let subject of update['@delete'])
      this.findListDeletes(subject, rewriters);
    return Promise.all([...rewriters].map(
      rewriter => rewriter.doRewrite(state, interim)));
  }

  private findListInserts(mode: keyof MeldConstraint, subject: GraphSubject,
    rewriter: (listId: string) => ListRewriter) {
    if (mode == 'check') {
      // In 'check' mode, ignore non-default lists
      if (!includesValue(subject, '@type') ||
        includesValue(subject, '@type', { '@id': meld.rdflseq })) {
        /**
         * In 'check' mode (initial transaction), a list key might be:
         * - `@list` with intended index and sub-index; will generate a new
         *   rdflseq position ID
         * - an existing rdflseq position ID IRI from a #property binding; we
         *   don't re-use this even if the index turns out to be correct
         * - anything else is treated as a normal property of the list (not an
         *   index)
         */
        if (isList(subject))
          this.addItems(subject, rewriter);
        for (let property in subject)
          this.addItemIfPosId(subject, property, rewriter);
      }
    } else {
      /**
       * In 'apply' mode, looking only for rdflseq position ID IRIs – the
       * subject will not yet be interpreted as a List.
       */
      for (let property in subject)
        this.addItemIfPosId(subject, property, rewriter);
    }
  }

  private addItems(subject: List & Reference, rewriter: (listId: string) => ListRewriter) {
    for (let [listIndex, item] of listItems(subject['@list'])) {
      if (isSlot(item) && typeof listIndex != 'string') {
        const slotInList: SlotInList = {
          property: ['@list', ...listIndex], id: item['@id']
        };
        const [index, subIndex] = listIndex;
        if (subIndex == null)
          rewriter(subject['@id']).addInsert([slotInList], index);
        else
          // Multiple-item insertion index with insertion order
          rewriter(subject['@id']).addInsert(
            Object.assign([], { [subIndex]: slotInList }), index);
      }
    }
  }

  private findListDeletes(subject: GraphSubject, rewriter: (listId: string) => ListRewriter) {
    for (let [property, object] of Object.entries(subject)) {
      if (isPropertyObject(property, object)) {
        const posId = meld.matchRdflseqPosId(property);
        if (posId != null && isReference(object))
          rewriter(subject['@id']).addDelete(posId);
      }
    }
  }

  private addItemIfPosId(subject: GraphSubject, property: string,
    rewriter: (listId: string) => ListRewriter) {
    const posId = meld.matchRdflseqPosId(property), object = subject[property];
    if (posId != null && isPropertyObject(property, object) &&
      (isReference(object) || isSubjectObject(object))) {
      const slotId = object['@id'];
      if (slotId != null)
        rewriter(subject['@id']).addInsert({ property: property, id: slotId }, posId);
    }
  }
}

/** @internal */
interface SlotInList {
  /** Property referencing the slot from the List. */
  property: SubjectProperty,
  /** Slot IRI */
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
  private preProcess(existing: PosItem<SlotInList>[], update: InterimUpdate) {
    const bySlot: { [slotId: string]: { old?: SlotInList; final?: string | number; }; } = {};
    for (let index in existing) { // 'in' allows for sparsity
      const { value: slot, posId } = existing[index];
      bySlot[slot.id] = { old: slot, final: this.deletes.has(posId) ? undefined : posId };
    }
    for (let { value: slot, posId, index } of this.inserts) {
      const prev = bySlot[slot.id]?.final;
      if (prev == null) {
        (bySlot[slot.id] ??= {}).final = posId ?? index;
      } else if (prev === (posId ?? index)) {
        // If re-inserting in the same position, don't process the insert
        if (posId != null)
          this.removeInsert(posId);
      } else {
        // In check mode, throw if finally inserting in more than one position
        if (this.mode == 'check')
          throw 'Slot cannot appear more than once in list.';
        // In apply mode, choose the lowest final position as authoritative
        if (posId != null && typeof prev == 'string' && prev !== posId) {
          const [final, toDel] = [prev, posId].sort();
          bySlot[slot.id].final = final;
          // We need to register the deletion so re-indexing is correct
          this.addDelete(toDel);
          // Assert deletion of the redundant index key
          update.assert({
            '@delete': { '@id': this.listId, [meld.rdflseqPosId(toDel)]: { '@id': slot.id } }
          });
        }
      }
    }
    return bySlot;
  }

  async doRewrite(state: MeldReadState, interim: InterimUpdate) {
    if (await this.isDefaultList(state, interim)) {
      const existing = await this.loadRelevantSlots(state);
      // Cache existing slots and their final positions, by slot ID
      const bySlot = this.preProcess(existing, interim);
      // Re-write the indexes based on all deletions and insertions
      this.rewriteIndexes(existing, {
        deleted: (slot, _, index) => {
          // If the slot is moving, we'll do the re-index in the insert
          if (bySlot[slot.id].final == null) {
            // Entail removal of the old slot index
            interim.entail({ '@delete': { '@id': slot.id, '@index': slot.index } });
            // Cascade the deletion of the slot in this position
            if (this.mode == 'check')
              interim.assert({ '@delete': { '@id': slot.id } });
          }
          // Only need to alias the key if it was a position ID
          if (typeof slot.property == 'string')
            interim.alias(this.listId, slot.property, ['@list', index]);
        },
        inserted: (slot, posId, index, oldIndex) => {
          const property = meld.rdflseqPosId(posId);
          if (this.mode == 'check') {
            // Remove the original inserted slot key from the update.
            interim.remove('@insert',
              addPropertyObject({ '@id': this.listId }, slot.property, { '@id': slot.id }));
            // Add the new slot with updated details at the new position ID.
            interim.assert({ // Asserting the new slot position
              '@insert': { '@id': this.listId, [property]: { '@id': slot.id } }
            });
          }
          const { old } = bySlot[slot.id];
          if (old?.index !== index) {
            interim.entail({ // Entailing the slot index
              '@insert': { '@id': slot.id, '@index': index },
              // Entail deletion of the old index if this slot has moved
              '@delete': old != null ? { '@id': slot.id, '@index': old.index } : []
            });
          }
          // Alias the generated property from the update to the inserted index
          interim.alias(this.listId, property, ['@list', ...oldIndex]);
        },
        reindexed: (slot, _posId, index) => {
          interim.entail({
            '@delete': { '@id': slot.id, '@index': slot.index },
            '@insert': { '@id': slot.id, '@index': index }
          });
        }
      });
    }
  }

  private async isDefaultList(state: MeldReadState, update: InterimUpdate): Promise<boolean> {
    // We only ever reach here in apply mode if we definitely have an RdfLseq
    if (this.mode == 'apply')
      return true;
    const sel = await state.read<Select>({
      '@select': '?type', '@where': { '@id': this.listId, '@type': '?type' }
    });
    if (!includesValue(sel[0] ?? {}, '?type')) {
      // No type yet, insert the default
      update.assert({ '@insert': { '@id': this.listId, '@type': meld.rdflseq } });
      return true;
    }
    return includesValue(sel[0], '?type', { '@id': meld.rdflseq });
  }

  /**
   * Loads relevant position identifiers. Relevance is defined as:
   * - Index >= `this.domain.index.min`
   * - PosId >= `this.domain.posId.min`
   * - Slot in inserted slots _and all after_ (may get deleted)
   */
  // TODO: This loads the whole list
  private async loadRelevantSlots(state: MeldReadState) {
    const affected: PosItem<SlotInList>[] = [];
    // Don't use Describe because that would generate a @list
    (await state.read<Select>({
      '@select': ['?property', '?slot', '?index'],
      '@where': {
        '@id': this.listId, '?property': { '@id': '?slot', '@index': '?index' }
      }
    })).forEach(sel => {
      const property = (<Reference>sel['?property'])['@id'];
      const posId = meld.matchRdflseqPosId(property);
      if (posId != null) {
        const id = (<Reference>sel['?slot'])['@id'], index = (<number>sel['?index']);
        affected[index] = <PosItem<SlotInList>>{ posId, value: { property, id, index } };
      }
    });
    // affected may have an empty head region but should be contiguous thereafter
    return affected;
  }
}
