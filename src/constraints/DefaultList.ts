import { Iri } from 'jsonld/jsonld-spec';
import { MeldConstraint, MeldReadState, InterimUpdate, Reference, Select, Subject } from '..';
import { jrql, toIndexNumber } from '../engine/dataset/JrqlQuads';
import { LseqDef, LseqIndexRewriter } from '../engine/lseq';
import { meld } from '../engine/MeldEncoding';
import { isReference } from '../jrql-support';
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
          const slotInList: SlotInList = {
            listKey, slotId: slot['@id'], index: slot[jrql.index]
          };
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
        const posId = meld.matchRdflseqPosId(listKey), slotRef = subject[listKey];
        if (posId != null && isReference(slotRef))
          (rewriters[listId] ??= new ListRewriter(listId, this.lseq, this.site))
            .addDelete({ listKey, slotId: slotRef['@id'] }, posId);
      }
    }
  }
}

/** @internal */
interface SlotInList {
  /** Full predicate including IRI prefix */
  listKey: Iri,
  slotId: Iri,
  /** We don't re-write items, just indexes */
  index: number
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
      const [existingPosIds, existingSlots] =
        await this.loadExistingSlotsInList(state, this.listId);
      this.rewriteIndexes(existingPosIds, {
        setDeleted: slotInList => {
          // Cascade the deletion of the slot in this position
          update.assert({ '@delete': { '@id': slotInList.slotId } })
        },
        setInsertPos: (slotInList, posId, index) => {
          // Setting the position of an inserted slot
          // Remove the original inserted slot index from the update
          update.remove('@insert', {
            '@id': this.listId, [slotInList.listKey]: {
              '@id': slotInList.slotId, [jrql.index]: slotInList.index
            }
          });
          // Add the new slot with updated details at the new position ID and index
          update.assert({ // Asserting the slot position
            '@insert': {
              '@id': this.listId,
              [meld.rdflseqPosId(posId)]: { '@id': slotInList.slotId }
            }
          })
          update.entail({ // Entailing the slot index
            '@insert': { '@id': slotInList.slotId, [jrql.index]: index }
          });
          // TODO: If a slot has moved, ensure it is removed from the old position
        },
        setPosIndex: (posId, index) => {
          update.assert({
            '@delete': {
              '@id': existingSlots[posId].slotId,
              [jrql.index]: existingSlots[posId].index
            },
            '@insert': {
              '@id': existingSlots[posId].slotId,
              [jrql.index]: index
            }
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
          slotId: (<Reference>sel['?slot'])['@id'],
          index: <number>sel['?index']
        };
      }
      return [existingPosIds, existingSlots];
    }, [[], {}]);
    existingPosIds.sort();
    return [existingPosIds, existingSlots];
  }
}
