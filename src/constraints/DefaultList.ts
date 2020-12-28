import { MeldConstraint, MeldReadState, MutableMeldUpdate, Select } from '..';
import { jrql, toIndexNumber } from '../engine/dataset/JrqlQuads';
import { meld } from '../engine/MeldEncoding';
import { includesValue } from '../updates';

/** @internal */
export class DefaultList implements MeldConstraint {
  constructor() {
  }

  async check(state: MeldReadState, update: MutableMeldUpdate) {
    // Look for list slots being inserted
    const slots = update['@insert'].filter(jrql.isSlot);
    if (slots.length) {
      // Look for the lists in the insert that will own those slots
      const lists = update['@insert'].filter(subject =>
        !includesValue(subject, '@type') && Object.keys(subject).find(key =>
          // Key is a data URL
          toIndexNumber(key) != null &&
          // Value is a reference to a slot
          slots.some(slot => includesValue(subject, key, slot))));

      return Promise.all(lists.map(async list => {
        // TODO: Use @insert @where @not @exists
        const sel = await state.read<Select>({
          '@select': '?type', '@where': { '@id': list['@id'], '@type': '?type' }
        });
        if (!includesValue(sel[0] ?? {}, '?type'))
          await update.append({
            '@insert': {
              '@id': list['@id'],
              '@type': meld.rdflseq.value
            }
          });
      }));
    }
  }

  async apply(state: MeldReadState, update: MutableMeldUpdate) {
    // If someone deletes the type of a list, re-insert the default
  }
}