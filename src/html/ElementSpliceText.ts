import { EventEmitter } from 'events';
import { TextSplice } from '../jrql-support';
import { textDiff } from '../subjects';

export class ElementSpliceText<E extends HTMLElement> extends EventEmitter {
  prev: string;

  constructor(
    readonly element: E
  ) {
    super();
    element.addEventListener('beforeinput', this.mark);
    element.addEventListener('input', this.onInput);
  }

  detach() {
    this.element.removeEventListener('beforeinput', this.mark);
    this.element.removeEventListener('input', this.onInput);
  }

  get text() {
    this.normalise();
    return this.element.textContent ?? '';
  }

  on(event: 'update', handler: (splice: TextSplice) => void) {
    return super.on(event, handler);
  }

  /** Apply remote update; does not trigger 'update' event */
  update(text: TextSplice[] | string) {
    // Ensure that the text content is one normalised Text node
    if (typeof text == 'string') {
      this.element.replaceChildren(document.createTextNode(text));
    } else {
      this.normalise();
      // Split the text on every splice
      text.sort(([i1], [i2]) => i2 - i1); // descending
      for (let [index, deleteCount, content] of text) {
        const head = <Text>this.element.firstChild;
        if (head == null) {
          if (index === 0 && content != null)
            this.element.appendChild(document.createTextNode(content));
          else
            throw new RangeError(`Curious splice ${text} into empty text`);
        } else {
          const tail = head.splitText(index);
          if (content) {
            this.element.insertBefore(document.createTextNode(content), tail);
          }
          if (deleteCount > 0) {
            tail.splitText(deleteCount); // Creates new tail
            tail.remove();
          }
        }
      }
      this.normalise();
    }
  }

  private normalise() {
    // Replace line-breaks with text nodes containing \n
    for (let child of this.element.childNodes) {
      if (child.nodeName === 'BR')
        child.replaceWith(document.createTextNode('\n'));
      else if (child.nodeName !== '#text')
        child.remove();
    }
    this.element.normalize();
  }

  private mark = () => { this.prev = this.text; };

  private onInput = () => {
    for (let splice of textDiff(this.prev, this.text))
      this.emit('update', splice);
  };
}