import { TextSplice } from '../jrql-support';
import { textDiff } from '../subjects';

export class ElementSpliceText<E extends HTMLElement> {
  private prev: string;
  private inputting?: TextSplice[];

  constructor(
    readonly element: E,
    text: string,
    private readonly inputHandler: (inputs: AsyncGenerator<TextSplice>) => void
  ) {
    if (!element.contentEditable)
      throw new RangeError('Element must be contenteditable');
    element.addEventListener('beforeinput', this.mark);
    element.addEventListener('input', this.onInput);
    // Ensure that the text content is one normalised Text node
    this.element.replaceChildren(document.createTextNode(text));
  }

  get() {
    this.normalise();
    return this.element.textContent ?? '';
  }

  splice(index: number, deleteCount: number, content?: string) {
    if (this.inputting == null) {
      const head = <Text>this.element.firstChild;
      if (head == null) {
        if (index === 0 && content != null)
          this.element.appendChild(document.createTextNode(content));
        else
          throw new RangeError(`Curious splice into empty text`);
      } else {
        const tail = head.splitText(index);
        if (content) {
          this.element.insertBefore(document.createTextNode(content), tail);
        }
        if (deleteCount > 0) {
          tail.splitText(deleteCount); // Creates new tail
          tail.remove();
        }
        this.element.normalize();
      }
    }
  }

  toJSON(): string {
    return this.get();
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

  private mark = () => { this.prev = this.get(); };

  private onInput = () => {
    const splice = textDiff(this.prev, this.get());
    if (splice != null) {
      if (this.inputting == null) {
        this.inputting = [splice];
        this.inputHandler(this.genInputs());
      } else {
        this.inputting.push(splice);
      }
    }
  };

  private async *genInputs() {
    while (this.inputting?.length)
      yield this.inputting.shift()!;
    delete this.inputting;
  }
}