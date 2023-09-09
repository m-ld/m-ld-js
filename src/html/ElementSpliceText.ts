import { TextSplice } from '../jrql-support';
import { textDiff } from '../subjects';
import { MeldStateMachine } from '../api';
import { TSeq, TSeqOperation } from '../tseq/index';

type Inputting = {
  ours: TSeqOperation[],
  theirs?: TSeqOperation[]
};

/**
 * A wrapper for a `contenteditable` HTML element, allowing the element's text
 * content to be spliced – that is, modified using a {@link TextSplice}, without
 * losing or misplacing a user's active selection range.
 */
export class ElementSpliceText {
  private inputting?: Inputting;
  /** Tracks the _synchronous_ state of the HTML */
  private readonly ours: TSeq;
  /** Tracks the _asynchronous_ state of m-ld */
  private readonly theirs: TSeq;

  /**
   * @param element the HTML element to manage as splice-able text. Must be
   * `contenteditable`.
   * @param initialText some initial text to populate the element with
   * @param clone
   * @param subjectId
   * @param property
   */
  constructor(
    readonly element: HTMLElement,
    readonly clone: MeldStateMachine,
    readonly subjectId: string,
    readonly property: string,
    initialText: string
  ) {
    if (!element.contentEditable)
      throw new RangeError('Element must be contenteditable');
    this.ours = new TSeq('ours');
    this.theirs = new TSeq('theirs');
    this.theirs.apply(this.ours.splice(0, 0, initialText));
    this.attachEventHandlers();
    // Ensure that the text content is one normalised Text node
    this.element.replaceChildren(document.createTextNode(initialText));
  }

  private attachEventHandlers() {
    let prev: string | undefined;
    this.element.addEventListener('beforeinput', () => {
      prev = this.get();
    });
    this.element.addEventListener('input', () => {
      const splice = textDiff(prev!, this.get());
      if (splice != null) {
        if (this.inputting == null) {
          this.inputting = { theirs: [], ours: [] };
          this.doWrite(this.inputting).finally(() => {
            delete this.inputting;
          });
        }
        this.inputting.ours.push(this.ours.splice(...splice));
      }
    });
  }

  protected doWrite(inputting: Inputting) {
    // Prior to obtaining the state lock, we may receive "their" splices, which
    // are stored in the inputting object by this.splice
    return this.clone.write(async state => {
      // State is now locked, not expecting any more "theirs". They were all
      // made against the same inputting base as our input(s). Apply them now.
      // This catches up our TSeq to theirs.
      for (let theirOp of inputting.theirs ?? [])
        this.apply(theirOp);
      // This signals that any splice coming in now is an echo to be ignored
      delete inputting.theirs;
      // Any initial "ours" are also made against the same base; but more may
      // come in, against the caught-up state.
      while (inputting.ours.length) {
        const splices = this.theirs.apply(inputting.ours.shift()!);
        state = await state.write(this.inputWriteRequest(splices));
      }
    });
  }

  protected inputWriteRequest(splices: TextSplice[]) {
    return {
      '@update': {
        '@id': this.subjectId,
        [this.property]: splices.map(splice => ({ '@splice': splice }))
      }
    };
  }

  get() {
    this.normalise();
    return this.element.textContent ?? '';
  }

  splice(index: number, deleteCount: number, content?: string) {
    if (this.inputting == null) {
      this.apply(this.theirs.splice(index, deleteCount, content));
    } else if (this.inputting.theirs != null) {
      // A splice has arrived while waiting for a lock; keep it
      this.inputting.theirs.push(this.theirs.splice(index, deleteCount, content));
    }
  }

  apply(theirOp: TSeqOperation) {
    for (let [index, deleteCount, content] of this.ours.apply(theirOp)) {
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
    this.element.childNodes.forEach(child => {
      if (child.nodeName === 'BR')
        child.replaceWith(document.createTextNode('\n'));
      else if (child.nodeName !== '#text')
        child.remove();
    });
    this.element.normalize();
  }
}
