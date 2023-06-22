export abstract class TSeqContainer<CharNode> {
  /** Characters contributed to TSeq string */
  private _charLength = 0;

  checkInvariants() {
    for (let child of this.children())
      child.checkInvariants();
  }

  get charLength() {
    return this._charLength;
  }

  incCharLength(inc: number) {
    this._charLength += inc;
    this.parent?.incCharLength(inc);
  }

  *chars(fromCharIndex = 0): IterableIterator<CharNode> {
    for (let child of this.children()) {
      if (fromCharIndex === 0 || fromCharIndex < child.charLength) {
        yield *child.chars(fromCharIndex);
        fromCharIndex = 0;
      } else {
        fromCharIndex -= child.charLength;
      }
    }
  }

  isAncestorOf(other: TSeqContainer<CharNode> | undefined) {
    let anc: TSeqContainer<CharNode> | undefined = other;
    while (anc != null && anc !== this)
      anc = anc.parent;
    return anc != null;
  }

  abstract get parent(): TSeqContainer<CharNode> | undefined;

  abstract children(): Iterable<TSeqContainer<CharNode>>;
}