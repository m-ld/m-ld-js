/**
 * Utility superclass for both TSeq nodes and process arrays, whose primary
 * purpose is to track character length. This allows for skipping tree traversal
 * when determining overall size, and when targeting a specific node by
 * character index.
 * @internal
 */
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