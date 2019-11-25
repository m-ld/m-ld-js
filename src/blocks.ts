import { Hash } from "./hash";

export abstract class HashBagBlock<D> {
  next(data: D): HashBagBlock<D> {
    return this.construct(this.id.add(this.hash(data)), data);
  }

  protected constructor(readonly id: Hash, readonly data: D) { }

  protected abstract construct(id: Hash, data: D): HashBagBlock<D>;

  protected abstract hash(data: D): Hash;
}

export class HashStringBagBlock extends HashBagBlock<string> {
  static genesis = (seed?: string): HashStringBagBlock =>
    new HashStringBagBlock(seed ? Hash.digest(seed) : Hash.random(), null);

  private constructor(id: Hash, data: string) {
    super(id, data);
  }

  protected construct(id: Hash, data: string): HashStringBagBlock {
    return new HashStringBagBlock(id, data);
  }

  protected hash(data: string): Hash {
    return Hash.digest(data);
  }
}
