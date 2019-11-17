import uuidv4 = require('uuid/v4');

export class UuidBagBlock {
  readonly id: string;
  readonly data: any;

  static genesis = (): UuidBagBlock => new UuidBagBlock(uuidv4(null));

  private constructor(id: string) {
    this.id = id;
  }
}