import { OrmSubject, OrmUpdating } from '../src/orm/index';
import { GraphSubject, Optional, Subject } from '../src/index';

export class Flintstone extends OrmSubject {
  name: string;
  height?: number;

  constructor(src: GraphSubject) {
    super(src);
    this.initSrcProperty(src, 'name', String);
    this.initSrcProperty(src, 'height', [Optional, Number]);
  }
}

export class Episode extends OrmSubject {
  title: string;
  starring: Flintstone[];

  constructor(src: GraphSubject, orm: OrmUpdating, title?: string) {
    super(src);
    this.initSrcProperty(src, 'title', String, { init: title });
    this.initSrcProperty(src, 'starring', [Array, Subject], {
      orm, construct: src => new Flintstone(src)
    });
  }
}

export class Series extends OrmSubject {
  episodes: Episode[];

  constructor(src: GraphSubject, orm: OrmUpdating, ...episodes: Episode[]) {
    super(src);
    this.episodes = episodes;
    this.initList(src, Subject, this.episodes, {
      get: i => this.episodes[i].src,
      set: async (i, v: GraphSubject) =>
        this.episodes[i] = await orm.get(v, src => new Episode(src, orm))
    });
  }
}