import { OrmSubject, OrmUpdating } from '../src/orm';
import { GraphSubject, JsType, Optional, Subject } from '../src';
import { property } from '../src/orm/OrmSubject';

////////////////////////////////////////////////////////////////////////////////
// NOTE These examples are duplicated to the OrmSubject documentation

export class Flintstone extends OrmSubject {
  @property(JsType.for(String))
  name: string;
  @property(JsType.for(Optional, Number))
  height?: number;

  constructor(src: GraphSubject) {
    super(src);
    this.initSrcProperties(src);
  }
}

export class Episode extends OrmSubject {
  @property(JsType.for(String))
  title: string;
  @property(JsType.for(Array, Subject))
  starring: Flintstone[];

  constructor(src: GraphSubject, orm: OrmUpdating, title?: string) {
    super(src);
    this.initSrcProperties(src, {
      title: { init: title },
      starring: { orm, construct: src => new Flintstone(src) },
    });
  }
}

export class Series extends OrmSubject {
  episodes: Episode[];

  constructor(src: GraphSubject, orm: OrmUpdating, ...episodes: Episode[]) {
    super(src);
    this.episodes = episodes;
    this.initSrcList(src, Subject, this.episodes, {
      get: i => this.episodes[i].src,
      set: async (i, v: GraphSubject) =>
        this.episodes[i] = await orm.get(v, src => new Episode(src, orm))
    });
  }
}