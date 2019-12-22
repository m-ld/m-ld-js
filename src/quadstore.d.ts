declare module 'quadstore' {
  import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
  import { Store, Stream, Quad, NamedNode, Term } from 'rdf-js';
  import { EventEmitter } from 'events';
  import { Readable } from 'readable-stream';

  interface IndexOptions {
    lt?: string;
    lte?: string;
    gt?: string;
    gte?: string;
    limit?: number;
    reverse?: boolean;
  }

  type MatchTerms<Q> = {
    [P in keyof Q]?: Q[P];
  }

  class QuadStore<Q> {
    constructor(abstractLevelDown: AbstractLevelDOWN, opts?: AbstractOpenOptions);

    boundary: string;
    
    get(matchTerms: MatchTerms<Q>): Promise<Q[]>;
    getByIndex(name: string, opts: IndexOptions): Promise<Q[]>;
    put(quads: Q | Q[]): Promise<never>;
    del(matchTerms: Q | Q[] | MatchTerms<Q>): Promise<never>;
    patch(oldQuads: Q | Q[] | MatchTerms<Q>, newQuads: Q | Q[]): Promise<never>;
    getStream(matchTerms: MatchTerms<Q>): Readable;
    getByIndexStream(name: string, opts: IndexOptions): Readable;
    putStream(quads: Readable): Promise<never>;
    delStream(quads: Readable): Promise<never>;
    registerIndex(name: string, keygen: (arg: Q) => any): QuadStore<Q>;
  }

  class RdfStore extends QuadStore<Quad> implements Store<Quad> {
    constructor(abstractLevelDown: AbstractLevelDOWN, opts?: AbstractOpenOptions);

    remove(stream: Stream<Quad>): EventEmitter;
    removeMatches(subject?: Term, predicate?: Term, object?: Term, graph?: Term): EventEmitter;
    deleteGraph(graph: string | Term): EventEmitter;
    match(subject?: Term, predicate?: Term, object?: Term, graph?: Term): Stream<Quad>;
    import(stream: Stream<Quad>): EventEmitter;
  }
}

declare module 'quadstore/lib/utils' {
  import { Stream, Quad } from 'rdf-js';

  function streamToArray(readStream: Stream<Quad>): Quad[];
  function createArrayStream(arr: Quad[]): Stream<Quad>;
}