import {
  DataFactory, DefaultGraph, NamedNode, Quad, Quad_Object, Quad_Predicate, Quad_Subject, QuadSet,
  QuadSource, RdfFactory
} from '../quads';
import { Quadstore } from 'quadstore';
import {
  AbstractChainedBatch, AbstractIterator, AbstractIteratorOptions, AbstractLevelDOWN
} from 'abstract-leveldown';
import { Observable } from 'rxjs';
import { generate as uuid } from 'short-uuid';
import { check, observeAsyncIterator, Stopwatch } from '../util';
import { LockManager } from '../locks';
import { BatchOpts, Binding, ResultType } from 'quadstore/dist/lib/types';
import { Context, Iri } from 'jsonld/jsonld-spec';
import { ActiveContext, activeCtx, compactIri, expandTerm } from '../jsonld';
import { Algebra } from 'sparqlalgebrajs';
import { newEngine } from 'quadstore-comunica';
import { DataFactory as RdfDataFactory } from 'rdf-data-factory';
import { JRQL, M_LD, QS, RDF, XS } from '../../ns';
import { empty, EmptyIterator, wrap } from 'asynciterator';
import { MutableOperation } from '../ops';
import { MeldError } from '../MeldError';
import type EventEmitter = require('events');

/**
 * Atomically-applied patch to a quad-store.
 */
export interface Patch {
  readonly deletes: Iterable<Quad> | Partial<Quad>;
  readonly inserts: Iterable<Quad>;
}

function isDeletePattern(deletes: Patch['deletes']): deletes is Partial<Quad> {
  return !(Symbol.iterator in deletes);
}

/**
 * Specialised patch that allows concatenation.
 */
export class PatchQuads extends MutableOperation<Quad> implements Patch {
  protected constructSet(quads?: Iterable<Quad>): QuadSet {
    return new QuadSet(quads);
  }
}

export type GraphName = DefaultGraph | NamedNode;

export interface KvpStore {
  /** Exact match kvp retrieval */
  get(key: string): Promise<Buffer | undefined>;
  /** Exact match kvp retrieval */
  has(key: string): Promise<boolean>;
  /** Kvp retrieval by range options */
  read(range: AbstractIteratorOptions<string>): Observable<[string, Buffer]>;
  /**
   * Ensures that write transactions are executed serially against the store.
   * @param txn prepares a write operation to be performed
   */
  transact<T = unknown>(txn: TxnOptions<KvpResult<T>>): Promise<T>;
}

/**
 * Writeable dataset. Transactions are atomically and serially applied.
 * Note that the patch created by a transaction can span Graphs - each
 * Quad in the patch will have a graph property.
 */
export interface Dataset extends KvpStore {
  readonly location: string;
  readonly rdf: Required<DataFactory>;

  graph(name?: GraphName): Graph;

  transact<T = unknown>(txn: TxnOptions<PatchResult<T>>): Promise<T>;

  clear(): Promise<void>;

  close(): Promise<void>;
  readonly closed: boolean;
}

export type Kvps = // NonNullable<BatchOpts['preWrite']> with strong kv types
  (batch: Pick<AbstractChainedBatch<string, Buffer>, 'put' | 'del'>) => Promise<unknown> | unknown;

export interface KvpResult<T = unknown> {
  kvps?: Kvps;
  after?(): unknown | Promise<unknown>;
  return?: T;
}

export interface PatchResult<T = unknown> extends KvpResult<T> {
  patch?: Patch;
}

export interface TxnOptions<T extends KvpResult> extends Partial<TxnContext> {
  prepare(txc: TxnContext): T | Promise<T>;
  lock?: string;
}

export interface TxnContext {
  id: string,
  sw: Stopwatch
}

const notClosed = check((d: Dataset) => !d.closed,
  // m-ld-specific error used here to simplify exception handling
  () => new MeldError('Clone has closed'));

/**
 * Read-only utility interface for reading Quads from a Dataset.
 */
export interface Graph extends RdfFactory, QuadSource {
  readonly name: GraphName;

  query(): Observable<Quad>;
  query(query: Algebra.Construct): Observable<Quad>;
  query(query: Algebra.Describe): Observable<Quad>;
  query(query: Algebra.Project): Observable<Binding>;
}

/**
 * Context for Quadstore dataset storage. Mix in with a domain context to
 * optimise (minimise) both control and user content.
 */
export const STORAGE_CONTEXT: Context = {
  qs: QS.$base,
  jrql: JRQL.$base,
  mld: M_LD.$base,
  xs: XS.$base,
  rdf: RDF.$base
};

export class QuadStoreDataset implements Dataset {
  readonly location: string;
  /* readonly */
  store: Quadstore;
  private readonly activeCtx?: Promise<ActiveContext>;
  private readonly lock = new LockManager;
  private isClosed: boolean = false;
  base: Iri | undefined;

  constructor(
    private readonly backend: AbstractLevelDOWN,
    context?: Context,
    private readonly events?: EventEmitter) {
    // Internal of level-js and leveldown
    this.location = (<any>backend).location ?? uuid();
    this.activeCtx = activeCtx({ ...STORAGE_CONTEXT, ...context });
    this.base = context?.['@base'] ?? undefined;
  }

  async initialise(): Promise<QuadStoreDataset> {
    const activeCtx = await this.activeCtx;
    this.store = new Quadstore({
      backend: this.backend,
      comunica: newEngine(),
      dataFactory: new RdfDataFactory(),
      indexes: [
        ['graph', 'subject', 'predicate', 'object'],
        ['graph', 'object', 'subject', 'predicate'],
        ['graph', 'predicate', 'object', 'subject']
      ],
      prefixes: activeCtx == null ? undefined : {
        expandTerm: term => expandTerm(term, activeCtx),
        compactIri: iri => compactIri(iri, activeCtx)
      }
    });
    await this.store.open();
    return this;
  }

  get rdf() {
    return <Required<DataFactory>>this.store.dataFactory;
  };

  graph(name?: GraphName): Graph {
    return new QuadStoreGraph(this, name || this.rdf.defaultGraph());
  }

  @notClosed.async
  transact<T, O extends PatchResult>(txn: TxnOptions<O>): Promise<T> {
    const lockKey = txn.lock ?? 'txn';
    const id = txn.id ?? uuid();
    const sw = txn.sw ?? new Stopwatch(lockKey, id);
    // The transaction lock ensures that read operations that are part of a
    // transaction (e.g. evaluating the @where clause) are not affected by
    // concurrent transactions (fully serialiseable consistency). This is
    // particularly important for SU-Set operation.
    /*
    TODO: This could be improved with snapshots, if all the reads were on the
    same event loop tick, see https://github.com/Level/leveldown/issues/486
    */
    sw.next('lock-wait');
    return this.lock.exclusive(lockKey, async () => {
      sw.next('prepare');
      const result = await txn.prepare({ id, sw: sw.lap });
      sw.next('apply');
      if (result.patch != null)
        await this.applyQuads(result.patch, { preWrite: result.kvps });
      else if (result.kvps != null)
        await this.applyKvps(result.kvps);
      sw.next('after');
      await result.after?.();
      sw.stop();
      return <T>result.return;
    }).then(result => {
      this.events?.emit('commit', txn.id);
      return result;
    }, err => {
      this.events?.emit('error', err);
      throw err;
    });
  }

  private async applyKvps(kvps: Kvps) {
    const batch = this.store.db.batch();
    await kvps(batch);
    return new Promise<void>((resolve, reject) =>
      batch.write(err => err ? reject(err) : resolve()));
  }

  private async applyQuads(patch: Patch, opts?: BatchOpts) {
    if (!isDeletePattern(patch.deletes)) {
      await this.store.multiPatch([...patch.deletes], [...patch.inserts], opts);
    } else {
      // FIXME: Not atomic! – Rarely used
      const { subject, predicate, object, graph } = patch.deletes;
      await new Promise((resolve, reject) =>
        this.store.removeMatches(subject, predicate, object, graph)
          .on('end', resolve).on('error', reject));
      await this.store.multiPut([...patch.inserts], opts);
    }
  }

  @notClosed.async
  get(key: string): Promise<Buffer | undefined> {
    return new Promise<Buffer | undefined>((resolve, reject) =>
      this.store.db.get(key, { asBuffer: true }, (err, buf) => {
        if (err) {
          if (err.message.startsWith('NotFound')) {
            resolve(undefined);
          } else {
            reject(err);
            this.events?.emit('error', err);
          }
        } else {
          resolve(buf);
        }
      }));
  }

  @notClosed.async
  has(key: string): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      const it = this.store.db.iterator({
        gte: key, limit: 1, values: false, keyAsBuffer: false
      });
      it.next((err, foundKey: string) => {
        if (err) {
          reject(err);
          this.events?.emit('error', err);
        } else if (foundKey != null) {
          resolve(foundKey === key);
        } else {
          resolve(false);
        }
        this.end(it);
      });
    });
  }

  @notClosed.rx
  read(range: AbstractIteratorOptions<string>): Observable<[string, Buffer]> {
    return new Observable(subs => {
      const it = this.store.db.iterator({ ...range, keyAsBuffer: false });
      const pull = () => {
        it.next((err, key: string, value: Buffer) => {
          if (err) {
            subs.error(err);
            this.end(it);
            this.events?.emit('error', err);
          } else if (key != null && value != null) {
            subs.next([key, value]);
            pull();
          } else {
            subs.complete();
            this.end(it);
          }
        });
      };
      pull();
    });
  }

  @notClosed.async
  clear(): Promise<void> {
    return new Promise<void>((resolve, reject) =>
      this.store.db.clear(err => {
        if (err) {
          reject(err);
          this.events?.emit('error', err);
        } else {
          resolve();
          this.events?.emit('clear');
        }
      }));
  }

  @notClosed.async
  close(): Promise<void> {
    // Make efforts to ensure no transactions are running
    return this.lock.exclusive('txn', () => {
      this.isClosed = true;
      return this.store.close();
    });
  }

  get closed(): boolean {
    return this.isClosed;
  }

  private end(it: AbstractIterator<any, any>) {
    it.end(err => err && this.events?.emit('error', err));
  }
}

class QuadStoreGraph implements Graph {
  constructor(
    readonly dataset: QuadStoreDataset,
    readonly name: GraphName) {
  }

  match: QuadSource['match'] = (subject, predicate, object, graph) => {
    if (graph != null && !graph.equals(this.name))
      return empty();
    else
      // Must specify graph term due to optimised indexing
      return (<QuadSource>this.dataset.store).match(subject, predicate, object, this.name);
  };

  query(): Observable<Quad>;
  query(query: Algebra.Construct): Observable<Quad>;
  query(query: Algebra.Describe): Observable<Quad>;
  query(query: Algebra.Project): Observable<Binding>;
  query(query?: Algebra.Project | Algebra.Describe | Algebra.Construct): Observable<Binding | Quad> {
    return observeAsyncIterator(async () => {
      try {
        if (query != null) {
          const stream = await this.dataset.store.sparqlStream(query);
          if (stream.type === ResultType.BINDINGS || stream.type === ResultType.QUADS)
            return stream.iterator;
        } else {
          return wrap(this.match());
        }
      } catch (err) {
        // TODO: Comunica bug? Cannot read property 'close' of undefined, if stream empty
        if (err instanceof TypeError)
          return new EmptyIterator();
        throw err;
      }
      throw new Error('Expected bindings or quads');
    });
  }

  skolem = () => this.dataset.rdf.namedNode(
    new URL(`/.well-known/genid/${uuid()}`, this.dataset.base).href);

  namedNode = this.dataset.rdf.namedNode;
  // noinspection JSUnusedGlobalSymbols
  blankNode = this.dataset.rdf.blankNode;
  literal = this.dataset.rdf.literal;
  variable = this.dataset.rdf.variable;
  defaultGraph = this.dataset.rdf.defaultGraph;

  quad = (subject: Quad_Subject, predicate: Quad_Predicate, object: Quad_Object) =>
    this.dataset.rdf.quad(subject, predicate, object, this.name);
}